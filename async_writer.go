package rotatefiles

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ErrWriteTimeout 在写入缓冲队列超时时返回
var ErrWriteTimeout = errors.New("io: write timeout")

type AsyncWriter interface {
	io.WriteCloser
	Flush(timeout time.Duration) error
	Stats() AsyncWriterStats
}

// asyncWriter 实现一个带缓冲、支持超时的异步写入器。
// 注意：为了保证 io.Writer 语义，Write 会拷贝传入的字节切片。
type asyncWriter struct {
	wc      io.WriteCloser
	ch      chan []byte
	done    chan struct{} // 写协程退出通知
	timeout time.Duration // 写入队列超时时间（0: 无超时，<0: 非阻塞尝试）
	closed  atomic.Bool   // 是否已经关闭

	// 统计指标
	writeCount atomic.Uint64             // 成功入队的写次数
	writeBytes atomic.Uint64             // 成功入队的字节总数
	dropCount  atomic.Uint64             // 因队列满/超时导致被丢弃的写次数
	errorCount atomic.Uint64             // 底层 Write 出错次数
	lastTime   atomic.Pointer[time.Time] // 最后写入时间

	// 错误回调
	lock     sync.RWMutex
	callback func(n int, err error)
}

// AsyncWriterStats 导出异步写入器的统计指标
type AsyncWriterStats struct {
	// 已成功入队的写入次数
	EnqueuedWrites uint64
	// 已成功入队的总字节数
	EnqueuedBytes uint64
	// 因队列满或超时被丢弃的写入次数
	DroppedWrites uint64
	// 底层 wc.Write 返回错误的次数
	WriteErrors uint64
	// 当前缓冲队列中的待写入条目数（近似值）
	QueueSize int
}

// 确保实现 io.WriteCloser
var _ io.WriteCloser = (*asyncWriter)(nil)

const flushCheckInterval = 10 * time.Millisecond

// 检查 writer 是否关闭
func (w *asyncWriter) isClosed() bool {
	return w == nil || w.closed.Load()
}

// 异步写协程：从队列中取数据写入到底层 writer
func (w *asyncWriter) asyncWrite() {
	defer close(w.done)

	// 防御性 recover，避免底层 wc.Write panic 导致 done 永不关闭
	defer func() {
		if r := recover(); r != nil {
			// 这里可以按需打日志，但组件本身不直接输出
		}
	}()

	for b := range w.ch {
		n, err := w.wc.Write(b)
		flushTime := time.Now()
		w.lastTime.Store(&flushTime)
		if err != nil {
			w.errorCount.Add(1)

			w.lock.RLock()
			cb := w.callback
			w.lock.RUnlock()

			if cb != nil {
				cb(n, err)
			}
		}
	}
}

// Write 实现 io.Writer 接口。
//   - 立即返回（可能只是入队成功）
//   - 根据 timeout 行为不同：
//     timeout == 0 : 阻塞直到入队
//     timeout < 0  : 非阻塞尝试入队，失败立即返回 ErrWriteTimeout
//     timeout > 0  : 在 timeout 内尝试入队，超时返回 ErrWriteTimeout
func (w *asyncWriter) Write(p []byte) (n int, err error) {
	if w.isClosed() || w.wc == nil {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}

	// 为避免调用方复用 p 导致数据错乱，这里必须拷贝
	buf := make([]byte, len(p))
	copy(buf, p)

	// 防御性 recover：如果在发送到已关闭 channel 时 panic，
	// 将其转化为 ErrClosedPipe 而不是整个程序崩溃。
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = io.ErrClosedPipe
		}
	}()

	// 不设置延迟超时时间：直接阻塞写入队列
	if w.timeout == 0 {
		w.ch <- buf
		w.writeCount.Add(1)
		w.writeBytes.Add(uint64(len(buf)))
		return len(p), nil
	}

	// timeout < 0：非阻塞写入，队列满则丢弃
	if w.timeout < 0 {
		select {
		case w.ch <- buf:
			w.writeCount.Add(1)
			w.writeBytes.Add(uint64(len(buf)))
			return len(p), nil
		default:
			w.dropCount.Add(1)
			return 0, ErrWriteTimeout
		}
	}

	// timeout > 0：在指定时间内尝试入队
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()

	select {
	case w.ch <- buf:
		w.writeCount.Add(1)
		w.writeBytes.Add(uint64(len(buf)))
		return len(p), nil
	case <-timer.C:
		w.dropCount.Add(1)
		return 0, ErrWriteTimeout
	}
}

// Close 关闭 writer：
// 1. 原子标记为关闭（幂等）
// 2. 关闭 channel 通知写协程退出
// 3. 等待写协程退出
// 4. 关闭底层 writer
func (w *asyncWriter) Close() error {
	if w == nil {
		return nil
	}

	// 保证只关闭一次
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}

	// 关闭队列，通知写协程退出
	close(w.ch)

	// 等待写协程结束（包括兜底 recover）
	<-w.done

	if w.wc != nil {
		return w.wc.Close()
	}
	return nil
}

// Flush 等待缓冲队列刷空。
// timeout <= 0 表示一直等待到队列为空。
// timeout > 0 表示在指定时间内等待队列变空，否则返回 ErrWriteTimeout。
// 注意：Flush 只保证队列为空，不严格保证最后一个元素已经完全写完
// （对日志类场景通常已经足够）。
func (w *asyncWriter) Flush(timeout time.Duration) error {
	if w.isClosed() {
		return nil
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		// len(ch) 在并发下是安全的
		if len(w.ch) == 0 {
			return nil
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return ErrWriteTimeout
		}

		time.Sleep(flushCheckInterval)
	}
}

// Stats 返回当前的统计指标快照。
// 调用是无锁的，仅使用原子读取。
func (w *asyncWriter) Stats() AsyncWriterStats {
	if w == nil {
		return AsyncWriterStats{}
	}
	return AsyncWriterStats{
		EnqueuedWrites: w.writeCount.Load(),
		EnqueuedBytes:  w.writeBytes.Load(),
		DroppedWrites:  w.dropCount.Load(),
		WriteErrors:    w.errorCount.Load(),
		QueueSize:      len(w.ch),
	}
}

// SetWriterErr 设置底层写错误回调。
// 回调在异步写协程中调用，不应执行耗时过长操作。
func (w *asyncWriter) SetWriterErr(call func(int, error)) {
	if w.isClosed() {
		return
	}
	w.lock.Lock()
	w.callback = call
	w.lock.Unlock()
}

// NewAsyncWriter 创建一个带缓冲、支持超时控制的异步 writer。
//   - wc: 底层 io.WriteCloser
//   - size: 缓冲队列容量（条目数）
//   - timeout:
//     =0  -> Write 阻塞直到数据入队
//     <0  -> 非阻塞尝试入队，队列满立即返回 ErrWriteTimeout
//     >0  -> 在 timeout 内尝试入队，超时返回 ErrWriteTimeout
//
// 返回值实现了 io.WriteCloser 接口；如需访问 Flush/Stats，可以类型断言为 AsyncWriter 接口。
func NewAsyncWriter(wc io.WriteCloser, size int, timeout time.Duration) io.WriteCloser {
	if size <= 0 {
		// 防御：避免 size=0 导致所有写都阻塞在队列上
		size = 1
	}

	w := &asyncWriter{
		wc:      wc,
		ch:      make(chan []byte, size),
		done:    make(chan struct{}),
		timeout: timeout,
	}

	w.closed.Store(false)
	go w.asyncWrite()

	return w
}
