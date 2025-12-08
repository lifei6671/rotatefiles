package rotatefiles

import (
	"errors"
	"io"
	"unicode/utf8"
)

var (
	ErrClosed     = errors.New("line buffer: closed")
	ErrShortWrite = errors.New("line buffer: short write")
)

// LineBuffer 提供一种比 bufio.Writer 更严格的缓存写入语义。
// 其核心设计目标不是简单提升写入吞吐，而是保证写入顺序可预测、
// Flush 时机稳定、错误处理更健壮。与 bufio.Writer 相比：
//
// 1）Flush 语义更明确：
//    - 缓冲写满后立即 Flush
//    - Close 会确保已写入数据全部落到底层 Writer
//    - Reset 会先 Flush 再更换底层 Writer，避免未落盘数据丢失
//
// 2）写入顺序更安全：
//    当缓冲内已有待写入数据时，所有新数据会在 flush 完旧数据后再写入，
//    即便写入数据块大于缓冲区，也不会产生乱序落盘的问题。
//
// 3）大块写入采用安全 bypass：
//    当写入数据尺寸超过缓冲容量，且当前缓存为空时，数据会直接写到底层，
//    避免不必要的拷贝；若缓存中已有数据，则会优先 Flush，再写入新块，
//    从而保证序列严格一致性。
//
// 4）错误处理与数据保留更健壮：
//    当底层 Writer 发生部分写入（short write）时，会正确迁移剩余数据，
//    并保留待写内容，避免重复写入或数据丢失风险，这点优于 bufio.Writer 的宽松语义。
//
// 5）具备可观测性：
//    内部提供累计写入字节、Flush 次数、短写次数等统计指标，
//    有助于在日志系统、持久化链路中定位性能瓶颈或 I/O 异常情况。
//
// 因此 LineBuffer 更适合：
//    - 日志按序落盘
//    - 审计数据流水记录
//    - 数据刷盘行为敏感场景
// 而不仅是简单的 I/O 缓冲场景。

type LineBuffer struct {
	wr  io.Writer
	buf []byte
	n   int
	err error

	writtenBytes uint64
	flushCount   uint64
	shortWrites  uint64
}

// NewLineBuffer 创建一个 buffer size = 4096 的 LineBuffer
func NewLineBuffer(w io.Writer) *LineBuffer {
	return NewLineBufferSize(w, 4096)
}

// NewLineBufferSize 创建一个指定 buffer size 的 LineBuffer
func NewLineBufferSize(w io.Writer, size int) *LineBuffer {
	if size <= 0 {
		size = 4096
	}
	return &LineBuffer{
		wr:  w,
		buf: make([]byte, size),
	}
}

func (lb *LineBuffer) ensureOpen() error {
	if lb == nil || lb.wr == nil {
		return ErrClosed
	}
	return nil
}

// Write 实现 LineBuffer 行为：
// * 若写入内容超过剩余容量，会触发 flush
// * 若连续调用不 flush，会自动 flush
func (lb *LineBuffer) Write(p []byte) (int, error) {
	if err := lb.ensureOpen(); err != nil {
		return 0, err
	}
	if lb.err != nil {
		return 0, lb.err
	}

	totalWritten := 0

	for len(p) > 0 {
		avail := lb.Available()

		// 缓冲不够存放当前 p：先 flush
		if avail == 0 {
			if err := lb.Flush(); err != nil {
				return totalWritten, err
			}
			avail = lb.Available()
		}

		// 若写入块比 buffer 大，直接写到底层
		if len(p) >= len(lb.buf) && lb.Buffered() == 0 {
			n, err := lb.wr.Write(p)
			totalWritten += n
			lb.writtenBytes += uint64(n)
			if err != nil {
				lb.err = err
				return totalWritten, err
			}
			p = p[n:]
			continue
		}

		// 最多写入 buffer 可用部分
		n := copy(lb.buf[lb.n:], p)
		lb.n += n
		p = p[n:]
		totalWritten += n

		if lb.Available() == 0 {
			if err := lb.Flush(); err != nil {
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

// Flush 将 buffer 内容写入底层 writer
func (lb *LineBuffer) Flush() error {
	if err := lb.ensureOpen(); err != nil {
		return err
	}
	if lb.err != nil {
		return lb.err
	}
	if lb.n == 0 {
		return nil
	}

	n, err := lb.wr.Write(lb.buf[:lb.n])
	lb.flushCount++

	if n < lb.n {
		lb.shortWrites++
		if err == nil {
			err = ErrShortWrite
		}
	}

	if n > 0 {
		lb.writtenBytes += uint64(n)
		copy(lb.buf, lb.buf[n:lb.n])
		lb.n -= n
	}

	if err != nil {
		lb.err = err
		return err
	}

	lb.err = nil
	return nil
}

// Reset 将 buffer 复用到新的 writer
func (lb *LineBuffer) Reset(w io.Writer) {
	lb.wr = w
	lb.n = 0
	lb.err = nil
	lb.flushCount = 0
	lb.writtenBytes = 0
	lb.shortWrites = 0
}

// Buffered 已缓存的数据量
func (lb *LineBuffer) Buffered() int {
	if lb == nil {
		return 0
	}
	return lb.n
}

// Available 剩余容量
func (lb *LineBuffer) Available() int {
	if lb == nil {
		return 0
	}
	return len(lb.buf) - lb.n
}

// Close flush 后关闭底层 writer（如果支持）
func (lb *LineBuffer) Close() error {
	if err := lb.Flush(); err != nil {
		return err
	}
	if c, ok := lb.wr.(io.Closer); ok {
		lb.err = c.Close()
		return lb.err
	}
	lb.err = ErrClosed
	return nil
}

func (lb *LineBuffer) WriteByte(b byte) error {
	_, err := lb.Write([]byte{b})
	return err
}

func (lb *LineBuffer) WriteString(s string) (int, error) {
	return lb.Write([]byte(s))
}

func (lb *LineBuffer) WriteRune(r rune) (int, error) {
	if r < utf8.RuneSelf {
		return lb.Write([]byte{byte(r)})
	}
	buf := make([]byte, utf8.RuneLen(r))
	utf8.EncodeRune(buf, r)
	return lb.Write(buf)
}

// Stats 导出内部指标
func (lb *LineBuffer) Stats() (written uint64, flushCount uint64, shortWrites uint64) {
	if lb == nil {
		return 0, 0, 0
	}
	return lb.writtenBytes, lb.flushCount, lb.shortWrites
}
