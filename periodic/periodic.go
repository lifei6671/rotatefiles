package periodic

import (
	"context"
	"sync"
	"time"
)

// Generator 定义一个周期性生成值的接口，强制支持 context
type Generator[T any] interface {
	Generate(ctx context.Context) (T, error)
}

// GeneratorFunc 匿名函数适配器
type GeneratorFunc[T any] func(ctx context.Context) (T, error)

func (f GeneratorFunc[T]) Generate(ctx context.Context) (T, error) {
	return f(ctx)
}

// Result 保存最新生成结果
type Result[T any] struct {
	Value   T
	At      time.Time
	Err     error
	Updated bool
}

type PeriodicGenerator[T any] interface {
	// Start 启动发生器
	Start(_ context.Context) error
	// Stop 停止发生器
	Stop(ctx context.Context) error
	// Wait 等待发生器关闭完成
	Wait()
	// Latest 获取最后一次生产的结果
	Latest() Result[T]
}

// SimplePeriodicGenerator 周期任务执行器
type SimplePeriodicGenerator[T any] struct {
	fn        Generator[T]
	interval  time.Duration
	lock      sync.RWMutex
	startOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc

	latest     Result[T]
	onError    func(error)
	immediate  bool
	retryDelay time.Duration

	stopped chan struct{}
}

// PeriodicOption 配置项
type PeriodicOption[T any] func(pg *SimplePeriodicGenerator[T])

// WithImmediate 表示 Start 后立即执行一次
func WithImmediate[T any]() PeriodicOption[T] {
	return func(pg *SimplePeriodicGenerator[T]) {
		pg.immediate = true
	}
}

// WithRetryDelay 设置执行失败后的额外等待时间
func WithRetryDelay[T any](delay time.Duration) PeriodicOption[T] {
	return func(pg *SimplePeriodicGenerator[T]) {
		pg.retryDelay = delay
	}
}

// WithErrorHandler 执行失败时的回调
func WithErrorHandler[T any](handler func(err error)) PeriodicOption[T] {
	return func(pg *SimplePeriodicGenerator[T]) {
		pg.onError = handler
	}
}

// WithParentContext 允许外部传递父 Context
func WithParentContext[T any](parent context.Context) PeriodicOption[T] {
	return func(pg *SimplePeriodicGenerator[T]) {
		if parent == nil {
			return
		}
		pg.ctx, pg.cancel = context.WithCancel(parent)
	}
}

// NewPeriodicGenerator 创建生成器
func NewPeriodicGenerator[T any](
	fn Generator[T],
	interval time.Duration,
	opts ...PeriodicOption[T],
) *SimplePeriodicGenerator[T] {
	if interval <= 0 {
		panic("periodic: interval must be > 0")
	}

	ctx, cancel := context.WithCancel(context.Background())

	pg := &SimplePeriodicGenerator[T]{
		fn:       fn,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		stopped:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(pg)
	}

	return pg
}

// NewPeriodicGeneratorFunc 创建基于函数的周期生成器
func NewPeriodicGeneratorFunc[T any](
	fn func(ctx context.Context) (T, error),
	interval time.Duration,
	opts ...PeriodicOption[T],
) *SimplePeriodicGenerator[T] {
	return NewPeriodicGenerator[T](GeneratorFunc[T](fn), interval, opts...)
}

// Start 启动运行
func (pg *SimplePeriodicGenerator[T]) Start(_ context.Context) error {
	pg.startOnce.Do(func() {
		go func() {
			defer close(pg.stopped)
			pg.runLoop()
		}()
	})
	return nil
}

// Stop 请求停止周期执行
func (pg *SimplePeriodicGenerator[T]) Stop(_ context.Context) error {
	pg.cancel()
	return nil
}

// Wait 阻塞等待执行退出
func (pg *SimplePeriodicGenerator[T]) Wait() {
	<-pg.stopped
}

// runLoop 负责周期调度逻辑
func (pg *SimplePeriodicGenerator[T]) runLoop() {
	if pg.immediate {
		pg.runOnce()
	}

	next := time.Now().Add(pg.interval)

	timer := time.NewTimer(pg.interval)
	defer timer.Stop()
	for {
		wait := time.Until(next)
		if wait > 0 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(wait)

			select {
			case <-timer.C:
			case <-pg.ctx.Done():
				return
			}
		} else {
			select {
			case <-pg.ctx.Done():
				return
			default:
			}
		}
		if pg.ctx.Err() != nil {
			return
		}

		pg.runOnce()
		next = next.Add(pg.interval)
	}
}

// runOnce 调用 Generate 方法并写入 latest
func (pg *SimplePeriodicGenerator[T]) runOnce() {
	value, err := pg.fn.Generate(pg.ctx)

	pg.lock.Lock()
	if err != nil {
		pg.latest.Err = err
		pg.latest.Updated = false
	} else {
		pg.latest.Value = value
		pg.latest.Updated = true
		pg.latest.At = time.Now()
		pg.latest.Err = nil
	}
	pg.lock.Unlock()

	if err != nil && pg.onError != nil {
		go pg.onError(err)
	}

	if err != nil && pg.retryDelay > 0 {
		select {
		case <-time.After(pg.retryDelay):
		case <-pg.ctx.Done():
			return
		}
	}
}

// Latest 返回快照
func (pg *SimplePeriodicGenerator[T]) Latest() Result[T] {
	pg.lock.RLock()
	defer pg.lock.RUnlock()
	return pg.latest
}
