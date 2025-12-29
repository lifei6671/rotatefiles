package rotatefiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lifei6671/rotatefiles/internal/fileutil"
	"github.com/lifei6671/rotatefiles/periodic"
)

//
// ================================
// ===== Public Types & API =======
// ================================
//

// RotateOption 传递给 NewRotateFile 的配置项。
// 该结构定义日志文件轮转规则、文件创建方式、清理策略等。
//
// RotateOption 的核心思想是将文件输出抽象为周期性配置结果，
// 配置由 RotateGenerator 推送，而 NewRotateFile 根据不同配置动态切换到新的日志文件。
//
// 必选项：
//   - RotateGenerator
//   - NewWriter
//
// 可选项：
//   - FlushDuration：周期 Flush 底层 writer 内容，避免积压。
//   - CheckDuration：周期检查文件是否被外部删除；若删除会自动恢复。
//   - MaxFileNum：最多保留历史文件数量。
//
// 使用示例：
//
//	rg, _ := NewSimpleRotateGenerator("1day", "app.log", nil)
//	opt := &RotateOption{
//		RotateGenerator: rg,
//		NewWriter: func(ctx context.Context, w io.WriteCloser) (AsyncWriter, error) {
//			return NewBufferedWriter(w), nil
//		},
//		FlushDuration: time.Second,
//		MaxFileNum:    7,
//	}
//	w, _ := NewRotateFile(opt)
//	w.Write([]byte("hello"))
type RotateOption struct {
	// RotateGenerator 配置文件发生器
	RotateGenerator RotateGenerator
	// NewWriter 对原始写入器进行包装，用于 提升效率
	NewWriter func(context.Context, io.WriteCloser) (AsyncWriter, error)
	// FlushDuration 控制写入磁盘间隔
	FlushDuration time.Duration
	// CheckDuration 检查文件是否存在的时间间隔
	CheckDuration time.Duration
	// MaxFileNum 保存的最大文件数量，多于该数量则清理，如果为0则忽略
	MaxFileNum int
}

// RotateGenerator 定义日志轮转策略来源。
// 该接口抽象了“周期生成 RotateInfo”的行为。
//
// 一个典型实现为 periodic.SimplePeriodicGenerator，
// 它每隔固定时长生成一个 RotateInfo。
//
// RotateGenerator 必须确保：
//   - 第一次 Generate() 调用能返回有效 FilePath
//   - OnGenerated 回调应该在切换文件时触发
//
// 推荐使用 NewSimpleRotateGenerator 快速构建。

type RotateGenerator interface {
	// Start 启动生成器
	Start(ctx context.Context) error
	// Generate 生成配置信息
	Generate() RotateInfo
	// Stop 关闭生成器
	Stop(ctx context.Context) error
	// OnGenerated 当有新配置生成时的回调
	OnGenerated(fn func(value RotateInfo, at time.Time))
}

var _ RotateGenerator = (*rotateGenerator)(nil)

type rotateGenerator struct {
	p periodic.PeriodicGenerator[RotateInfo]
}

// GeneratorFunc 是 RotateGenerator 的函数适配器。
type GeneratorFunc func(ctx context.Context) (RotateInfo, error)

// NewRotateGenerator 创建一个通用的 RotateGenerator。
// generator 用于生成 RotateInfo，interval 表示回调周期。
//
// 推荐场景：自定义非时间轮转规则，例如按照写入大小切分。
// NewRotateGenerator 初始化一个配置生成器
func NewRotateGenerator(generator GeneratorFunc, interval time.Duration) RotateGenerator {
	return &rotateGenerator{
		p: periodic.NewPeriodicGeneratorFunc[RotateInfo](generator, interval),
	}
}

// NewSimpleRotateGenerator 使用内置规则创建 RotateGenerator。
// rule 必须来自 defaultRotateRules，例如：
//   - "1day"、"1hour"、"5min" 等
//
// filename 表示主日志文件名称；
// 轮转后的文件名自动附加规则生成的后缀。
//
// 当内部生成器遇到错误时，会触发 onErr 回调。
func NewSimpleRotateGenerator(rule string, filename string, onErr func(err error)) (RotateGenerator, error) {
	rt, has := defaultRotateRules[rule]
	if !has {
		return nil, errors.New("no default rotate rules")
	}
	return &rotateGenerator{
		p: periodic.NewPeriodicGeneratorFunc[RotateInfo](
			func(ctx context.Context) (RotateInfo, error) {
				return RotateInfo{
					Symlink:  filename,
					Filename: filename,
					FilePath: strings.Join([]string{filename, rt.SuffixFunc()}, ""),
					RuleName: rule,
				}, nil
			},
			rt.Duration,
			periodic.WithErrorHandler[RotateInfo](onErr),
			periodic.WithImmediate[RotateInfo](),
		),
	}, nil
}

func (r *rotateGenerator) Generate() RotateInfo {
	return r.p.Latest().Value
}

func (r *rotateGenerator) Stop(ctx context.Context) error {
	return r.p.Stop(ctx)
}

func (r *rotateGenerator) Start(ctx context.Context) error {
	return r.p.Start(ctx)
}

func (r *rotateGenerator) OnGenerated(fn func(value RotateInfo, at time.Time)) {
	r.p.OnGenerated(fn)
}

type rotateFile struct {
	opt        *RotateOption
	currentCfg RotateInfo // 当前文件信息
	lastFlush  time.Time  // 最近一次已经落盘时间

	// 底层文件
	outFile *os.File
	// 写缓冲
	buf AsyncWriter

	onErr func(err error)

	flushTicker *time.Ticker
	checkTicker *time.Ticker
	stopChan    chan struct{}
	closed      atomic.Bool

	lock sync.Mutex
}

// RotateFileOption 表示对 rotateFile 额外增强的选项。
type RotateFileOption func(*rotateFile)

// WithOnErr 注册错误处理回调。
// 所有内部错误并不会影响主流程，只会通过 OnErr 提供观察能力。
func WithOnErr(fn func(err error)) RotateFileOption {
	return func(f *rotateFile) {
		f.onErr = fn
	}
}

// NewRotateFile 创建一个支持自动轮转的 io.WriteCloser。
// 返回的 writer 可直接作为日志输出使用。
//
// 调用方应保证：
//   - 必须保持返回 writer 的生命周期
//   - 使用完成后调用 Close()，避免底层资源泄漏
//
// 它具备以下能力：
//  1. 根据 RotateGenerator 推送的配置自动切换文件
//  2. 按 FlushDuration 定时落盘，避免内存积压
//  3. 按 MaxFileNum 保留历史日志文件
//  4. 如文件被外部删除，会自动恢复
//
// 调用示例：
//
//	rg, _ := NewSimpleRotateGenerator("1day", "app.log", nil)
//	opt := &RotateOption{
//		RotateGenerator: rg,
//		NewWriter: func(ctx context.Context, wc io.WriteCloser) (AsyncWriter, error) {
//			return NewAsyncWriterImpl(wc), nil
//		},
//		FlushDuration: time.Second,
//		MaxFileNum:    3,
//	}
//	w, _ := NewRotateFile(opt, WithOnErr(log.Println))
//	w.Write([]byte("start log"))
//
// NewRotateFile 创建最终使用的 Writer
func NewRotateFile(opt *RotateOption, opts ...RotateFileOption) (io.WriteCloser, error) {
	if opt == nil {
		return nil, errors.New("RotateOption is nil")
	}
	if opt.RotateGenerator == nil {
		return nil, errors.New("RotateGenerator required")
	}
	if opt.NewWriter == nil {
		return nil, errors.New("NewWriter required")
	}

	rf := &rotateFile{
		opt:      opt,
		stopChan: make(chan struct{}),
	}
	for _, optFn := range opts {
		optFn(rf)
	}
	// 初始化刷盘定时器
	if opt.FlushDuration > 0 {
		rf.flushTicker = time.NewTicker(opt.FlushDuration)
		go rf.doCheckFlushed(opt.FlushDuration)
	}
	// 初始化检查定时器
	if opt.CheckDuration > 0 {
		rf.checkTicker = time.NewTicker(opt.CheckDuration)
		go rf.doCheckOpened()
	}

	// 初始化第一次配置
	cfg := opt.RotateGenerator.Generate()
	if err := rf.applyConfig(cfg); err != nil {
		return nil, err
	}
	// 当有新配置生成后，执行一次切换配置
	opt.RotateGenerator.OnGenerated(func(value RotateInfo, at time.Time) {
		if err := rf.applyConfig(value); err != nil && rf.onErr != nil {
			rf.onErr(fmt.Errorf("RotateGenerator.OnGenerate applyConfig error: %w - %s", err, value.FilePath))
		}
	})

	return rf, nil
}

func (r *rotateFile) doCheckOpened() {
	if r.checkTicker != nil {
		defer r.checkTicker.Stop()

		for {
			select {
			case <-r.checkTicker.C:
				if err := r.applyConfig(r.opt.RotateGenerator.Generate()); err != nil && r.onErr != nil {
					r.onErr(fmt.Errorf("rotateFile.doCheckOpened: %w", err))
				}
			case <-r.stopChan:
				return
			}
		}
	}
}

func (r *rotateFile) fileExists(outFile string) bool {
	if r.outFile == nil || !fileutil.FileExists(outFile) {
		return false
	}
	info, err := os.Stat(outFile)
	if err != nil {
		return false
	}
	oldInfo, err := r.outFile.Stat()
	if err != nil {
		return false
	}

	return os.SameFile(info, oldInfo)
}

// doCheckFlushed 用于自动检查文件切换和周期 flush
func (r *rotateFile) doCheckFlushed(dur time.Duration) {
	if r.flushTicker != nil {
		defer r.flushTicker.Stop()

		for {
			select {
			case <-r.flushTicker.C:
				r.doFlush(dur)
			case <-r.stopChan:
				return
			}
		}
	}
}

// Write 表现如普通文件
func (r *rotateFile) Write(p []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.buf == nil {
		return 0, io.ErrClosedPipe
	}

	n, err := r.buf.Write(p)
	stats := r.buf.Stats()
	// 这里只需要粗略判断即可
	if stats.QueueSize == 0 {
		r.lastFlush = time.Now()
	}
	return n, err
}

// Close 关闭底层 output writer。
// Close 具有幂等性，可安全执行多次。
//
// Close 会：
//   - flush buffer
//   - 关闭底层文件
//   - 停止定时检查
//   - 停止 RotateGenerator
//
// Close 之后文件不会再“复活”。
func (r *rotateFile) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(r.stopChan)

	r.lock.Lock()
	defer r.lock.Unlock()

	var flushErr, bufCloseErr, fileCloseErr error

	if r.buf != nil {
		flushErr = r.buf.Flush(200 * time.Millisecond)
		bufCloseErr = r.buf.Close()
		r.buf = nil
	}

	if r.outFile != nil {
		fileCloseErr = r.outFile.Close()
		r.outFile = nil
	}

	if r.opt.RotateGenerator != nil {
		_ = r.opt.RotateGenerator.Stop(context.Background())
	}

	return errors.Join(flushErr, bufCloseErr, fileCloseErr)
}

// applyConfig 根据最新 RotateInfo 切换日志文件。
// 若 FilePath 不存在，先创建；若旧文件与配置不一致，则关闭旧文件。
//
// 该方法内部保证线程安全，不需外部加锁。
func (r *rotateFile) applyConfig(info RotateInfo) error {
	// 如果已经关闭了，则直接返回
	if r.closed.Load() {
		return nil
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	fileExists := r.fileExists(info.FilePath)
	if !fileExists {
		dir := filepath.Dir(info.FilePath)
		if err := fileutil.KeepDirExists(dir); err != nil {
			return fmt.Errorf("rotateFile.applyConfig: %w - %s", err, dir)
		}
	}
	needRotate := false
	// 如果配置变了也需要重新创建文件
	if !r.currentCfg.Equal(info) {
		needRotate = true
	}

	if r.outFile == nil || !fileExists || needRotate {
		// 第一步、打开新文件
		fd, err := os.OpenFile(info.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open file failed: %w - %s", err, info.FilePath)
		}

		// 第二步、创建新 writer
		buf, err := r.opt.NewWriter(context.Background(), fd)
		if err != nil {
			// 这里失败了不影响日志写入
			_ = fd.Close()
			return fmt.Errorf("new writer failed: %w", err)
		}
		// 第三步、清理旧资源
		if r.outFile != nil {
			bfFlushErr := r.buf.Flush(20 * time.Millisecond)
			bfCloseErr := r.buf.Close()
			if bfFlushErr != nil && r.onErr != nil {
				r.onErr(fmt.Errorf("rotateFile.applyConfig flush err: %w", bfFlushErr))
			}
			if bfCloseErr != nil && r.onErr != nil {
				r.onErr(fmt.Errorf("rotateFile.applyConfig close err: %w", bfCloseErr))
			}
		}
		r.currentCfg = info
		r.buf = buf
		r.outFile = fd
	}
	if err := info.CheckSymlink(); err != nil {
		if r.onErr != nil {
			r.onErr(fmt.Errorf("[applyConfig] check symlink failed: %w - %s", err, info.FilePath))
		}
		return fmt.Errorf("applyConfig: check symlink failed: %w - %s", err, info.FilePath)
	}

	r.cleanOldFilesLocked()
	return nil
}

// doFlush 执行周期 flush。
// flush 失败不会中断主流程，但会通过 onErr 暴露。
func (r *rotateFile) doFlush(dur time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.buf == nil {
		return
	}
	if r.lastFlush.IsZero() || time.Since(r.lastFlush) >= dur {
		if err := r.buf.Flush(20 * time.Millisecond); err != nil && r.onErr != nil {
			r.onErr(fmt.Errorf("[doFlush]flush failed: %w", err))
		}
		r.lastFlush = time.Now()
	}
}

// cleanOldFilesLocked 执行文件清理策略。
// 该操作严格基于 rotate rule 的后缀正则执行。
// 若 Rule 未配置正则，将跳过清理。
func (r *rotateFile) cleanOldFilesLocked() {
	if r.opt.MaxFileNum <= 0 {
		return
	}
	rt, has := defaultRotateRules[r.currentCfg.RuleName]
	if !has || rt == nil || rt.SuffixExpr == nil {
		// 没配置正则，出于保守原则，不做清理
		return
	}
	dir := filepath.Dir(r.currentCfg.FilePath)
	base := filepath.Base(r.currentCfg.Filename)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if r.onErr != nil {
			r.onErr(fmt.Errorf("[doFlush] read dir failed: %w", err))
		}
		return
	}

	var matched []string

	prefix := base
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		if filepath.Base(name) == base {
			continue // 原始软链文件不删
		}
		if !strings.HasPrefix(name, prefix) {
			// 不存在指定的前缀不处理
			continue
		}
		suffix := name[len(prefix):]
		// 如果正则能匹配到则说明要清理
		if rt.SuffixExpr.MatchString(suffix) {
			matched = append(matched, filepath.Join(dir, e.Name()))
		}
	}

	if len(matched) <= r.opt.MaxFileNum {
		return
	}

	sort.Slice(matched, func(i, j int) bool {
		fi1, _ := os.Stat(matched[i])
		fi2, _ := os.Stat(matched[j])
		if fi1 != nil && fi2 != nil {
			return fi1.ModTime().Before(fi2.ModTime())
		}
		return matched[i] < matched[j]
	})

	delList := matched[:len(matched)-r.opt.MaxFileNum]
	for _, f := range delList {
		if err := os.Remove(f); err != nil && r.onErr != nil {
			r.onErr(fmt.Errorf("remove file failed: %w - %s", err, f))
		}
	}
}

func (r *rotateFile) Sync() error {
	if r.closed.Load() {
		return nil
	}
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.buf != nil {
		return r.buf.Flush(time.Microsecond * 10)
	}
	return nil
}
