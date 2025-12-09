package rotatefiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lifei6671/rotatefiles/internal/fileutil"
	"github.com/lifei6671/rotatefiles/periodic"
)

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

type GeneratorFunc func(ctx context.Context) (RotateInfo, error)

// NewRotateGenerator 初始化一个配置生成器
func NewRotateGenerator(generator GeneratorFunc, interval time.Duration) RotateGenerator {
	return &rotateGenerator{
		p: periodic.NewPeriodicGeneratorFunc[RotateInfo](generator, interval),
	}
}

// NewSimpleRotateGenerator 使用内置规则
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

	lock sync.Mutex
}

type RotateFileOption func(*rotateFile)

func WithOnErr(fn func(err error)) RotateFileOption {
	return func(f *rotateFile) {
		f.onErr = fn
	}
}

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
	if !fileutil.FileExists(outFile) {
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

func (r *rotateFile) Close() error {
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

// applyConfig 初始化或切换文件
func (r *rotateFile) applyConfig(info RotateInfo) error {
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
	if err := info.CheckSymlink(); err != nil && r.onErr != nil {
		r.onErr(fmt.Errorf("[applyConfig] check symlink failed: %w - %s", err, info.FilePath))
	}

	r.cleanOldFilesLocked()
	return nil
}

// doFlush 执行主动落盘
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

// cleanOldFilesLocked 清理超限文件
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
		return
	}

	var matched []string

	prefix := base
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		log.Println(name, base)
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
			log.Println(suffix, name)
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
