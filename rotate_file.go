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
	"time"

	"github.com/lifei6671/rotatefiles/periodic"
)

type RotateOption struct {
	// RotateGenerator 配置文件发生器
	RotateGenerator RotateGenerator
	// NewWriter 对原始写入器进行包装，用于 提升效率
	NewWriter func(context.Context, io.WriteCloser) (AsyncWriter, error)
	// FlushDuration 控制写入磁盘间隔
	FlushDuration time.Duration
	// MaxFileNum 保存的最大文件数量，多于该数量则清理，如果为0则忽略
	MaxFileNum int
}

// RotateInfo 文件切割的必要参数
type RotateInfo struct {
	// 日志软链
	Symlink string
	// 原始日志路径：./log/app.log
	Filename string
	// 实际软链指向的文件地址： ./log/app.log.2025111111
	FilePath string
}

// Equal 判断两个配置是否一致
func (f RotateInfo) Equal(o RotateInfo) bool {
	return f.Symlink == o.Symlink && f.Filename == o.Filename && f.FilePath == o.FilePath
}

// IsNeedSymlink 判断软链是否和目标文件一致
func (f RotateInfo) IsNeedSymlink() bool {
	return f.Symlink != "" && f.Symlink != f.FilePath
}

type RotateGenerator interface {
	// Start 启动生成器
	Start(ctx context.Context) error
	// Generate 生成配置信息
	Generate() RotateInfo
	// Stop 关闭生成器
	Stop(ctx context.Context) error
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

type rotateFile struct {
	opt        *RotateOption
	currentCfg RotateInfo // 当前文件信息
	lastFlush  time.Time  // 最近一次已经落盘时间

	// 底层文件
	outFile *os.File
	// 写缓冲
	buf AsyncWriter

	flushTicker *time.Ticker
	stopChan    chan struct{}

	lock sync.Mutex
}

// NewRotateFile 创建最终使用的 Writer
func NewRotateFile(opt *RotateOption) (io.WriteCloser, error) {
	if opt == nil {
		return nil, errors.New("RotateOption is nil")
	}
	if opt.RotateGenerator == nil {
		return nil, errors.New("RotateGenerator required")
	}
	if opt.NewWriter == nil {
		return nil, errors.New("NewWriter required")
	}
	if opt.FlushDuration <= 0 {
		opt.FlushDuration = 300 * time.Millisecond
	}

	rf := &rotateFile{
		opt:         opt,
		stopChan:    make(chan struct{}),
		flushTicker: time.NewTicker(opt.FlushDuration),
	}

	// 初始化第一次配置
	cfg := opt.RotateGenerator.Generate()
	if err := rf.applyConfig(cfg); err != nil {
		return nil, err
	}

	// 启动周期刷新逻辑 & 轮换检测
	go rf.loop()

	return rf, nil
}

// loop 用于自动检查文件切换和周期 flush
func (r *rotateFile) loop() {
	defer r.flushTicker.Stop()

	var lastCfg RotateInfo = r.currentCfg

	for {
		select {
		case <-r.flushTicker.C:
			r.doFlush()
			cfg := r.opt.RotateGenerator.Generate()
			if !cfg.Equal(lastCfg) {
				_ = r.rotateTo(cfg)
				lastCfg = cfg
			}

		case <-r.stopChan:
			return
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

	// 创建目录
	if err := os.MkdirAll(filepath.Dir(info.FilePath), 0755); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}

	// 打开新文件
	fd, err := os.OpenFile(info.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file failed: %w - %s", err, info.FilePath)
	}

	// 创建新 writer
	buf, err := r.opt.NewWriter(context.Background(), fd)
	if err != nil {
		_ = fd.Close()
		return fmt.Errorf("new writer failed: %w", err)
	}

	r.currentCfg = info
	r.buf = buf
	r.outFile = fd

	r.lastFlush = time.Now()

	if info.IsNeedSymlink() {
		_ = r.applySymlink(info)
	}

	r.cleanOldFilesLocked()
	return nil
}

// rotateTo 尝试切换到新目标文件
func (r *rotateFile) rotateTo(info RotateInfo) error {
	r.lock.Lock()

	if info.Equal(r.currentCfg) {
		r.lock.Unlock()
		return nil
	}

	// 旧 writer flush
	if r.buf != nil {
		_ = r.buf.Flush(20 * time.Millisecond)
		_ = r.buf.Close()
	}
	// 关闭旧文件
	if r.outFile != nil {
		_ = r.outFile.Close()
	}
	r.lock.Unlock()
	return r.applyConfig(info)
}

// doFlush 执行主动落盘
func (r *rotateFile) doFlush() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.buf == nil {
		return
	}
	_ = r.buf.Flush(20 * time.Millisecond)
}

// applySymlink 创建软链
func (r *rotateFile) applySymlink(info RotateInfo) error {
	if info.Symlink == "" {
		return nil
	}

	_ = os.Remove(info.Symlink) // 忽略错误

	rel, err := filepath.Rel(filepath.Dir(info.Symlink), info.FilePath)
	if err != nil {
		return err
	}

	return os.Symlink(rel, info.Symlink)
}

// cleanOldFilesLocked 清理超限文件
func (r *rotateFile) cleanOldFilesLocked() {
	if r.opt.MaxFileNum <= 0 {
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
		if filepath.Base(e.Name()) == base {
			continue // 原始软链文件不删
		}
		if len(e.Name()) > len(prefix) && e.Name()[:len(prefix)] == prefix {
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
		_ = os.Remove(f)
	}
}
