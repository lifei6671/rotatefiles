package rotatefiles

import "time"

type option struct {
	// 时钟发生器
	clock Clock
	// 日志软链
	symlink string
	// 需要保留的日志数量
	rotationLimit uint
	// 原始日志路径：logs/app.log
	filename string
	// 时区
	location *time.Location
}

type Option func(*option)

func WithClock(clock Clock) Option {
	return func(o *option) {
		o.clock = clock
	}
}
func WithSymlink(symlink string) Option {
	return func(o *option) {
		o.symlink = symlink
	}
}

func WithRotationLimit(limit uint) Option {
	return func(o *option) {
		o.rotationLimit = limit
	}
}
func WithFilename(filename string) Option {
	return func(o *option) {
		o.filename = filename
	}
}

func WithLocation(location *time.Location) Option {
	return func(o *option) {
		o.location = location
	}
}
