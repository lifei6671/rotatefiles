package rotatefiles

import "time"

// UTC 一个支持生成 UTC 时间的时钟
var UTC = NewClock()

// Local 生成本地时间的时钟
var Local = NewLocalClock()

// Clock 生成时间的时钟接口
type Clock interface {
	Now() time.Time
}

type utcClock struct{}

func NewClock() Clock {
	return &utcClock{}
}
func (c *utcClock) Now() time.Time {
	return time.Now().UTC()
}

type localClock struct{}

func NewLocalClock() Clock {
	return &localClock{}
}
func (c *localClock) Now() time.Time {
	return time.Now().Local()
}
