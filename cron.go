package rotatefiles

import (
	"log"
	"sync"
	"time"
)

var nowFunc = time.Now

func NewSimpleCron(duration time.Duration) *SimpleCron {
	s := &SimpleCron{
		duration: duration,
		closed:   make(chan struct{}),
	}
	_ = s.start()
	return s
}

// SimpleCron 一个简单的周期性执行任务的时间管理器
type SimpleCron struct {
	lastTime int64
	running  bool
	duration time.Duration
	timer    *time.Timer
	lock     sync.Mutex
	jobs     []func()
	closed   chan struct{}
}

func (s *SimpleCron) start() error {
	if s.duration.Nanoseconds() == 0 {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.running || s.timer != nil {
		return nil
	}
	s.running = true
	s.timer = time.AfterFunc(s.duration, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.running {
			return
		}
		s.timer.Reset(s.next())
		for i := range s.jobs {
			job := s.jobs[i]
			go job()
		}
	})
	s.lastTime = nowFunc().Unix()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("Recovered in SimpleCron", r)
			}
		}()
		s.doCheck()
	}()
	return nil
}

func (s *SimpleCron) doCheck() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-timer.C:
			s.check()
		}
	}
}

func (s *SimpleCron) check() {
	s.lock.Lock()
	defer s.lock.Unlock()
	nowUnix := nowFunc().Unix()
	defer func() {
		s.lastTime = nowUnix
	}()
	if !s.running {
		return
	}
	diff := nowUnix - s.lastTime
	// 检查始终误差，如果误差过大需要重置始终
	if diff == 1 || diff == 2 {
		return
	}
	s.timer.Stop()
	s.timer.Reset(s.next())
}

func (s *SimpleCron) next() time.Duration {
	_, offsetSec := nowFunc().Zone()
	nowLocalTS := nowFunc().UnixNano() + int64(time.Duration(offsetSec)*time.Second)
	next := int64(s.duration) - nowLocalTS%int64(s.duration)
	return time.Duration(next)
}

func (s *SimpleCron) AddJob(job func()) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.jobs = append(s.jobs, job)
}

// Stop 停止定时器
func (s *SimpleCron) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.running {
		return
	}
	s.running = false
	if s.timer != nil {
		s.timer.Stop()
	}
	close(s.closed)
}
