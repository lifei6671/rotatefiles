package fileutil

import (
	"math/rand"
	"os"
	"strconv"
	"time"
)

// KeepDirExists 如果目录不存在则创建目录
func KeepDirExists(dir string) error {
	info, statErr := os.Stat(dir)
	if statErr == nil && info.IsDir() {
		return nil
	}

	// 文件存在，但是不是目录,将其重命名
	if statErr == nil {
		newName := dir + "_not_dir_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		if errRename := os.Rename(dir, newName); errRename != nil {
			if !os.IsNotExist(errRename) {
				return errRename
			}
		}
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// FileExists 判断指定路径的文件或目录是否存在
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

// IsSameFile 判断两个文件是否相同
func IsSameFile(f1, f2 string) bool {
	s1, e1 := os.Stat(f1)
	s2, e2 := os.Stat(f2)
	if e1 != nil || e2 != nil {
		return false
	}
	return os.SameFile(s1, s2)
}

// RandomDelay 依据周期时间生成随机延迟时间
// 以尽可能避开整点时间
func RandomDelay(cycle time.Duration) time.Duration {
	const (
		minDelay      = 5 * time.Second
		maxDelayRatio = 2
		hourDelay     = time.Hour
		fiveMinDelay  = 5 * time.Minute
		minThreshold  = 60 * time.Second
	)

	if cycle > hourDelay {
		cycle = hourDelay
	}

	maxVal := int(cycle.Seconds() / maxDelayRatio)
	if maxVal <= 0 {
		maxVal = int(minThreshold.Seconds())
	}

	minVal := int(minDelay.Seconds())
	if cycle > fiveMinDelay {
		minVal = int(minThreshold.Seconds())
	}

	val := time.Duration(minVal+rand.Intn(minVal)) * time.Second
	return val
}
