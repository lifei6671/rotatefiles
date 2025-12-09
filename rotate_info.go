package rotatefiles

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lifei6671/rotatefiles/internal/fileutil"
)

// RotateInfo 用于描述日志文件与软链之间的绑定关系
//
//	RawFilename:   原始配置中的文件路径（未被格式化或扩展）
//	Symlink:       指向当前写入文件的符号链接，例如 "app.log"
//	Filename:      实际写入的物理文件，例如 "app.log.20250101"
type RotateInfo struct {
	// 日志软链
	Symlink string
	// 原始日志路径：./log/app.log
	Filename string
	// 实际软链指向的文件地址： ./log/app.log.2025111111
	FilePath string
	// 使用到的规则
	RuleName string
}

// Equal 判断两个配置是否一致
func (fi RotateInfo) Equal(o RotateInfo) bool {
	return fi.Symlink == o.Symlink && fi.Filename == o.Filename && fi.FilePath == o.FilePath
}

// IsNeedSymlink 判断软链是否和目标文件一致
func (fi RotateInfo) IsNeedSymlink() bool {
	return fi.Symlink != "" && fi.Symlink != fi.FilePath
}

// CheckSymlink 保证 Symlink 必须正确指向 Filename。
// 在以下所有情况下它都会自行处理：
//   - Symlink 不存在：自动创建
//   - Symlink 存在但指向错误文件：移除并重新创建
//   - Symlink 是损坏链接：覆盖
//   - Symlink 是普通文件或非法节点：自动备份并创建新链接
//   - 多进程/多 writer 并发竞争：采用 retry 策略消除竞争影响
//
// 该方法是生产级日志轮转的基础设施，保证 “当前日志文件” 的链接引用一致性。
func (fi RotateInfo) CheckSymlink() error {
	if !fi.IsNeedSymlink() {
		// 不需要创建符号链接（例如直接写文件，不使用 symlink）
		return nil
	}
	if !fileutil.FileExists(fi.FilePath) {
		return fmt.Errorf("CheckSymlink: target file does not exist: %q", fi.FilePath)
	}

	symDir := filepath.Dir(fi.Symlink)
	// 确保软链所在目录存在
	if err := fileutil.KeepDirExists(symDir); err != nil {
		return fmt.Errorf("CheckSymlink: cannot create dir %q: %w", symDir, err)
	}

	// -------------------------
	// 1. 处理已存在的 Symlink 路径
	// -------------------------
	if fileutil.FileExists(fi.Symlink) {
		// 判断是否是符号链接
		if _, err := os.Readlink(fi.Symlink); err == nil {
			// 已经是符号链接，检查是否指向正确目标
			if fileutil.IsSameFile(fi.Symlink, fi.FilePath) {
				// 正常情况，已经指向目标文件，直接返回
				return nil
			}

			// 是符号链接但指向错误 → 删除以覆盖
			if errRm := os.Remove(fi.Symlink); errRm != nil && !os.IsNotExist(errRm) {
				return fmt.Errorf("CheckSymlink: failed to remove old symlink %q: %w", fi.Symlink, errRm)
			}
		} else {
			// 不是符号链接或损坏（可能是普通文件 / 目录 / 权限异常）
			backup := fmt.Sprintf("%s.bak_%s", fi.Symlink, time.Now().Format("20060102150405"))
			if errRe := os.Rename(fi.Symlink, backup); errRe != nil && !os.IsNotExist(errRe) {
				return fmt.Errorf("CheckSymlink: failed to backup non-symlink %q: %w", fi.Symlink, errRe)
			}
		}
	}

	// -------------------------
	// 2. 构造相对路径
	// -------------------------
	target := fi.Filename
	if rel, err := filepath.Rel(symDir, fi.FilePath); err == nil {
		target = rel
	}

	// -------------------------
	// 3. 创建符号链接（带 retry）
	// -------------------------
	const retry = 3
	var createErr error

	for i := 0; i < retry; i++ {
		createErr = os.Symlink(target, fi.Symlink)
		if createErr == nil {
			return nil
		}

		// 处理并发写入场景：
		// 如果其他进程创建了 Symlink，则验证是否正确指向
		if os.IsExist(createErr) {
			if fileutil.IsSameFile(fi.Symlink, fi.FilePath) {
				return nil // 已经被别人正确创建
			}
			// 否则删除并重试
			_ = os.Remove(fi.Symlink)
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// 非 IsExist 错误直接返回
		return fmt.Errorf("CheckSymlink: os.Symlink(%q → %q) failed: %w", target, fi.Symlink, createErr)
	}

	return fmt.Errorf("CheckSymlink: failed after %d retries: %w", retry, createErr)
}
