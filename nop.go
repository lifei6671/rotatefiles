package rotatefiles

import (
	"io"
)

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error {
	return nil
}

// NopCloser  不需要关闭的 writer 包装
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{Writer: w}
}
