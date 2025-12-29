//go:build !windows

package fileutil

import "errors"

func sameFileByID(_, _ string) (bool, error) {
	return false, errors.New("not supported")
}
