//go:build windows

package fileutil

import (
	"os"
	"syscall"
)

func sameFileByID(f1, f2 string) (bool, error) {
	id1, err := getFileID(f1)
	if err != nil {
		return false, err
	}
	id2, err := getFileID(f2)
	if err != nil {
		return false, err
	}
	return id1.VolumeSerialNumber == id2.VolumeSerialNumber &&
		id1.FileIndexHigh == id2.FileIndexHigh &&
		id1.FileIndexLow == id2.FileIndexLow, nil
}

type fileID struct {
	VolumeSerialNumber uint32
	FileIndexHigh      uint32
	FileIndexLow       uint32
}

func getFileID(path string) (*fileID, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := syscall.Handle(f.Fd())

	var info syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(h, &info)
	if err != nil {
		return nil, err
	}

	return &fileID{
		VolumeSerialNumber: info.VolumeSerialNumber,
		FileIndexHigh:      info.FileIndexHigh,
		FileIndexLow:       info.FileIndexLow,
	}, nil
}
