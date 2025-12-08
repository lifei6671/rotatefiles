package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"time"

	"github.com/lifei6671/rotatefiles"
)

func main() {
	filename, _ := filepath.Abs("./app.log")
	generator, err := rotatefiles.NewSimpleRotateGenerator("1min", filename, func(err error) {
		log.Println(err)
	})
	if err != nil {
		log.Fatalf("new rotatefiles failed: %s", err)
	}
	_ = generator.Start(context.Background())
	opt := &rotatefiles.RotateOption{
		RotateGenerator: generator,
		NewWriter: func(ctx context.Context, w io.WriteCloser) (rotatefiles.AsyncWriter, error) {
			return rotatefiles.NewAsyncWriter(w, 2014, rotatefiles.WithTimeout(time.Microsecond*10)), nil
		},
		FlushDuration: time.Second,
		MaxFileNum:    10,
	}

	rotateFile, err := rotatefiles.NewRotateFile(opt, rotatefiles.WithOnErr(func(err error) {
		log.Println(err)
	}))
	if err != nil {
		log.Fatalf("new rotatefile failed: %s", err)
	}
	defer rotateFile.Close()

	for i := 0; i < 500; i++ {
		timer := time.NewTicker(time.Second)
		go func() {
			for t := range timer.C {
				rotateFile.Write([]byte(fmt.Sprintf("%d - %d hello world\n", i, t.Unix())))
			}
		}()
		time.Sleep(time.Second * 5)
		timer.Stop()
	}

}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
