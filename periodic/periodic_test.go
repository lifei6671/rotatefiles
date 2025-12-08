package periodic

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type FileInfo struct {
	FileName string
}

func TestGeneratorFunc_Generate(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	type testCase[T any] struct {
		name    string
		f       GeneratorFunc[T]
		args    args
		want    T
		wantErr bool
	}
	tests := []testCase[FileInfo]{
		{
			name: "ok",
			f: func(ctx context.Context) (FileInfo, error) {
				return FileInfo{FileName: "test"}, nil
			},
			args: args{
				ctx: context.Background(),
			},
			want:    FileInfo{FileName: "test"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.f.Generate(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Generate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Generate() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPeriodicGenerator_Latest(t *testing.T) {
	type testCase[T any] struct {
		name string
		pg   *SimplePeriodicGenerator[T]
		want Result[T]
	}
	tests := []testCase[FileInfo]{
		{
			name: "ok",
			pg: func() *SimplePeriodicGenerator[FileInfo] {
				return NewPeriodicGeneratorFunc(func(ctx context.Context) (FileInfo, error) {
					return FileInfo{FileName: "test"}, nil
				}, time.Millisecond*100,
					WithImmediate[FileInfo](),
					WithRetryDelay[FileInfo](time.Millisecond*10),
					WithErrorHandler[FileInfo](func(err error) {
						t.Logf("PeriodicGenerator_Latest_Err:%s", err)
					}),
				)
			}(),
			want: Result[FileInfo]{
				Value: FileInfo{FileName: "test"},
			},
		},
		{
			name: "fail",
			pg: func() *SimplePeriodicGenerator[FileInfo] {
				return NewPeriodicGeneratorFunc(func(ctx context.Context) (FileInfo, error) {
					return FileInfo{}, errors.New("mock error")
				}, time.Millisecond*100,
					WithImmediate[FileInfo](),
					WithRetryDelay[FileInfo](time.Millisecond*10),
					WithErrorHandler[FileInfo](func(err error) {
						t.Logf("PeriodicGenerator_Latest_Err:%s", err)
					}),
				)
			}(),
			want: Result[FileInfo]{
				Value: FileInfo{FileName: ""},
			},
		},
	}
	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tt.pg.Start(ctx)
			defer tt.pg.Stop(ctx)
			time.Sleep(time.Millisecond * 110)
			if got := tt.pg.Latest(); got.Value.FileName != tt.want.Value.FileName {
				t.Errorf("Latest() = %v, want %v", got, tt.want)
			}
		})
	}
}
