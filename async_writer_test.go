package rotatefiles

import (
	"io"
	"reflect"
	"testing"
	"time"
)

func Test_asyncWriter_Flush(t *testing.T) {

	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name    string
		aw      AsyncWriter
		args    args
		wantErr bool
	}{
		{
			name: "success",
			aw: func() AsyncWriter {
				return NewAsyncWriter(NopCloser(io.Discard), 1024, WithTimeout(time.Millisecond))
			}(),
			args: args{
				timeout: time.Second,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.aw.Flush(tt.args.timeout); (err != nil) != tt.wantErr {
				t.Errorf("Flush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_asyncWriter_Stats(t *testing.T) {

	tests := []struct {
		name string
		aw   AsyncWriter
		want AsyncWriterStats
	}{
		{
			name: "success",
			aw: func() AsyncWriter {
				aw := NewAsyncWriter(NopCloser(io.Discard), 1024, WithTimeout(time.Millisecond))
				aw.Write([]byte("hello"))
				return aw
			}(),
			want: AsyncWriterStats{
				EnqueuedWrites: 1,
				EnqueuedBytes:  5,
				DroppedWrites:  0,
				WriteErrors:    0,
				QueueSize:      1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.aw.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_asyncWriter_Write(t *testing.T) {

	type args struct {
		p []byte
	}
	tests := []struct {
		name    string
		args    args
		wc      AsyncWriter
		wantN   int
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				p: []byte("hello world"),
			},
			wc:      NewAsyncWriter(NopCloser(io.Discard), 1024, WithTimeout(time.Millisecond)),
			wantN:   11,
			wantErr: false,
		},
		{
			name: "timeout_success",
			args: args{
				p: []byte("hello world"),
			},
			wc: func() AsyncWriter {
				aw := NewAsyncWriter(NopCloser(io.Discard), 0, WithTimeout(-10))
				return aw
			}(),
			wantN:   11,
			wantErr: false,
		},
		{
			name: "timeout_zero_success",
			args: args{
				p: []byte("hello world"),
			},
			wc:      NewAsyncWriter(NopCloser(io.Discard), 1024, WithTimeout(0)),
			wantN:   11,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotN, err := tt.wc.Write(tt.args.p)
			defer tt.wc.Close()
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("Write() gotN = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}
