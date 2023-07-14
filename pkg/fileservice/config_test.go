package fileservice

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetRawBackendByBackend(t *testing.T) {
	type args struct {
		src string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: memFileServiceBackend, args: args{src: memFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: diskFileServiceBackend, args: args{src: diskFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: diskETLFileServiceBackend, args: args{src: diskETLFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: s3FileServiceBackend, args: args{src: s3FileServiceBackend}, want: s3FileServiceBackend},
		{name: minioFileServiceBackend, args: args{src: minioFileServiceBackend}, want: minioFileServiceBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetRawBackendByBackend(tt.args.src), "GetRawBackendByBackend(%v)", tt.args.src)
		})
	}
}
