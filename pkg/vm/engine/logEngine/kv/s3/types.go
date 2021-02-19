package s3

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

/*
type KV interface {
	Del(string) error
	Set(string, []byte) error
	Get(string) ([]byte, *aio.AIO, aio.RequestId, bool, error)
}
*/

type Config struct {
	Path            string
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
}

type KV struct {
	cli  *s3.S3
	cfg  *Config
	sess *session.Session
}
