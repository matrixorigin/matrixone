package s3

import (
	"bytes"
	"encoding/gob"
	"matrixbase/pkg/vm/process"
	"os"
	"path"

	aio "github.com/traetox/goaio"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func init() {
	gob.Register(Config{})
}

func New(cfg *Config) (*KV, error) {
	a := new(KV)
	a.cfg = cfg
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         &cfg.Endpoint,
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.AccessKeySecret, ""),
	})
	if err != nil {
		return nil, err
	}
	a.cli, a.sess = s3.New(sess), sess
	return a, nil
}

func (a *KV) Del(name string) error {
	if _, err := a.cli.DeleteObject(&s3.DeleteObjectInput{
		Key:    aws.String(name),
		Bucket: aws.String(a.cfg.Bucket),
	}); err != nil {
		return err
	}
	return nil
}

func (a *KV) Set(k string, v []byte) error {
	_, err := s3manager.NewUploader(a.sess).Upload(&s3manager.UploadInput{
		Key:    aws.String(k),
		Body:   bytes.NewReader(v),
		Bucket: aws.String(a.cfg.Bucket),
	})
	return err
}

func (a *KV) Get(k string, proc *process.Process) ([]byte, *aio.AIO, aio.RequestId, error) {
	name := path.Join(a.cfg.Path, k)
	fp, err := os.Create(name)
	if err != nil {
		return nil, nil, 0, err
	}
	if _, err := s3manager.NewDownloader(a.sess).Download(fp, &s3.GetObjectInput{
		Key:    aws.String(k),
		Bucket: aws.String(a.cfg.Bucket),
	}); err != nil {
		fp.Close()
		return nil, nil, 0, err
	}
	fp.Close()
	return readFile(name, proc)
}

func readFile(name string, proc *process.Process) ([]byte, *aio.AIO, aio.RequestId, error) {
	a, err := aio.NewAIO(name, os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, 0, err
	}
	fi, err := os.Stat(name)
	if err != nil {
		return nil, nil, 0, err
	}
	size := fi.Size()
	data, err := proc.Alloc(size)
	if err != nil {
		return nil, nil, 0, err
	}
	id, err := a.ReadAt(data, 0)
	if err != nil {
		a.Close()
		return nil, nil, 0, err
	}
	return data, a, id, nil
}
