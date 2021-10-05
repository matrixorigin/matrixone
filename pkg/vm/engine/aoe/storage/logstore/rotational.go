package logstore

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

var (
	DefaultSuffix string = ".rot"
)

type Rotational struct {
	Dir         string
	NamePrefix  string
	NameSuffix  string
	MaxSize     int
	file        *os.File
	mu          sync.RWMutex
	currSize    int64
	currVersion uint64
}

func NewRotational(dir, prefix, suffix string, maxSize int) (*Rotational, error) {
	rot := &Rotational{
		Dir:        dir,
		NamePrefix: prefix,
		NameSuffix: suffix,
		MaxSize:    maxSize,
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return rot, nil
}

func (r *Rotational) nextFileName() string {
	v := atomic.AddUint64(&r.currVersion, uint64(1))
	base := fmt.Sprintf("%s-%d%s", r.NamePrefix, v-1, r.NameSuffix)
	return path.Join(r.Dir, base)
}

func (r *Rotational) scheduleNew() error {
	name := r.nextFileName()
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	r.currSize = 0
	r.file = f
	return nil
}

func (r *Rotational) scheduleRotate() error {
	if err := r.file.Close(); err != nil {
		return err
	}

	return r.scheduleNew()
}

func (r *Rotational) Write(buf []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delta := int64(len(buf))
	if delta > int64(r.MaxSize) {
		return n, errors.New(fmt.Sprintf("MaxSize is %d, but %d is received", r.MaxSize, delta))
	}
	if r.file == nil {
		if err = r.scheduleNew(); err != nil {
			return n, err
		}
	}
	if r.currSize+delta > int64(r.MaxSize) {
		if err = r.scheduleRotate(); err != nil {
			return n, err
		}
	}
	n, err = r.file.Write(buf)
	r.currSize += int64(n)
	return n, err
}

func (r *Rotational) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
