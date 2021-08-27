package kv

import (
	"matrixone/pkg/vm/process"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

func New(name string) (*KV, error) {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		if err := os.Mkdir(name, os.FileMode(0775)); err != nil {
			return nil, err
		}
	}
	return &KV{name}, nil
}

func (a *KV) Close() error {
	return nil
}

func (a *KV) Del(k string) error {
	return os.Remove(path.Join(a.name, k))
}

func (a *KV) Set(k string, v []byte) error {
	return os.WriteFile(path.Join(a.name, k), v, os.FileMode(0666))
}

func (a *KV) GetCopy(k string) ([]byte, error) {
	v, err := os.ReadFile(path.Join(a.name, k))
	if os.IsNotExist(err) {
		err = nil
	}
	return v, err
}

func (a *KV) Get(k string, proc *process.Process) ([]byte, error) {
	return readFile(path.Join(a.name, k))
}

func readFile(name string) ([]byte, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(name, unix.O_RDWR|unix.O_DIRECT, 0664)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	return unix.Mmap(fd, 0, int(fi.Size()), unix.PROT_WRITE|unix.PROT_READ, unix.MAP_PRIVATE)
}
