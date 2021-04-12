package kv

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"os"
	"path"

	aio "github.com/traetox/goaio"
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

func (a *KV) Get(k string, proc *process.Process) ([]byte, *aio.AIO, aio.RequestId, error) {
	return readFile(path.Join(a.name, k), proc)
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
	data = data[:mempool.CountSize+size]
	id, err := a.ReadAt(data[mempool.CountSize:], 0)
	if err != nil {
		a.Close()
		return nil, nil, 0, err
	}
	return data, a, id, nil
}
