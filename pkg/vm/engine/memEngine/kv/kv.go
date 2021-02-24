package kv

import (
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func New() *KV {
	return &KV{make(map[string][]byte)}
}

func (a *KV) Close() error {
	return nil
}

func (a *KV) Del(k string) error {
	delete(a.mp, k)
	return nil
}

func (a *KV) Set(k string, v []byte) error {
	a.mp[k] = v
	return nil
}

func (a *KV) Get(k string, proc *process.Process) ([]byte, error) {
	v, ok := a.mp[k]
	if !ok {
		return nil, NotExist
	}
	data, err := proc.Alloc(int64(len(v)))
	if err != nil {
		return nil, err
	}
	copy(data[mempool.CountSize:], v)
	return data[:len(v)+mempool.CountSize], nil
}
