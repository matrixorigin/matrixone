package kv

import "matrixbase/pkg/vm/mempool"

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

func (a *KV) Get(k string, mp *mempool.Mempool) ([]byte, error) {
	v, ok := a.mp[k]
	if !ok {
		return nil, NotExist
	}
	data := mp.Alloc(len(v))
	copy(data[mempool.CountSize:], v)
	return data[:len(v)+mempool.CountSize], nil
}
