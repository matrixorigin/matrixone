package kv

import "bytes"

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

func (a *KV) Get(k string, buf *bytes.Buffer) ([]byte, error) {
	v, ok := a.mp[k]
	if !ok {
		return nil, NotExist
	}
	buf.Reset()
	if len(v) > buf.Cap() {
		buf.Grow(len(v))
	}
	data := buf.Bytes()[:len(v)]
	copy(data, v)
	return data, nil
}

func (a *KV) Size() int {
	return len(a.mp)
}

func (a *KV) Keys() []string {
	var keys []string
	for key, _ := range a.mp {
		keys = append(keys, key)
	}
	return keys
}
