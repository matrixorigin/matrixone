package kv

import "bytes"

func New() *KV {
	kv := &KV{}
	kv.mp = make(map[string][]byte)
	return kv
}

func (a *KV) Close() error {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()
	var keys []string
	for k, _ := range a.mp {
		keys = append(keys,k)
	}
	for _, k := range keys {
		delete(a.mp,k)
	}
	return nil
}

func (a *KV) Del(k string) error {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()
	delete(a.mp, k)
	return nil
}

func (a *KV) Set(k string, v []byte) error {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()
	a.mp[k] = v
	return nil
}

func (a *KV) Get(k string, buf *bytes.Buffer) ([]byte, error) {
	a.rwlock.RLock()
	defer a.rwlock.RUnlock()
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
	a.rwlock.RLock()
	defer a.rwlock.RUnlock()
	return len(a.mp)
}

func (a *KV) Keys() []string {
	a.rwlock.RLock()
	defer a.rwlock.RUnlock()
	var keys []string
	for key, _ := range a.mp {
		keys = append(keys, key)
	}
	return keys
}
