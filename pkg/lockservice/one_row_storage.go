package lockservice

import "bytes"

type oneRowStorage struct {
	key   []byte
	value Lock
}

func newOneRowStorage() LockStorage {
	return &oneRowStorage{}
}

func (k *oneRowStorage) Add(key []byte, value Lock) {
	if !bytes.Equal(k.key, key) {
		panic("invalid use one row storage")
	}
	k.value = value
}

func (k *oneRowStorage) Get(key []byte) (Lock, bool) {
	if !bytes.Equal(k.key, key) {
		panic("invalid use one row storage")
	}

	return k.value, k.value.isEmpty()
}

func (k *oneRowStorage) Len() int {
	if k.value.isEmpty() {
		return 0
	}
	return 1
}

func (k *oneRowStorage) Delete(key []byte) (Lock, bool) {
	if !bytes.Equal(k.key, key) {
		panic("invalid use one row storage")
	}
	v := k.value
	k.value = Lock{}
	return v, v.isEmpty()
}

func (k *oneRowStorage) Seek(key []byte) ([]byte, Lock, bool) {
	if !bytes.Equal(k.key, key) {
		panic("invalid use one row storage")
	}
	return key, k.value, k.value.isEmpty()
}

func (k *oneRowStorage) Prev(key []byte) ([]byte, Lock, bool) {
	panic("not support one row storage")
}

func (k *oneRowStorage) Range(
	start, end []byte,
	fn func([]byte, Lock) bool) {
	panic("not support one row storage")
}

func (k *oneRowStorage) Iter(fn func([]byte, Lock) bool) {
	if k.value.isEmpty() {
		return
	}
	fn(k.key, k.value)
}

func (k *oneRowStorage) Clear() {
	k.value = Lock{}
}
