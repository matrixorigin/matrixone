package message

import "sync"

var pool = sync.Pool{
	New: func() interface{} { return new(Message) },
}

func Acquire() *Message {
	m := pool.Get().(*Message)
	m.Reset()
	return m
}

func Release(m *Message) {
	pool.Put(m)
}
