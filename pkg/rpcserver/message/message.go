package message

import "sync"

var pool = sync.Pool{
	New: func() interface{} { return new(Message) },
}

func Acquire() *Message {
	return pool.Get().(*Message)
}

func Release(m *Message) {
	pool.Put(m)
}
