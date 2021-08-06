package fastmap

import (
	"sync"
)

var Pool = sync.Pool{
	New: func() interface{} {
		return New()
	},
}

func New() *Map {
	vs := make([][]int, Group)
	ks := make([][]uint64, Group)
	for i := 0; i < Group; i++ {
		vs[i] = make([]int, 0, 16)
		ks[i] = make([]uint64, 0, 16)
	}
	return &Map{Ks: ks, Vs: vs}
}

func (m *Map) Reset() {
	for i := 0; i < Group; i++ {
		m.Ks[i] = m.Ks[i][:0]
		m.Vs[i] = m.Vs[i][:0]
	}
}

func (m *Map) Set(k uint64, v int) {
	slot := k & GroupMask
	m.Vs[slot] = append(m.Vs[slot], v)
	m.Ks[slot] = append(m.Ks[slot], k)
	return
}

func (m *Map) Get(k uint64) (int, bool) {
	slot := k & GroupMask
	if i := Find(m.Ks[slot], k); i != -1 {
		return m.Vs[slot][i], true
	}
	return -1, false
}
