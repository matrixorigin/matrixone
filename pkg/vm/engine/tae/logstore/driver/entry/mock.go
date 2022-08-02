package entry

import (
	"bytes"
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var buf []byte

func init() {
	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf = bs.Bytes()
}

func MockEntry() *Entry {
	payloadSize := 100

	e := entry.GetBase()
	info := &entry.Info{GroupLSN: uint64(rand.Intn(1000))}
	e.SetInfo(info)
	payload := make([]byte, payloadSize)
	copy(payload, buf)
	err := e.SetPayload(payload)
	if err != nil {
		panic(err)
	}
	e.PrepareWrite()
	return NewEntry(e)
}
