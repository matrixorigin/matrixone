package entry

import (
	"bytes"
	"math/rand"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	info := &entry.Info{GroupLSN: uint64(rand.Intn(10))}
	e.SetInfo(info)
	payload := make([]byte, payloadSize)
	copy(payload, buf)
	e.SetPayload(payload)
	return NewEntry(e)
}
