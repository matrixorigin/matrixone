package entry

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type Entry struct {
	Entry entry.Entry
	Info  *entry.Info //for wal in post append
	Lsn   uint64
	Ctx   any //for addr in batchstore
	err   error
	wg    *sync.WaitGroup
}

func NewEntry(e entry.Entry) *Entry {
	en := &Entry{
		Entry: e,
		wg:    &sync.WaitGroup{},
	}
	en.wg.Add(1)
	return en
}
func NewEmptyEntry() *Entry {
	en := &Entry{
		Entry: entry.GetBase(),
		wg:    &sync.WaitGroup{},
	}
	en.wg.Add(1)
	return en
}
func (e *Entry) SetInfo() {
	info := e.Entry.GetInfo()
	if info != nil {
		e.Info = info.(*entry.Info)
	}
}
func (e *Entry) ReadFrom(r io.Reader) {
	if err := binary.Read(r, binary.BigEndian, &e.Lsn); err != nil {
		return
	}
	e.Entry.ReadFrom(r)
}

func (e *Entry) ReadAt(r *os.File, offset int) (int, error) {
	lsnbuf := make([]byte, 8)
	n, err := r.ReadAt(lsnbuf, int64(offset))
	if err != nil {
		return n, err
	}
	offset += 8

	bbuf := bytes.NewBuffer(lsnbuf)
	if err := binary.Read(bbuf, binary.BigEndian, &e.Lsn); err != nil {
		return n, err
	}

	n2, err := e.Entry.ReadAt(r, offset)
	return n2 + n, err
}

func (e *Entry) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, e.Lsn); err != nil {
		return 0, err
	}
	n, err := e.Entry.WriteTo(w)
	n += 8
	return n, err
}

func (e *Entry) WaitDone() error {
	e.wg.Wait()
	return e.err
}

func (e *Entry) DoneWithErr(err error) {
	e.err = err
	info := e.Entry.GetInfo()
	if info != nil {
		e.Info = info.(*entry.Info)
	}
	e.wg.Done()
}

func (e *Entry) GetSize() int {
	return e.Entry.TotalSize() + 8 //LSN
}
