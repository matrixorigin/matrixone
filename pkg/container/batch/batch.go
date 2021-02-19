package batch

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/compress"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/process"
)

func New(attrs []string) *Batch {
	return &Batch{
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func (bat *Batch) GetVector(name string, mp *mempool.Mempool, proc *process.Process) (*vector.Vector, error) {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}
		if bat.Is == nil {
			return bat.Vecs[i], nil
		}
		// io wait
		if bat.Is[i].Wg != nil {
			if err := bat.Is[i].Wg.Wait(); err != nil {
				return nil, err
			}
		}
		data := bat.Vecs[i].Data
		// decompress
		if bat.Is[i].Alg == compress.Lz4 {
			var err error

			n := int(encoding.DecodeInt32(data[len(data)-4:]))
			buf := mp.Alloc(n)
			if buf, err = compress.Decompress(data[mempool.CountSize:len(data)-4], buf[mempool.CountSize:], bat.Is[i].Alg); err != nil {
				mp.Free(buf)
				mp.Free(data)
				return nil, err
			}
			mp.Free(data)
			data = buf[:mempool.CountSize+n]
		}
		if err := bat.Vecs[i].Read(data, proc); err != nil {
			mp.Free(data)
			return nil, err
		}
		return bat.Vecs[i], nil
	}
	return nil, fmt.Errorf("attribute '%s' not exist", name)
}

func (bat *Batch) Free(p *process.Process, mp *mempool.Mempool) {
	for _, vec := range bat.Vecs {
		vec.Free(p, mp)
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, attr := range bat.Attrs {
		buf.WriteString(fmt.Sprintf("%s\n", attr))
		buf.WriteString(fmt.Sprintf("\t%s\n", bat.Vecs[i]))
	}
	return buf.String()
}

func (w *WaitGroup) Wait() error {
	_, err := w.Ap.WaitFor(w.Id)
	w.Ap.Close()
	return err
}
