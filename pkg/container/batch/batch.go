package batch

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/compress"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func New(ro bool, attrs []string) *Batch {
	return &Batch{
		Ro:    ro,
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func (bat *Batch) Reorder(attrs []string) {
	if bat.Ro {
		bat.Cow()
	}
	for i, name := range attrs {
		for j, attr := range bat.Attrs {
			if name == attr {
				bat.Is[i], bat.Is[j] = bat.Is[j], bat.Is[i]
				bat.Vecs[i], bat.Vecs[j] = bat.Vecs[j], bat.Vecs[i]
				bat.Attrs[i], bat.Attrs[j] = bat.Attrs[j], bat.Attrs[i]
			}
		}
	}
}

func (bat *Batch) Length(proc *process.Process) (int, error) {
	vec, err := bat.GetVector(bat.Attrs[0], proc)
	if err != nil {
		return -1, err
	}
	return vec.Length(), nil
}

func (bat *Batch) Prefetch(attrs []string, vecs []*vector.Vector, proc *process.Process) error {
	var err error

	for i, attr := range attrs {
		if vecs[i], err = bat.GetVector(attr, proc); err != nil {
			return err
		}
	}
	return nil
}

func (bat *Batch) GetVector(name string, proc *process.Process) (*vector.Vector, error) {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}
		if len(bat.Is) <= i || bat.Is[i].Wg == nil {
			return bat.Vecs[i], nil
		}
		// io wait
		if err := bat.Is[i].Wg.Wait(); err != nil {
			return nil, err
		}
		data := bat.Vecs[i].Data
		// decompress
		if bat.Is[i].Alg == compress.Lz4 {
			var err error

			n := int(encoding.DecodeInt32(data[len(data)-4:]))
			buf := proc.Mp.Alloc(n)
			if buf, err = compress.Decompress(data[mempool.CountSize:len(data)-4], buf[mempool.CountSize:], bat.Is[i].Alg); err != nil {
				proc.Mp.Free(buf)
				return nil, err
			}
			proc.Mp.Free(data)
			data = buf[:mempool.CountSize+n]
			bat.Vecs[i].Data = data
		}
		if err := bat.Vecs[i].Read(data); err != nil {
			return nil, err
		}
		copy(data, encoding.EncodeUint64(bat.Is[i].Ref))
		bat.Is[i].Wg = nil
		return bat.Vecs[i], nil
	}
	return nil, fmt.Errorf("attribute '%s' not exist", name)
}

func (bat *Batch) WaitIo() {
	for _, i := range bat.Is {
		if i.Wg != nil {
			i.Wg.Wait()
			i.Wg = nil
		}
	}
}

func (bat *Batch) Clean(proc *process.Process) {
	bat.WaitIo()
	if bat.SelsData != nil {
		proc.Free(bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	for _, vec := range bat.Vecs {
		if vec.Data != nil {
			copy(vec.Data, mempool.OneCount)
			vec.Free(proc)
		}
	}
}

func (bat *Batch) Reduce(attrs []string, proc *process.Process) {
	if bat.Ro {
		bat.Cow()
	}
	for _, attr := range attrs {
		for i := 0; i < len(bat.Attrs); i++ {
			if bat.Attrs[i] != attr {
				continue
			}
			bat.Vecs[i].Free(proc)
			if bat.Vecs[i].Data == nil {
				if len(bat.Is) > i {
					bat.Is = append(bat.Is[:i], bat.Is[i+1:]...)
				}
				bat.Vecs = append(bat.Vecs[:i], bat.Vecs[i+1:]...)
				bat.Attrs = append(bat.Attrs[:i], bat.Attrs[i+1:]...)
				i--
			}
			break
		}
	}
}

func (bat *Batch) Cow() {
	attrs := make([]string, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrs[i] = attr
	}
	bat.Ro = false
	bat.Attrs = attrs
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	if len(bat.Sels) > 0 {
		fmt.Printf("%v\n", bat.Sels)
	}
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
