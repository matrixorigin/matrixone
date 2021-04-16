package mergeorder

import (
	"bytes"
	"container/heap"
	"fmt"
	"matrixone/pkg/compare"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/google/uuid"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("])"))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	uuid, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	ctr.spill.id = fmt.Sprintf("%s.%v", proc.Id, uuid)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			ctr.spill.e = n.E
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				return true, err
			}
			if len(ctr.ptns) == 0 {
				ctr.state = EmitPrepare
			} else {
				ctr.state = Merge
			}
		case Emit:
			if err := ctr.emit(proc); err != nil {
				return true, err
			}
			ctr.bat.Reduce(ctr.attrs, proc)
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			if len(ctr.ptn.heap) == 0 {
				ctr.clean(proc)
				return true, nil
			}
			return false, nil
		case Merge:
			if err := ctr.merge(proc); err != nil {
				ctr.clean(proc)
				return true, err
			}
			ctr.state = MergeEmit
		case MergeEmit:
			if err := ctr.mergeemit(proc); err != nil {
				ctr.clean(proc)
				return true, err
			}
			ctr.bat.Reduce(ctr.attrs, proc)
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			if len(ctr.heap) == 0 {
				ctr.clean(proc)
				return true, nil
			}
			return false, nil
		case EmitPrepare:
			for _, bat := range ctr.ptn.bats {
				if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
					ctr.clean(proc)
					return true, err
				}
			}
			ptn := ctr.ptn
			ptn.mins = make([]int64, len(ptn.bats))
			ptn.lens = make([]int64, len(ptn.bats))
			for i, bat := range ptn.bats {
				if n := len(bat.Sels); n > 0 {
					ptn.lens[i] = int64(n)
				} else {
					n, err := bat.Length(proc)
					if err != nil {
						ctr.clean(proc)
						return true, err
					}
					ptn.lens[i] = int64(n)
				}
			}
			ctr.state = Emit
		}
	}
	return false, nil
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.attrs == nil {
				ctr.attrs = make([]string, len(n.Fs))
				for i, f := range n.Fs {
					ctr.attrs[i] = f.Attr
				}
				bat.Reorder(ctr.attrs)
				ctr.spill.attrs = make([]string, len(bat.Attrs))
				for i, attr := range bat.Attrs {
					ctr.spill.attrs[i] = attr
				}
				ctr.cmps = make([]compare.Compare, len(n.Fs))
				for i, f := range n.Fs {
					ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
				}
				ctr.spill.cs = make([]uint64, len(bat.Attrs))
				ctr.spill.md = make([]metadata.Attribute, len(bat.Attrs))
				for i, attr := range bat.Attrs {
					vec, err := bat.GetVector(attr, proc)
					if err != nil {
						bat.Clean(proc)
						return err
					}
					ctr.spill.md[i] = metadata.Attribute{
						Name: attr,
						Type: vec.Typ,
						Alg:  compress.Lz4,
					}
					ctr.spill.cs[i] = encoding.DecodeUint64(vec.Data[:mempool.CountSize])
				}
			} else {
				bat.Reorder(ctr.spill.attrs)
			}
			if ctr.ptn == nil {
				ctr.ptn = &partition{cmps: ctr.cmps}
			}
			if n := len(bat.Sels); n > 0 {
				ctr.ptn.rows += int64(n)
			} else {
				n, err := bat.Length(proc)
				if err != nil {
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
				ctr.ptn.rows += int64(n)
			}
			if ctr.ptn.rows > proc.Lim.PartitionRows {
				if err := ctr.newSpill(ctr.ptn, proc); err != nil {
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
				for i, pbat := range ctr.ptn.bats {
					if err := pbat.Prefetch(pbat.Attrs, pbat.Vecs, proc); err != nil {
						reg.Wg.Done()
						bat.Clean(proc)
						ctr.ptn.bats = ctr.ptn.bats[i:]
						return err
					}
					if err := ctr.ptn.r.Write(pbat); err != nil {
						reg.Wg.Done()
						bat.Clean(proc)
						ctr.ptn.bats = ctr.ptn.bats[i:]
						return err
					}
					pbat.Clean(proc)
				}
				ctr.ptn.bats = nil
				if err := ctr.ptn.r.Write(bat); err != nil {
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
				bat.Clean(proc)
				ctr.ptns = append(ctr.ptns, ctr.ptn)
				ctr.ptn = nil
			} else {
				ctr.ptn.bats = append(ctr.ptn.bats, bat)
			}
			reg.Wg.Done()
		}
	}
	if ctr.ptn != nil && len(ctr.ptns) > 0 {
		if err := ctr.newSpill(ctr.ptn, proc); err != nil {
			return err
		}
		for i, bat := range ctr.ptn.bats {
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				ctr.ptn.bats = ctr.ptn.bats[i:]
				return err
			}
			if err := ctr.ptn.r.Write(bat); err != nil {
				ctr.ptn.bats = ctr.ptn.bats[i:]
				return err
			}
			bat.Clean(proc)
		}
	}
	return nil
}

func (ctr *Container) emit(proc *process.Process) error {
	if ctr.ptn.heap == nil {
		ctr.ptn.heap = make([]int, len(ctr.ptn.bats))
		for i := range ctr.ptn.bats {
			ctr.ptn.heap[i] = i
		}
		heap.Init(ctr.ptn)
	}
	if ctr.bat == nil {
		ctr.bat = batch.New(true, ctr.spill.attrs)
		for i := range ctr.spill.md {
			ctr.bat.Vecs[i] = vector.New(ctr.spill.md[i].Type)
		}
	}
	return ctr.ptn.emit(ctr.bat, proc)
}

func (ctr *Container) merge(proc *process.Process) error {
	var bat *batch.Batch

	for _, ptn := range ctr.ptns {
		if err := ctr.loads(ptn, proc); err != nil {
			return err
		}
		if ptn.heap == nil {
			ptn.heap = make([]int, len(ptn.bats))
			for i := range ptn.bats {
				ptn.heap[i] = i
			}
			heap.Init(ptn)
		}
		segs := ptn.r.Segments()
		for len(ptn.heap) > 0 {
			if bat == nil {
				bat = batch.New(true, ctr.spill.attrs)
				for i := range ctr.spill.md {
					bat.Vecs[i] = vector.New(ctr.spill.md[i].Type)
				}
			}
			if err := ptn.emit(bat, proc); err != nil {
				return err
			}
			if err := ptn.r.Write(bat); err != nil {
				return err
			}
			bat.Clean(proc)
			bat = nil
		}
		ptn.segs = ptn.r.Segments()[len(segs):]
		for _, bat := range ptn.bats {
			bat.Clean(proc)
		}
		ptn.bats = nil
	}
	return nil
}

func (ctr *Container) mergeemit(proc *process.Process) error {
	if ctr.heap == nil {
		ctr.heap = make([]int, len(ctr.ptns))
		for i := range ctr.heap {
			ctr.heap[i] = i
		}
		ctr.mins = make([]int64, len(ctr.heap))
		ctr.lens = make([]int64, len(ctr.heap))
		for i, ptn := range ctr.ptns {
			if err := ctr.load(ptn, proc); err != nil {
				return err
			}
			if n := len(ptn.bat.Sels); n > 0 {
				ctr.lens[i] = int64(n)
			} else {
				n, err := ptn.bat.Length(proc)
				if err != nil {
					return err
				}
				ctr.lens[i] = int64(n)
			}
		}
		heap.Init(ctr)
	}
	if ctr.bat == nil {
		ctr.bat = batch.New(true, ctr.spill.attrs)
		for i := range ctr.spill.md {
			ctr.bat.Vecs[i] = vector.New(ctr.spill.md[i].Type)
		}
	}
	for row := int64(0); row < proc.Lim.BatchRows; row++ {
		if len(ctr.heap) == 0 {
			break
		}
		idx := ctr.heap[0]
		min := ctr.mins[idx]
		if len(ctr.ptns[idx].bat.Sels) > 0 {
			min = ctr.ptns[idx].bat.Sels[min]
		}
		for i, vec := range ctr.bat.Vecs {
			if vec.Data == nil {
				if err := vec.UnionOne(ctr.ptns[idx].bat.Vecs[i], min, proc); err != nil {
					return err
				}
				copy(vec.Data[:mempool.CountSize], ctr.ptns[idx].bat.Vecs[i].Data[:mempool.CountSize])
			} else {
				if err := vec.UnionOne(ctr.ptns[idx].bat.Vecs[i], min, proc); err != nil {
					return err
				}
			}
		}
		if ctr.mins[idx]+1 < ctr.lens[idx] {
			ctr.mins[idx]++
			heap.Fix(ctr, 0)
		} else {
			ptn := ctr.ptns[idx]
			ptn.bat.Clean(proc)
			if err := ctr.load(ptn, proc); err != nil {
				return err
			}
			ctr.mins[idx] = 0
			if ptn.bat == nil {
				heap.Remove(ctr, 0)
			} else if n := len(ptn.bat.Sels); n > 0 {
				ctr.lens[idx] = int64(n)
			} else {
				n, err := ptn.bat.Length(proc)
				if err != nil {
					return err
				}
				ctr.lens[idx] = int64(n)
			}
		}
	}
	return nil
}

func (ctr *Container) load(ptn *partition, proc *process.Process) error {
	if ptn.r == nil {
		ptn.bat = ptn.bats[0]
		ptn.bats = ptn.bats[1:]
		return nil
	}
	if len(ptn.segs) == 0 {
		ptn.bat = nil
		return nil
	}
	bat, err := ptn.r.Segment(ptn.segs[0], proc).Read(ctr.spill.cs, ctr.spill.attrs, proc)
	if err != nil {
		return err
	}
	if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
		bat.Clean(proc)
		return err
	}
	ptn.bat = bat
	ptn.segs = ptn.segs[1:]
	return nil
}

func (ctr *Container) loads(ptn *partition, proc *process.Process) error {
	if ptn.bats == nil {
		ptn.bats = make([]*batch.Batch, 0, len(ptn.segs))
		segs := ptn.r.Segments()
		for _, seg := range segs {
			bat, err := ptn.r.Segment(seg, proc).Read(ctr.spill.cs, ctr.spill.attrs, proc)
			if err != nil {
				return err
			}
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				bat.Clean(proc)
				return err
			}
			ptn.bats = append(ptn.bats, bat)
		}
	}
	ptn.mins = make([]int64, len(ptn.bats))
	ptn.lens = make([]int64, len(ptn.bats))
	for i, bat := range ptn.bats {
		if n := len(bat.Sels); n > 0 {
			ptn.lens[i] = int64(n)
		} else {
			n, err := bat.Length(proc)
			if err != nil {
				return err
			}
			ptn.lens[i] = int64(n)
		}
	}
	return nil
}

func (ctr *Container) newSpill(ptn *partition, proc *process.Process) error {
	id := pkey(ctr.spill.id, len(ctr.ptns))
	if err := ctr.spill.e.Create(id, ctr.spill.md); err != nil {
		return err
	}
	r, err := ctr.spill.e.Relation(id)
	if err != nil {
		ctr.spill.e.Delete(id)
		return err
	}
	ptn.r = r
	return nil
}

func (ptn *partition) emit(bat *batch.Batch, proc *process.Process) error {
	for row := int64(0); row < proc.Lim.BatchRows; row++ {
		if len(ptn.heap) == 0 {
			break
		}
		idx := ptn.heap[0]
		min := ptn.mins[idx]
		if len(ptn.bats[idx].Sels) > 0 {
			min = ptn.bats[idx].Sels[min]
		}
		for i, vec := range bat.Vecs {
			if vec.Data == nil {
				if err := vec.UnionOne(ptn.bats[idx].Vecs[i], min, proc); err != nil {
					return err
				}
				copy(vec.Data[:mempool.CountSize], ptn.bats[idx].Vecs[i].Data[:mempool.CountSize])
			} else {
				if err := vec.UnionOne(ptn.bats[idx].Vecs[i], min, proc); err != nil {
					return err
				}
			}
		}
		if ptn.mins[idx]+1 < ptn.lens[idx] {
			ptn.mins[idx]++
			heap.Fix(ptn, 0)
		} else {
			heap.Remove(ptn, 0)
		}
	}
	return nil
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	for i, ptn := range ctr.ptns {
		for _, bat := range ptn.bats {
			bat.Clean(proc)
		}
		if ptn.r != nil {
			ctr.spill.e.Delete(pkey(ctr.spill.id, i))
		}
	}
}

func pkey(id string, num int) string {
	return fmt.Sprintf("%s.%v", id, num)
}
