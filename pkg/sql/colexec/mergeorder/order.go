package mergeorder

import (
	"bytes"
	"errors"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/partition"
	"matrixone/pkg/sort"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
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
	ctr.ds = make([]bool, len(n.Fs))
	ctr.attrs = make([]string, len(n.Fs))
	for i, f := range n.Fs {
		ctr.attrs[i] = f.Attr
		ctr.ds[i] = f.Type == Descending
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if err := ctr.eval(proc); err != nil {
				ctr.state = End
				return true, err
			}
			if !n.Flg {
				ctr.bat.Reduce(ctr.attrs, proc)
			}
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			ctr.clean(proc)
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.Ax = nil
			return true, nil
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
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(ctr.attrs)
			} else {
				bat.Reorder(ctr.bat.Attrs)
			}
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			if ctr.bat == nil {
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
			}
			if len(bat.Sels) == 0 {
				for sel, nsel := int64(0), int64(bat.Vecs[0].Length()); sel < nsel; sel++ {
					{
						for i, vec := range ctr.bat.Vecs {
							if vec.Data == nil {
								if err := vec.UnionOne(bat.Vecs[i], sel, proc); err != nil {
									reg.Ch = nil
									reg.Wg.Done()
									bat.Clean(proc)
									return err
								}
								copy(vec.Data[:mempool.CountSize], bat.Vecs[i].Data[:mempool.CountSize])
							} else {
								if err := vec.UnionOne(bat.Vecs[i], sel, proc); err != nil {
									reg.Ch = nil
									reg.Wg.Done()
									bat.Clean(proc)
									return err
								}
							}
						}
					}
				}
			} else {
				for _, sel := range bat.Sels {
					{
						for i, vec := range ctr.bat.Vecs {
							if vec.Data == nil {
								if err := vec.UnionOne(bat.Vecs[i], sel, proc); err != nil {
									reg.Ch = nil
									reg.Wg.Done()
									bat.Clean(proc)
									return err
								}
								copy(vec.Data[:mempool.CountSize], bat.Vecs[i].Data[:mempool.CountSize])
							} else {
								if err := vec.UnionOne(bat.Vecs[i], sel, proc); err != nil {
									reg.Ch = nil
									reg.Wg.Done()
									bat.Clean(proc)
									return err
								}
							}
							ctr.bat.Vecs[i] = vec
						}
					}
				}
			}
			if proc.Size() > proc.Lim.Size {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return errors.New("out of memory")
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) eval(proc *process.Process) error {
	ovec := ctr.bat.Vecs[0]
	n := ovec.Length()
	data, err := proc.Alloc(int64(n * 8))
	if err != nil {
		return err
	}
	sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
	sels = sels[:n]
	{
		for i := range sels {
			sels[i] = int64(i)
		}
	}
	sort.Sort(ctr.ds[0], sels, ovec)
	if len(ctr.attrs) == 1 {
		ctr.bat.Sels = sels
		ctr.bat.SelsData = data
		return nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.attrs); i < j; i++ {
		desc := ctr.ds[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := ctr.bat.Vecs[i]
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(desc, sels[ps[i]:], vec)
			} else {
				sort.Sort(desc, sels[ps[i]:ps[i+1]], vec)
			}
		}
		ovec = vec
	}
	ctr.bat.Sels = sels
	ctr.bat.SelsData = data
	return nil

}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	{
		for _, reg := range proc.Reg.Ws {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
