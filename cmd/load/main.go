package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/logEngine"
	"matrixone/pkg/vm/metadata"
	"os"
	"strconv"
	"time"
)

const (
	BatchSize = 1000000
	ShortForm = "2006-01-02"
)

func main() {
	db, err := logEngine.New("test.db")
	if err != nil {
		log.Fatal(err)
	}
	r, err := db.Relation("lineorder_flat")
	if err != nil {
		log.Fatal(err)
	}
	ms := r.Attribute()
	fmt.Printf("insert into lineorder_flat(%v)\n", ms)
	bat := newBatch(ms)
	for i := 1; i < len(os.Args); i++ {
		writeUnit(os.Args[i], bat, r, ms)
		fmt.Printf("process '%v' ok\n", os.Args[i])
	}
}

func writeUnit(name string, bat *batch.Batch, r engine.Relation, ms []metadata.Attribute) {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	ts, err := csv.NewReader(f).ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	for len(ts) > 0 {
		n := len(ts)
		if n > BatchSize {
			n = BatchSize
		}
		for i, m := range ms {
			if bat.Vecs[i], err = convert(i, n, ts, m.Type.Oid, bat.Vecs[i]); err != nil {
				log.Fatal(err)
			}
		}
		ts = ts[n:]
		if err := r.Write(bat); err != nil {
			log.Fatal(err)
		}
	}

}

// index = 5, 15
func convert(idx, length int, ts [][]string, typ types.T, vec *vector.Vector) (*vector.Vector, error) {
	switch typ {
	case types.T_int8:
		vs := vec.Col.([]int8)
		if cap(vs) < length {
			vs = make([]int8, length)
		}
		vs = vs[:length]
		for i := 0; i < length; i++ {
			v, err := strconv.ParseInt(ts[i][idx], 0, 8)
			if err != nil {
				return nil, err
			}
			vs[i] = int8(v)
		}
		vec.SetCol(vs)
	case types.T_int32:
		if idx == 5 || idx == 15 {
			vs := vec.Col.([]int32)
			if cap(vs) < length {
				vs = make([]int32, length)
			}
			vs = vs[:length]
			for i := 0; i < length; i++ {
				v, err := time.Parse(ShortForm, ts[i][idx])
				if err != nil {
					return nil, err
				}
				vs[i] = int32(v.Unix())
			}
			vec.SetCol(vs)
		} else {
			vs := vec.Col.([]int32)
			if cap(vs) < length {
				vs = make([]int32, length)
			}
			vs = vs[:length]
			for i := 0; i < length; i++ {
				v, err := strconv.ParseInt(ts[i][idx], 0, 32)
				if err != nil {
					return nil, err
				}
				vs[i] = int32(v)
			}
			vec.SetCol(vs)
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		if cap(vs) < length {
			vs = make([]int64, length)
		}
		vs = vs[:length]
		for i := 0; i < length; i++ {
			v, err := strconv.ParseInt(ts[i][idx], 0, 64)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		}
		vec.SetCol(vs)
	case types.T_float64:
		vs := vec.Col.([]float64)
		if cap(vs) < length {
			vs = make([]float64, length)
		}
		vs = vs[:length]
		for i := 0; i < length; i++ {
			v, err := strconv.ParseFloat(ts[i][idx], 64)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		}
		vec.SetCol(vs)
	case types.T_varchar:
		vs := vec.Col.(*types.Bytes)
		if cap(vs.Offsets) < length {
			vs.Offsets = make([]uint32, length)
			vs.Lengths = make([]uint32, length)
		}
		o := uint32(0)
		vs.Offsets = vs.Offsets[:length]
		vs.Lengths = vs.Lengths[:length]
		vs.Data = vs.Data[:0]
		buf := bytes.NewBuffer(vs.Data)
		for i := 0; i < length; i++ {
			vs.Offsets[i] = o
			s := ts[i][idx]
			vs.Lengths[i] = uint32(len(s))
			o += vs.Lengths[i]
			buf.WriteString(s)
		}
		vs.Data = buf.Bytes()
		vec.Col = vs
	default:
		return nil, fmt.Errorf("type '%s' not yet implemented", typ)
	}
	return vec, nil
}

func newBatch(ms []metadata.Attribute) *batch.Batch {
	attrs := make([]string, len(ms))
	for i, m := range ms {
		attrs[i] = m.Name
	}
	bat := batch.New(true, attrs)
	for i, m := range ms {
		bat.Vecs[i] = vector.New(m.Type)
	}
	return bat
}
