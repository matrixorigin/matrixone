// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sample

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math/rand"
)

// sPool is the pool for sample.
type sPool struct {
	proc *process.Process

	// need reordering or not.
	// if true, output method should reorder the result as the order of [groupList, sampleList, other columns].
	needReorder       bool
	extraColumnsIndex []int

	// sample type.
	// same as the Type attribute of sample.Argument.
	typ int

	// capacity for each group.
	capacity int

	// 1000 means 10.00%, 1234 means 12.34%
	percents int

	// merge sample related.
	nGroup, nSample int

	// pools for each group to do sample by only one column.
	sPools []singlePool
	// pools for each group to do sample by multi columns.
	mPools []multiPool

	// reused memory for sample vectors.
	columns sampleColumnList
}

func newSamplePoolByRows(proc *process.Process, capacity int, sampleColumnCount int) *sPool {
	return &sPool{
		proc:        proc,
		needReorder: true,
		typ:         sampleByRow,
		capacity:    capacity,
		columns:     make(sampleColumnList, sampleColumnCount),
	}
}

func newSamplePoolByPercent(proc *process.Process, per float64, sampleColumnCount int) *sPool {
	return &sPool{
		proc:        proc,
		needReorder: true,
		typ:         sampleByPercent,
		percents:    int(per * 100),
		columns:     make(sampleColumnList, sampleColumnCount),
	}
}

func (s *sPool) growSiPool(target int) {
	if target <= len(s.sPools) {
		return
	}
	for len(s.sPools) < target {
		sp := singlePool{
			capacity: s.capacity,
			seen:     0,
			space:    s.capacity,
			bat:      nil,
		}
		s.sPools = append(s.sPools, sp)
	}
}

func (s *sPool) growMulPool(target int, colNumber int) {
	if target <= len(s.mPools) {
		return
	}
	for len(s.mPools) < target {
		s1 := make([]int, colNumber)
		for i := range s1 {
			s1[i] = s.capacity
		}

		sp := multiPool{
			full:   false,
			seen:   0,
			have:   0,
			bat:    nil,
			space:  s1,
			tSpace: make([]int, colNumber),
		}
		s.mPools = append(s.mPools, sp)
	}
}

func (s *sPool) updateReOrderList(
	sampleVectors, groupVectors []*vector.Vector, inputBatch *batch.Batch) {
	if s.needReorder && s.extraColumnsIndex == nil {
		offset := len(inputBatch.Vecs)
		s.extraColumnsIndex = make([]int, len(sampleVectors)+len(groupVectors))

		index := 0
		for _, vec1 := range groupVectors {
			get := false
			for j, vec2 := range inputBatch.Vecs {
				if vec1 == vec2 {
					s.extraColumnsIndex[index] = j
					get = true
					break
				}
			}
			if !get {
				s.extraColumnsIndex[index] = offset
				offset++
			}
			index++
		}
		for _, vec1 := range sampleVectors {
			get := false
			for j, vec2 := range inputBatch.Vecs {
				if vec1 == vec2 {
					s.extraColumnsIndex[index] = j
					get = true
					break
				}
			}
			if !get {
				s.extraColumnsIndex[index] = offset
				offset++
			}
			index++
		}
	}
}

func (s *sPool) Sample(groupIndex int, sampleVectors []*vector.Vector, groupVectors []*vector.Vector, inputBatch *batch.Batch) error {
	s.updateReOrderList(sampleVectors, groupVectors, inputBatch)

	if len(sampleVectors) > 1 {
		return s.sampleFromColumns(groupIndex, sampleVectors, inputBatch)
	}
	return s.sampleFromColumn(groupIndex, sampleVectors[0], inputBatch)
}

func (s *sPool) sampleFromColumn(groupIndex int, sampleVec *vector.Vector, bat *batch.Batch) error {
	if groupIndex == 0 {
		return nil
	}
	s.growSiPool(groupIndex)
	groupIndex--

	s.updateReused1(sampleVec)

	switch s.typ {
	case sampleByRow:
		return s.sPools[groupIndex].addByRow(s.proc, s.columns[0], bat)
	case sampleByPercent:
		return s.sPools[groupIndex].addByPercent(s.proc, s.columns[0], bat, s.percents)
	}
	return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
}

func (s *sPool) sampleFromColumns(groupIndex int, sampleVectors []*vector.Vector, bat *batch.Batch) (err error) {
	if groupIndex == 0 {
		return
	}
	s.growMulPool(groupIndex, len(sampleVectors))
	groupIndex--

	s.updateReused2(sampleVectors)

	switch s.typ {
	case sampleByRow:
		return s.mPools[groupIndex].addByRow(s.proc, s.columns, bat)
	case sampleByPercent:
		return s.mPools[groupIndex].addByPercent(s.proc, s.columns, bat, s.percents)
	}
	return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
}

func (s *sPool) BatchSample(length int, groupList []uint64, sampleVectors []*vector.Vector, groupVectors []*vector.Vector, inputBatch *batch.Batch) (err error) {
	s.updateReOrderList(sampleVectors, groupVectors, inputBatch)

	if len(sampleVectors) > 1 {
		return s.batchSampleFromColumns(length, groupList, sampleVectors, inputBatch)
	}
	return s.batchSampleFromColumn(length, groupList, sampleVectors[0], inputBatch)
}

func (s *sPool) batchSampleFromColumn(length int, groupList []uint64, sampleVec *vector.Vector, bat *batch.Batch) (err error) {
	s.updateReused1(sampleVec)

	mp := s.proc.Mp()

	switch s.typ {
	case sampleByRow:
		for row, v := range groupList[:length] {
			if v == 0 {
				continue
			}

			groupIndex := int(v)
			s.growSiPool(groupIndex)
			groupIndex--

			err = s.sPools[groupIndex].addRow(s.proc, mp, s.columns[0], bat, row)
			if err != nil {
				return err
			}
		}

	case sampleByPercent:
		for row, v := range groupList[:length] {
			if v == 0 {
				continue
			}

			groupIndex := int(v)
			s.growSiPool(groupIndex)
			groupIndex--

			err = s.sPools[groupIndex].addRowByPercent(s.proc, mp, s.columns[0], bat, row, s.percents)
			if err != nil {
				return err
			}
		}
	default:
		return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
	}
	return nil
}

func (s *sPool) batchSampleFromColumns(length int, groupList []uint64, sampleVectors []*vector.Vector, bat *batch.Batch) (err error) {
	s.updateReused2(sampleVectors)

	mp := s.proc.Mp()

	switch s.typ {
	case sampleByRow:
		for row, v := range groupList[:length] {
			if v == 0 {
				continue
			}
			groupIndex := int(v)
			s.growMulPool(groupIndex, len(sampleVectors))
			groupIndex--

			err = s.mPools[groupIndex].addRow(s.proc, mp, s.columns, bat, row)
			if err != nil {
				return err
			}
		}

	case sampleByPercent:
		for row, v := range groupList[:length] {
			if v == 0 {
				continue
			}
			groupIndex := int(v)
			s.growMulPool(groupIndex, len(sampleVectors))
			groupIndex--

			err = s.mPools[groupIndex].addRowByPercent(s.proc, mp, s.columns, bat, row, s.percents)
			if err != nil {
				return err
			}
		}
	default:
		return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
	}
	return nil
}

func (s *sPool) updateReused1(col *vector.Vector) {
	if col.IsConst() {
		if col.IsConstNull() {
			s.columns[0] = sColumnConst{isnull: true}
		} else {
			s.columns[0] = sColumnConst{isnull: false}
		}
	} else {
		if col.GetNulls().IsEmpty() {
			s.columns[0] = sColumnNormalWithoutNull{}
		} else {
			s.columns[0] = sColumnNormalWithNull{nsp: col.GetNulls()}
		}
	}
}

func (s *sPool) updateReused2(columns []*vector.Vector) {
	for i, col := range columns {
		if col.IsConst() {
			if col.IsConstNull() {
				s.columns[i] = sColumnConst{isnull: true}
			} else {
				s.columns[i] = sColumnConst{isnull: false}
			}
		} else {
			if col.GetNulls().IsEmpty() {
				s.columns[i] = sColumnNormalWithoutNull{}
			} else {
				s.columns[i] = sColumnNormalWithNull{nsp: col.GetNulls()}
			}
		}
	}
}

func (s *sPool) Output(end bool) (bat *batch.Batch, err error) {
	if !end {
		if s.typ == sampleByRow {
			return batch.EmptyBatch, nil
		}
	}

	mp := s.proc.Mp()
	if len(s.sPools) > 0 {
		bat = s.sPools[0].bat
		s.sPools[0].bat = nil

		for i := 1; i < len(s.sPools); i++ {
			if s.sPools[i].bat == nil {
				continue
			}

			bat, err = bat.Append(s.proc.Ctx, mp, s.sPools[i].bat)
			if err != nil {
				s.proc.PutBatch(bat)
				return nil, err
			}

			s.proc.PutBatch(s.sPools[i].bat)
			s.sPools[i].bat = nil
		}
	} else if len(s.mPools) > 0 {
		bat = s.mPools[0].bat
		s.mPools[0].bat = nil

		for i := 1; i < len(s.mPools); i++ {
			if s.mPools[i].bat == nil {
				continue
			}

			bat, err = bat.Append(s.proc.Ctx, mp, s.mPools[i].bat)
			if err != nil {
				s.proc.PutBatch(bat)
				return nil, err
			}

			s.proc.PutBatch(s.mPools[i].bat)
			s.mPools[i].bat = nil
		}
	}

	// If a middle result is empty, cannot return nil directly. It will cause the pipeline closed early.
	if bat == nil {
		if !end {
			return batch.EmptyBatch, nil
		}
	} else {
		if s.needReorder {
			vv := make([]*vector.Vector, 0, len(bat.Vecs))
			for _, index := range s.extraColumnsIndex {
				vv = append(vv, bat.Vecs[index])
				bat.Vecs[index] = nil
			}
			for _, vec := range bat.Vecs {
				if vec != nil {
					vv = append(vv, vec)
				}
			}
			bat.Vecs = vv
		}
	}

	return bat, nil
}

// pool for sample by single column.
type singlePool struct {
	capacity int

	// count of rows which has been seen.
	seen int
	// free space of pool.
	space int

	// items in pool.
	bat *batch.Batch
}

func (sp *singlePool) addByRow(proc *process.Process, column sampleColumn, bat *batch.Batch) error {
	k := bat.RowCount()
	mp := proc.Mp()

	randReplaceStart := 0
	if sp.space > 0 {
		if column.anyNull() {
			offset, length := 0, 0
			oldSpace := sp.space

			// case: pool must have enough space to store all the values.
			if k <= sp.space {
				for i := 0; i < k; i++ {
					if column.isNull(i) {
						if length > 0 {
							err := sp.appendResult(proc, mp, bat, offset, length)
							if err != nil {
								return err
							}
							sp.space -= length
							length = 0
						}
						offset = i + 1
						continue
					}
					length++
				}
				if offset < k && length > 0 {
					err := sp.appendResult(proc, mp, bat, offset, length)
					if err != nil {
						return err
					}
					sp.space -= length
				}
				sp.seen += oldSpace - sp.space
				return nil
			}

			// case: pool's space maybe unable to store all the values.
			var i = 0
			for ; i < k && sp.space >= length; i++ {
				if column.isNull(i) {
					if length > 0 {
						err := sp.appendResult(proc, mp, bat, offset, length)
						if err != nil {
							return err
						}
						sp.space -= length
						length = 0
					}
					offset = i + 1
					continue
				}
				length++
			}
			if length > 0 {
				if sp.space >= length {
					err := sp.appendResult(proc, mp, bat, offset, length)
					if err != nil {
						return err
					}
					sp.space -= length
					sp.seen += oldSpace
					return nil
				}
				err := sp.appendResult(proc, mp, bat, offset, sp.space)
				if err != nil {
					return err
				}
				offset += sp.space
				sp.space = 0
			}
			sp.seen += oldSpace - sp.space
			randReplaceStart = offset

		} else {
			// case: pool can store.
			if k <= sp.space {
				sp.space -= k
				sp.seen += k
				err := sp.appendResult(proc, mp, bat, 0, k)
				if err != nil {
					return err
				}
				return nil
			}
			// case: pool can store part of rows.
			randReplaceStart = sp.space
			err := sp.appendResult(proc, mp, bat, 0, sp.space)
			if err != nil {
				return err
			}
			sp.seen += sp.space
			sp.space = 0
		}
	}

	// after some append action, batch still has rows need to add.
	var r int
	if column.anyNull() {
		for i := randReplaceStart; i < k; i++ {
			if column.isNull(i) {
				continue
			}
			sp.seen++

			r = rand.Intn(sp.seen)
			if r < sp.capacity {
				err := batRowReplace(mp, sp.bat, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	} else {
		for i := randReplaceStart; i < k; i++ {
			sp.seen++

			r = rand.Intn(sp.seen)
			if r < sp.capacity {
				err := batRowReplace(mp, sp.bat, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sp *singlePool) addByPercent(proc *process.Process, column sampleColumn, bat *batch.Batch, percent int) error {
	if percent == 0 {
		return nil
	}

	k := bat.RowCount()
	mp := proc.Mp()

	if column.anyNull() {
		for i := 0; i < k; i++ {
			if column.isNull(i) {
				continue
			}

			if percent == 10000 || rand.Intn(10000) < percent {
				if err := sp.appendResult(proc, mp, bat, i, 1); err != nil {
					return err
				}
			}
		}
	} else {
		for i := 0; i < k; i++ {
			if percent == 10000 || rand.Intn(10000) < percent {
				if err := sp.appendResult(proc, mp, bat, i, 1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sp *singlePool) addRow(proc *process.Process, mp *mpool.MPool, column sampleColumn, bat *batch.Batch, row int) (err error) {
	if column.isNull(row) {
		return
	}

	sp.seen++
	if sp.space > 0 {
		sp.space--
		err = sp.appendResult(proc, mp, bat, row, 1)
	} else {
		r := rand.Intn(sp.seen)
		if r < sp.capacity {
			err = batRowReplace(mp, sp.bat, bat, r, row)
		}
	}
	return err
}

func (sp *singlePool) addRowByPercent(proc *process.Process, mp *mpool.MPool, column sampleColumn, bat *batch.Batch, row int, percent int) (err error) {
	if percent == 0 {
		return
	}

	if column.isNull(row) {
		return
	}

	if percent == 10000 || rand.Intn(10000) < percent {
		err = sp.appendResult(proc, mp, bat, row, 1)
	}
	return
}

func (sp *singlePool) appendResult(proc *process.Process, mp *mpool.MPool, bat *batch.Batch, offset int, length int) (err error) {
	if sp.bat == nil {
		sp.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range sp.bat.Vecs {
			sp.bat.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
		}
	}

	for i := range sp.bat.Vecs {
		if err = sp.bat.Vecs[i].UnionBatch(bat.Vecs[i], int64(offset), length, nil, mp); err != nil {
			return err
		}
	}
	sp.bat.AddRowCount(length)
	return nil
}

// pool for sample by multi columns.
type multiPool struct {
	full bool

	// free space for each column
	space []int
	// count of rows which has been seen.
	seen int
	// count of rows which has been stored.
	have int

	// items in pool.
	bat *batch.Batch

	// reused.
	tSpace []int
}

func (p *multiPool) isFull() bool {
	for _, sp := range p.space {
		if sp > 0 {
			return false
		}
	}
	return true
}

func (p *multiPool) appendOneRow(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int) (err error) {
	if p.bat == nil {
		p.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range p.bat.Vecs {
			p.bat.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
		}
	}

	for i := range p.bat.Vecs {
		if err = p.bat.Vecs[i].UnionBatch(bat.Vecs[i], int64(row), 1, nil, mp); err != nil {
			return err
		}
	}
	p.bat.AddRowCount(1)

	for i, col := range columns {
		if !col.isNull(row) {
			p.space[i]--
		}
	}
	p.have++
	return nil
}

func (p *multiPool) addByRow(proc *process.Process, columns sampleColumnList, bat *batch.Batch) (err error) {
	k := bat.RowCount()
	var i = 0
	if !p.full {
		for ; i < k; i++ {
			if p.isFull() {
				break
			}
			p.seen++
			for colIndex := range p.space {
				if p.space[colIndex] > 0 && !columns[colIndex].isNull(i) {
					err = p.appendOneRow(proc, proc.Mp(), columns, bat, i)
					if err != nil {
						return err
					}
					break
				}
			}
		}
	}

	var r int
	var canReplace bool
	mp := proc.Mp()
	for ; i < k; i++ {
		if columns.isAllNull(i) {
			continue
		}
		p.seen++

		r = rand.Intn(p.seen)
		if r < p.have {
			// check if replace action will cause sample condition failed.
			canReplace = true
			for j := range p.space {
				p.tSpace[j] = p.space[j]
				if columns[j].isNull(i) {
					if p.space[j] < -1 {
						p.tSpace[j]++
					} else {
						canReplace = false
						break
					}
				}
			}

			if canReplace {
				p.tSpace, p.space = p.space, p.tSpace
				err = batRowReplace(mp, p.bat, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	}
	p.full = p.isFull()
	return nil
}

func (p *multiPool) addByPercent(proc *process.Process, columns sampleColumnList, bat *batch.Batch, percent int) (err error) {
	if percent == 0 {
		return nil
	}

	k := bat.RowCount()

	for i := 0; i < k; i++ {
		if columns.isAllNull(i) {
			continue
		}
		if percent == 10000 || rand.Intn(10000) < percent {
			err = p.appendOneRow(proc, proc.Mp(), columns, bat, i)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *multiPool) addRow(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int) (err error) {
	if columns.isAllNull(row) {
		return
	}

	if !p.full {
		p.seen++
		for colIndex := range p.space {
			if p.space[colIndex] > 0 && !columns[colIndex].isNull(row) {
				err = p.appendOneRow(proc, proc.Mp(), columns, bat, row)
				if err != nil {
					return err
				}
				break
			}
		}
		p.full = p.isFull()
	} else {
		p.seen++
		r := rand.Intn(p.seen)
		if r < p.have {
			// check if replace action will cause sample condition failed.
			canReplace := true
			for j := range p.space {
				p.tSpace[j] = p.space[j]
				if columns[j].isNull(row) {
					if p.space[j] < -1 {
						p.tSpace[j]++
					} else {
						canReplace = false
						break
					}
				}
			}

			if canReplace {
				p.tSpace, p.space = p.space, p.tSpace
				err = batRowReplace(mp, p.bat, bat, r, row)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *multiPool) addRowByPercent(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int, percent int) (err error) {
	if percent == 0 {
		return
	}

	if columns.isAllNull(row) {
		return
	}
	if percent == 10000 || rand.Intn(10000) < percent {
		err = p.appendOneRow(proc, proc.Mp(), columns, bat, row)
	}
	return nil
}

type sampleColumn interface {
	isNull(index int) bool
	anyNull() bool
}
type sColumnConst struct{ isnull bool }
type sColumnNormalWithNull struct{ nsp *nulls.Nulls }
type sColumnNormalWithoutNull struct{}

func (s sColumnConst) isNull(_ int) bool              { return s.isnull }
func (s sColumnNormalWithNull) isNull(index int) bool { return s.nsp.Contains(uint64(index)) }
func (s sColumnNormalWithoutNull) isNull(_ int) bool  { return false }

func (s sColumnConst) anyNull() bool             { return s.isnull }
func (s sColumnNormalWithNull) anyNull() bool    { return true }
func (s sColumnNormalWithoutNull) anyNull() bool { return false }

type sampleColumnList []sampleColumn

func (l sampleColumnList) isAllNull(index int) bool {
	for _, col := range l {
		if !col.isNull(index) {
			return false
		}
	}
	return true
}

// batRowReplace replaces the row1 of toBatch with the bat's row2.
// TODO: need an optimized function to do the row replace work.
func batRowReplace(mp *mpool.MPool, toBatch *batch.Batch, bat *batch.Batch, row1, row2 int) (err error) {
	var right int
	for i, vec := range bat.Vecs {
		right = row2
		if vec.IsConst() {
			right = 0
		}
		switch vec.GetType().Oid {
		case types.T_int8:
			err = vector.SetFixedAt[int8](toBatch.Vecs[i], row1, vector.GetFixedAt[int8](vec, right))
		case types.T_int16:
			err = vector.SetFixedAt[int16](toBatch.Vecs[i], row1, vector.GetFixedAt[int16](vec, right))
		case types.T_int32:
			err = vector.SetFixedAt[int32](toBatch.Vecs[i], row1, vector.GetFixedAt[int32](vec, right))
		case types.T_int64:
			err = vector.SetFixedAt[int64](toBatch.Vecs[i], row1, vector.GetFixedAt[int64](vec, right))
		case types.T_uint8:
			err = vector.SetFixedAt[uint8](toBatch.Vecs[i], row1, vector.GetFixedAt[uint8](vec, right))
		case types.T_uint16:
			err = vector.SetFixedAt[uint16](toBatch.Vecs[i], row1, vector.GetFixedAt[uint16](vec, right))
		case types.T_uint32:
			err = vector.SetFixedAt[uint32](toBatch.Vecs[i], row1, vector.GetFixedAt[uint32](vec, right))
		case types.T_uint64:
			err = vector.SetFixedAt[uint64](toBatch.Vecs[i], row1, vector.GetFixedAt[uint64](vec, right))
		case types.T_float32:
			err = vector.SetFixedAt[float32](toBatch.Vecs[i], row1, vector.GetFixedAt[float32](vec, right))
		case types.T_float64:
			err = vector.SetFixedAt[float64](toBatch.Vecs[i], row1, vector.GetFixedAt[float64](vec, right))
		case types.T_date:
			err = vector.SetFixedAt[types.Date](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Date](vec, right))
		case types.T_datetime:
			err = vector.SetFixedAt[types.Datetime](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Datetime](vec, right))
		case types.T_timestamp:
			err = vector.SetFixedAt[types.Timestamp](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Timestamp](vec, right))
		case types.T_time:
			err = vector.SetFixedAt[types.Time](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Time](vec, right))
		case types.T_enum:
			err = vector.SetFixedAt[types.Enum](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Enum](vec, right))
		case types.T_decimal64:
			err = vector.SetFixedAt[types.Decimal64](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Decimal64](vec, right))
		case types.T_decimal128:
			err = vector.SetFixedAt[types.Decimal128](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Decimal128](vec, right))
		case types.T_TS:
			err = vector.SetFixedAt[types.TS](toBatch.Vecs[i], row1, vector.GetFixedAt[types.TS](vec, right))
		case types.T_Rowid:
			err = vector.SetFixedAt[types.Rowid](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Rowid](vec, right))
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
			types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64:
			err = vector.SetBytesAt(toBatch.Vecs[i], row1, vec.GetBytesAt(right), mp)
		default:
			err = moerr.NewInternalErrorNoCtx("unsupported type for sample pool.")
		}
		if err != nil {
			return err
		}
	}
	return nil
}
