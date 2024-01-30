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

	// requireReorder indicates whether the pool should do reorder for input batch's vectors before sample.
	// if true, vectors will be reordered as [groupList, sampleList, other columns].
	requireReorder bool
	otherPartIndex []int

	// sample type.
	// same as the Type attribute of sample.Argument.
	typ     sPoolType
	isMerge bool

	// if true, the returned batch will contain a column which indicates the number of rows that have been seen.
	outputRowCount bool

	// a helper to adjust the row picked probability for merge sample.
	baseRow           int64
	probabilityFactor float64

	// some fields for performance.
	canCheckFull bool

	// capacity for each group.
	capacity int

	// 1000 means 10.00%, 1234 means 12.34%
	percents int

	// pools for each group to do sample by only one column.
	sPools []singlePool
	// pools for each group to do sample by multi columns.
	mPools []multiPool

	// reused memory for sample vectors.
	columns sampleColumnList
	// reused memory to do real sample. there is no need to free it because all its memory is from outer batch and vectors.
	reOrderedInput *batch.Batch
}

type sPoolType int

const (
	rowSamplePool sPoolType = iota
	percentSamplePool
)

func newSamplePoolByRows(proc *process.Process, capacity int, sampleColumnCount int, outputRowSeen bool) *sPool {
	return &sPool{
		proc:           proc,
		requireReorder: true,
		typ:            rowSamplePool,
		isMerge:        false,
		outputRowCount: outputRowSeen,
		baseRow:        0,
		canCheckFull:   false,
		capacity:       capacity,
		columns:        make(sampleColumnList, sampleColumnCount),
	}
}

func newSamplePoolByPercent(proc *process.Process, per float64, sampleColumnCount int) *sPool {
	return &sPool{
		proc:           proc,
		requireReorder: true,
		typ:            percentSamplePool,
		isMerge:        false,
		outputRowCount: false,
		canCheckFull:   false,
		percents:       int(per * 100),
		columns:        make(sampleColumnList, sampleColumnCount),
	}
}

func newSamplePoolByRowsForMerge(proc *process.Process, capacity int, sampleColumnCount int, outputRowSeen bool) *sPool {
	return &sPool{
		proc:           proc,
		requireReorder: false,
		typ:            rowSamplePool,
		isMerge:        true,
		outputRowCount: outputRowSeen,
		canCheckFull:   false,
		capacity:       capacity,
		columns:        make(sampleColumnList, sampleColumnCount),
	}
}

func (s *sPool) setPerfFields(isGroupBy bool) {
	s.canCheckFull = !isGroupBy && !s.isMerge && s.typ == rowSamplePool
}

// the base row will adjust the picked probability for each batch if this was a merge sample.
// for example, the row from 1,000 rows data source will be picked more times than the row from 100 rows data source.
func (s *sPool) updateProbabilityAdjustmentFactor(inputBatch *batch.Batch) {
	if s.isMerge {
		row := vector.GetFixedAt[int64](inputBatch.Vecs[len(inputBatch.Vecs)-1], 0)
		if s.baseRow == 0 {
			s.baseRow = row
		}
		s.probabilityFactor = float64(row) / float64(s.baseRow)
	}
	s.probabilityFactor = 1
}

// randWithFactor returns a random number between [0, seen) with a factor.
// if factor > 1, the result will be more close to 0.
// if factor < 1, the result will be more close to the number seen.
func randWithFactor(fCap float64, seen int, factor float64) int {
	r := rand.Intn(seen)
	fr := float64(r)
	fSeen := float64(seen)
	if factor > 1 {
		if fSeen-fr <= (fSeen-fCap)/factor {
			return int(fr / fSeen * fCap)
		}
		return r
	}

	if fr < fCap*factor {
		return int(fr / factor)
	}
	return seen
}

func (s *sPool) IsFull() bool {
	if !s.canCheckFull {
		return false
	}
	if len(s.sPools) > 0 {
		return s.sPools[0].space == 0
	}
	if len(s.mPools) > 0 {
		return s.mPools[0].isFull()
	}
	return false
}

func (s *sPool) Free() {
	mp := s.proc.Mp()
	for _, sp := range s.sPools {
		sp.data.clean(mp)
	}
	for _, m := range s.mPools {
		m.data.clean(mp)
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
			data:     poolData{validBatch: nil, invalidBatch: nil},
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
			space:  s1,
			data:   poolData{validBatch: nil, invalidBatch: nil},
			tSpace: make([]int, colNumber),
		}
		s.mPools = append(s.mPools, sp)
	}
}

func (s *sPool) vectorReOrder(
	sampleVectors, groupVectors []*vector.Vector, inputBatch *batch.Batch) *batch.Batch {
	if !s.requireReorder {
		return inputBatch
	}

	// get a reorder list at first time.
	if s.otherPartIndex == nil {
		offset := len(inputBatch.Vecs)
		got := make([]bool, len(inputBatch.Vecs))

		for _, vec1 := range groupVectors {
			for j, vec2 := range inputBatch.Vecs {
				if vec1 == vec2 {
					got[j] = true
					break
				}
			}
		}
		for _, vec1 := range sampleVectors {
			for j, vec2 := range inputBatch.Vecs {
				if vec1 == vec2 {
					got[j] = true
					break
				}
			}
		}

		s.reOrderedInput = batch.NewWithSize(offset)
		s.otherPartIndex = make([]int, 0, len(inputBatch.Vecs))
		for i, g := range got {
			if !g {
				s.otherPartIndex = append(s.otherPartIndex, i)
			}
		}
	}

	// reorder vectors.
	s.reOrderedInput.Vecs = s.reOrderedInput.Vecs[:0]
	s.reOrderedInput.Vecs = append(s.reOrderedInput.Vecs, groupVectors...)
	s.reOrderedInput.Vecs = append(s.reOrderedInput.Vecs, sampleVectors...)
	for _, index := range s.otherPartIndex {
		s.reOrderedInput.Vecs = append(s.reOrderedInput.Vecs, inputBatch.Vecs[index])
	}
	s.reOrderedInput.SetRowCount(inputBatch.RowCount())

	return s.reOrderedInput
}

func (s *sPool) Sample(groupIndex int, sampleVectors []*vector.Vector, groupVectors []*vector.Vector, inputBatch *batch.Batch) error {
	s.reOrderedInput = s.vectorReOrder(sampleVectors, groupVectors, inputBatch)

	if len(sampleVectors) > 1 {
		return s.sampleFromColumns(groupIndex, sampleVectors, s.reOrderedInput)
	}
	return s.sampleFromColumn(groupIndex, sampleVectors[0], s.reOrderedInput)
}

func (s *sPool) sampleFromColumn(groupIndex int, sampleVec *vector.Vector, bat *batch.Batch) error {
	if groupIndex == 0 {
		return nil
	}
	s.growSiPool(groupIndex)
	groupIndex--

	s.updateReused1(sampleVec)

	switch s.typ {
	case rowSamplePool:
		s.updateProbabilityAdjustmentFactor(bat)
		return s.sPools[groupIndex].addRows(s.proc, s.columns[0], bat, s.probabilityFactor)
	case percentSamplePool:
		return s.sPools[groupIndex].addRowsByPercent(s.proc, s.columns[0], bat, s.percents)
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
	case rowSamplePool:
		s.updateProbabilityAdjustmentFactor(bat)
		return s.mPools[groupIndex].addRows(s.proc, s.columns, bat, s.probabilityFactor)
	case percentSamplePool:
		return s.mPools[groupIndex].addRowsByPercent(s.proc, s.columns, bat, s.percents)
	}
	return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
}

func (s *sPool) BatchSample(offset, length int, groupList []uint64, sampleVectors []*vector.Vector, groupVectors []*vector.Vector, inputBatch *batch.Batch) (err error) {
	s.reOrderedInput = s.vectorReOrder(sampleVectors, groupVectors, inputBatch)

	if len(sampleVectors) > 1 {
		return s.batchSampleFromColumns(offset, length, groupList, sampleVectors, s.reOrderedInput)
	}
	return s.batchSampleFromColumn(offset, length, groupList, sampleVectors[0], s.reOrderedInput)
}

func (s *sPool) batchSampleFromColumn(offset, length int, groupList []uint64, sampleVec *vector.Vector, bat *batch.Batch) (err error) {
	s.updateReused1(sampleVec)

	mp := s.proc.Mp()
	row := offset

	switch s.typ {
	case rowSamplePool:
		s.updateProbabilityAdjustmentFactor(bat)
		for _, v := range groupList[:length] {
			if v == 0 {
				continue
			}

			groupIndex := int(v)
			s.growSiPool(groupIndex)
			groupIndex--

			err = s.sPools[groupIndex].addOneRow(s.proc, mp, s.columns[0], bat, row, s.probabilityFactor)
			if err != nil {
				return err
			}
			row++
		}

	case percentSamplePool:
		for _, v := range groupList[:length] {
			if v == 0 {
				continue
			}

			groupIndex := int(v)
			s.growSiPool(groupIndex)
			groupIndex--

			err = s.sPools[groupIndex].addOneRowByPercent(s.proc, mp, s.columns[0], bat, row, s.percents)
			if err != nil {
				return err
			}
			row++
		}
	default:
		return moerr.NewInternalErrorNoCtx("unexpected sample type %d", s.typ)
	}
	return nil
}

func (s *sPool) batchSampleFromColumns(offset, length int, groupList []uint64, sampleVectors []*vector.Vector, bat *batch.Batch) (err error) {
	s.updateReused2(sampleVectors)

	mp := s.proc.Mp()
	row := offset

	switch s.typ {
	case rowSamplePool:
		s.updateProbabilityAdjustmentFactor(bat)
		for _, v := range groupList[:length] {
			if v == 0 {
				continue
			}
			groupIndex := int(v)
			s.growMulPool(groupIndex, len(sampleVectors))
			groupIndex--

			err = s.mPools[groupIndex].addOneRow(s.proc, mp, s.columns, bat, row, s.probabilityFactor)
			if err != nil {
				return err
			}
			row++
		}

	case percentSamplePool:
		for _, v := range groupList[:length] {
			if v == 0 {
				continue
			}
			groupIndex := int(v)
			s.growMulPool(groupIndex, len(sampleVectors))
			groupIndex--

			err = s.mPools[groupIndex].addOneRowByPercent(s.proc, mp, s.columns, bat, row, s.percents)
			if err != nil {
				return err
			}
			row++
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

func (s *sPool) Result(end bool) (bat *batch.Batch, err error) {
	if !end {
		if s.typ == rowSamplePool {
			return batch.EmptyBatch, nil
		}
	}

	mp := s.proc.Mp()
	seenRows := 0
	if len(s.sPools) > 0 {
		bat = s.sPools[0].data.flush()
		seenRows += s.sPools[0].seen
		if bat != nil {
			for i := 1; i < len(s.sPools); i++ {
				b := s.sPools[i].data.flush()
				if b == nil {
					continue
				}

				seenRows += s.sPools[i].seen
				bat, err = bat.Append(s.proc.Ctx, mp, b)
				s.proc.PutBatch(b)
				if err != nil {
					s.proc.PutBatch(bat)
					return nil, err
				}
			}
		}
	} else if len(s.mPools) > 0 {
		bat = s.mPools[0].data.flush()
		seenRows += s.mPools[0].seen

		for i := 1; i < len(s.mPools); i++ {
			b := s.mPools[i].data.flush()
			if b == nil {
				continue
			}

			seenRows += s.mPools[i].seen
			bat, err = bat.Append(s.proc.Ctx, mp, b)
			s.proc.PutBatch(b)
			if err != nil {
				s.proc.PutBatch(bat)
				return nil, err
			}
		}
	}

	// If a middle result is empty, cannot return nil directly. It will cause the pipeline closed early.
	if bat == nil && !end {
		return batch.EmptyBatch, nil
	}

	if s.outputRowCount {
		bat.Vecs = append(bat.Vecs, nil)
		bat.Vecs[len(bat.Vecs)-1], err = vector.NewConstFixed[int64](types.T_int64.ToType(), int64(seenRows), bat.RowCount(), mp)
		if err != nil {
			s.proc.PutBatch(bat)
			return nil, err
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

	data poolData
}

func (sp *singlePool) addRows(proc *process.Process, column sampleColumn, bat *batch.Batch, factor float64) error {
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
						err := sp.data.appendInvalidRow(proc, mp, bat, i)
						if err != nil {
							return err
						}
						if length > 0 {
							err = sp.data.appendValidRow(proc, mp, bat, offset, length)
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
					err := sp.data.appendValidRow(proc, mp, bat, offset, length)
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
					err := sp.data.appendInvalidRow(proc, mp, bat, i)
					if err != nil {
						return err
					}
					if length > 0 {
						err = sp.data.appendValidRow(proc, mp, bat, offset, length)
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
					err := sp.data.appendValidRow(proc, mp, bat, offset, length)
					if err != nil {
						return err
					}
					sp.space -= length
					sp.seen += oldSpace
					return nil
				}
				err := sp.data.appendValidRow(proc, mp, bat, offset, sp.space)
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
				err := sp.data.appendValidRow(proc, mp, bat, 0, k)
				if err != nil {
					return err
				}
				return nil
			}
			// case: pool can store part of rows.
			randReplaceStart = sp.space
			err := sp.data.appendValidRow(proc, mp, bat, 0, sp.space)
			if err != nil {
				return err
			}
			sp.seen += sp.space
			sp.space = 0
		}
	}

	// after some append action, batch still has rows need to add.
	var r int
	if factor == 1 {
		if column.anyNull() {
			for i := randReplaceStart; i < k; i++ {
				if column.isNull(i) {
					err := sp.data.appendInvalidRow(proc, mp, bat, i)
					if err != nil {
						return err
					}
					continue
				}
				sp.seen++

				r = rand.Intn(sp.seen)
				if r < sp.capacity {
					err := sp.data.replaceValidRow(mp, bat, r, i)
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
					err := sp.data.replaceValidRow(mp, bat, r, i)
					if err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	floatCapacity := float64(sp.capacity)
	if column.anyNull() {
		for i := randReplaceStart; i < k; i++ {
			if column.isNull(i) {
				err := sp.data.appendInvalidRow(proc, mp, bat, i)
				if err != nil {
					return err
				}
				continue
			}
			sp.seen++

			r = randWithFactor(floatCapacity, sp.seen, factor)
			if r < sp.capacity {
				err := sp.data.replaceValidRow(mp, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	} else {
		for i := randReplaceStart; i < k; i++ {
			sp.seen++

			r = randWithFactor(floatCapacity, sp.seen, factor)
			if r < sp.capacity {
				err := sp.data.replaceValidRow(mp, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sp *singlePool) addRowsByPercent(proc *process.Process, column sampleColumn, bat *batch.Batch, percent int) error {
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
				if err := sp.data.appendValidRow(proc, mp, bat, i, 1); err != nil {
					return err
				}
			}
		}
	} else {
		for i := 0; i < k; i++ {
			if percent == 10000 || rand.Intn(10000) < percent {
				if err := sp.data.appendValidRow(proc, mp, bat, i, 1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (sp *singlePool) addOneRow(proc *process.Process, mp *mpool.MPool, column sampleColumn, bat *batch.Batch, row int, factor float64) (err error) {
	if column.isNull(row) {
		return sp.data.appendInvalidRow(proc, mp, bat, row)
	}

	sp.seen++
	if sp.space > 0 {
		sp.space--
		err = sp.data.appendValidRow(proc, mp, bat, row, 1)
	} else {
		var r int
		if factor == 1 {
			r = rand.Intn(sp.seen)
		} else {
			r = randWithFactor(float64(sp.capacity), sp.seen, factor)
		}
		if r < sp.capacity {
			err = sp.data.replaceValidRow(mp, bat, r, row)
		}
	}
	return err
}

func (sp *singlePool) addOneRowByPercent(proc *process.Process, mp *mpool.MPool, column sampleColumn, bat *batch.Batch, row int, percent int) (err error) {
	if percent == 0 {
		return
	}

	if column.isNull(row) {
		return
	}

	if percent == 10000 || rand.Intn(10000) < percent {
		err = sp.data.appendValidRow(proc, mp, bat, row, 1)
	}
	return
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

	// reused.
	tSpace []int

	data poolData
}

func (p *multiPool) isFull() bool {
	for _, sp := range p.space {
		if sp > 0 {
			return false
		}
	}
	return true
}

func (p *multiPool) appendValidRow(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int) (err error) {
	err = p.data.appendValidRow(proc, mp, bat, row, 1)

	for i, col := range columns {
		if !col.isNull(row) {
			p.space[i]--
		}
	}
	p.have++
	return err
}

func (p *multiPool) tryReplaceValidRow(mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row1, row2 int) (err error) {
	// the replacement action cannot destroy the original integrity of the sample pool.
	for j := range p.space {
		p.tSpace[j] = p.space[j]
		if columns[j].isNull(row2) {
			if p.space[j] < -1 {
				p.tSpace[j]++
			} else {
				return
			}
		}
	}

	p.tSpace, p.space = p.space, p.tSpace
	return p.data.replaceValidRow(mp, bat, row1, row2)
}

func (p *multiPool) addRows(proc *process.Process, columns sampleColumnList, bat *batch.Batch, factor float64) (err error) {
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
					err = p.appendValidRow(proc, proc.Mp(), columns, bat, i)
					if err != nil {
						return err
					}
					break
				}
			}
		}
	}

	var r int
	mp := proc.Mp()
	if factor == 1 {
		for ; i < k; i++ {
			if columns.isAllNull(i) {
				continue
			}
			p.seen++

			r = rand.Intn(p.seen)
			if r < p.have {
				err = p.tryReplaceValidRow(mp, columns, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	} else {
		fCap := float64(p.have)
		for ; i < k; i++ {
			if columns.isAllNull(i) {
				continue
			}
			p.seen++

			r = randWithFactor(fCap, p.seen, factor)
			if r < p.have {
				err = p.tryReplaceValidRow(mp, columns, bat, r, i)
				if err != nil {
					return err
				}
			}
		}
	}

	p.full = p.isFull()
	return nil
}

func (p *multiPool) addRowsByPercent(proc *process.Process, columns sampleColumnList, bat *batch.Batch, percent int) (err error) {
	if percent == 0 {
		return nil
	}

	k := bat.RowCount()

	for i := 0; i < k; i++ {
		if columns.isAllNull(i) {
			continue
		}
		if percent == 10000 || rand.Intn(10000) < percent {
			err = p.appendValidRow(proc, proc.Mp(), columns, bat, i)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *multiPool) addOneRow(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int, factor float64) (err error) {
	if columns.isAllNull(row) {
		return p.data.appendInvalidRow(proc, mp, bat, row)
	}

	if !p.full {
		p.seen++
		for colIndex := range p.space {
			if p.space[colIndex] > 0 && !columns[colIndex].isNull(row) {
				err = p.appendValidRow(proc, proc.Mp(), columns, bat, row)
				if err != nil {
					return err
				}
				break
			}
		}
		p.full = p.isFull()
	} else {
		p.seen++

		var r int
		if factor == 1 {
			r = rand.Intn(p.seen)
		} else {
			r = randWithFactor(float64(p.have), p.seen, factor)
		}
		if r < p.have {
			err = p.tryReplaceValidRow(mp, columns, bat, r, row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *multiPool) addOneRowByPercent(proc *process.Process, mp *mpool.MPool, columns sampleColumnList, bat *batch.Batch, row int, percent int) (err error) {
	if percent == 0 {
		return
	}

	if columns.isAllNull(row) {
		return p.data.appendInvalidRow(proc, mp, bat, row)
	}

	if percent == 10000 || rand.Intn(10000) < percent {
		err = p.appendValidRow(proc, mp, columns, bat, row)
	}
	return
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
