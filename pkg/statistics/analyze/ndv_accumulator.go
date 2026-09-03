// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyze

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrAccumulatorLimit = errors.New("analyze: NDV accumulator distinct-value limit exceeded")
	ErrAccumulatorState = errors.New("analyze: incompatible NDV accumulator state")
)

type ValueHash [16]byte

func HashValue(canonicalValue []byte) ValueHash {
	sum := sha256.Sum256(canonicalValue)
	var result ValueHash
	copy(result[:], sum[:len(result)])
	return result
}

// HashTypedValue includes the logical type fingerprint in the hash domain.
// Partial states with different column types can therefore never become
// accidentally interchangeable merely because their vector bytes match.
func HashTypedValue(typeID uint32, width, scale int32, canonicalValue []byte) ValueHash {
	valueDigest := sha256.Sum256(canonicalValue)
	var input [44]byte
	binary.BigEndian.PutUint32(input[0:4], typeID)
	binary.BigEndian.PutUint32(input[4:8], uint32(width))
	binary.BigEndian.PutUint32(input[8:12], uint32(scale))
	copy(input[12:], valueDigest[:])
	sum := sha256.Sum256(input[:])
	var result ValueHash
	copy(result[:], sum[:len(result)])
	return result
}

// NDVAccumulator keeps row-frequency and block-incidence frames separately.
// maxValues is a hard per-frame bound; a frame that crosses it is disabled
// rather than partially published.
type NDVAccumulator struct {
	maxValues uint64

	sampleRows uint64
	rowCounts  map[ValueHash]uint64
	rowErr     error

	incidenceBlocks uint64
	incidenceCounts map[ValueHash]uint64
	incidenceErr    error
	blockOpen       bool
	blockValues     map[ValueHash]struct{}
}

func NewNDVAccumulator(maxValues uint64) *NDVAccumulator {
	return &NDVAccumulator{
		maxValues:       maxValues,
		rowCounts:       make(map[ValueHash]uint64),
		incidenceCounts: make(map[ValueHash]uint64),
	}
}

func (a *NDVAccumulator) ObserveSampleValue(value ValueHash) error {
	if a == nil || a.maxValues == 0 {
		return ErrAccumulatorLimit
	}
	a.sampleRows++
	if a.rowErr != nil {
		return a.rowErr
	}
	if count, exists := a.rowCounts[value]; exists {
		if count == math.MaxUint64 {
			a.rowErr = ErrAccumulatorState
			clear(a.rowCounts)
			return a.rowErr
		}
		a.rowCounts[value] = count + 1
		return nil
	}
	if uint64(len(a.rowCounts)) == a.maxValues {
		a.rowErr = ErrAccumulatorLimit
		clear(a.rowCounts)
		return a.rowErr
	}
	a.rowCounts[value] = 1
	return nil
}

// BeginIncidenceBlock opens one admitted physical block. Every visible value
// from that block must be observed before EndIncidenceBlock. Repeated values in
// a block contribute one incidence, regardless of row count.
func (a *NDVAccumulator) BeginIncidenceBlock() error {
	if a == nil || a.blockOpen {
		return ErrAccumulatorState
	}
	a.blockOpen = true
	if a.incidenceErr == nil {
		if a.blockValues == nil {
			a.blockValues = make(map[ValueHash]struct{})
		} else {
			clear(a.blockValues)
		}
	}
	return nil
}

func (a *NDVAccumulator) ObserveIncidenceValue(value ValueHash) error {
	if a == nil || !a.blockOpen {
		return ErrAccumulatorState
	}
	if a.incidenceErr != nil {
		return a.incidenceErr
	}
	if _, exists := a.blockValues[value]; exists {
		return nil
	}
	if uint64(len(a.blockValues)) == a.maxValues {
		a.incidenceErr = ErrAccumulatorLimit
		a.blockValues = nil
		clear(a.incidenceCounts)
		return a.incidenceErr
	}
	a.blockValues[value] = struct{}{}
	return nil
}

func (a *NDVAccumulator) EndIncidenceBlock() error {
	if a == nil || !a.blockOpen {
		return ErrAccumulatorState
	}
	a.blockOpen = false
	a.incidenceBlocks++
	if a.incidenceErr != nil {
		return a.incidenceErr
	}
	for value := range a.blockValues {
		if count := a.incidenceCounts[value]; count == math.MaxUint64 {
			a.incidenceErr = ErrAccumulatorState
			clear(a.incidenceCounts)
			break
		} else if count == 0 && uint64(len(a.incidenceCounts)) == a.maxValues {
			a.incidenceErr = ErrAccumulatorLimit
			clear(a.incidenceCounts)
			break
		} else {
			a.incidenceCounts[value] = count + 1
		}
	}
	clear(a.blockValues)
	return a.incidenceErr
}

func (a *NDVAccumulator) Merge(other *NDVAccumulator) error {
	if a == nil || other == nil || a.blockOpen || other.blockOpen || a.maxValues != other.maxValues {
		return ErrAccumulatorState
	}
	if math.MaxUint64-a.sampleRows < other.sampleRows || math.MaxUint64-a.incidenceBlocks < other.incidenceBlocks {
		return ErrAccumulatorState
	}
	a.sampleRows += other.sampleRows
	a.incidenceBlocks += other.incidenceBlocks
	a.rowErr = mergeValueCounts(a.rowCounts, a.maxValues, a.rowErr, other.rowCounts, other.rowErr)
	if a.rowErr != nil {
		clear(a.rowCounts)
	}
	a.incidenceErr = mergeValueCounts(
		a.incidenceCounts, a.maxValues, a.incidenceErr,
		other.incidenceCounts, other.incidenceErr,
	)
	if a.incidenceErr != nil {
		clear(a.incidenceCounts)
	}
	if a.rowErr != nil && a.incidenceErr != nil {
		return ErrAccumulatorLimit
	}
	return nil
}

func mergeValueCounts(
	dst map[ValueHash]uint64,
	maxValues uint64,
	dstErr error,
	src map[ValueHash]uint64,
	srcErr error,
) error {
	if dstErr != nil {
		return dstErr
	}
	if srcErr != nil {
		return srcErr
	}
	for value, count := range src {
		current, exists := dst[value]
		if !exists && uint64(len(dst)) == maxValues {
			return ErrAccumulatorLimit
		}
		if math.MaxUint64-current < count {
			return ErrAccumulatorState
		}
		dst[value] = current + count
	}
	return nil
}

func (a *NDVAccumulator) Estimate(populationNonNull float64) (NDVEstimate, error) {
	if a == nil || a.blockOpen || a.rowErr != nil {
		return NDVEstimate{}, ErrAccumulatorState
	}
	singletons := uint64(0)
	for _, count := range a.rowCounts {
		if count == 1 {
			singletons++
		}
	}
	incidenceDistinct := uint64(0)
	incidenceSingletons := uint64(0)
	incidenceDoubletons := uint64(0)
	incidenceBlocks := uint64(0)
	if a.incidenceErr == nil {
		incidenceBlocks = a.incidenceBlocks
		incidenceDistinct = uint64(len(a.incidenceCounts))
		for _, count := range a.incidenceCounts {
			switch count {
			case 1:
				incidenceSingletons++
			case 2:
				incidenceDoubletons++
			}
		}
	}
	observedDistinct := uint64(len(a.rowCounts))
	if a.incidenceErr == nil {
		observedDistinct = uint64(len(a.incidenceCounts))
		for value := range a.rowCounts {
			if _, exists := a.incidenceCounts[value]; !exists {
				observedDistinct++
			}
		}
	}
	return EstimateSampledNDV(SampledNDVInput{
		PopulationRows:      populationNonNull,
		SampleRows:          a.sampleRows,
		SampleDistinct:      uint64(len(a.rowCounts)),
		SampleSingletons:    singletons,
		IncidenceBlocks:     incidenceBlocks,
		IncidenceDistinct:   incidenceDistinct,
		IncidenceSingletons: incidenceSingletons,
		IncidenceDoubletons: incidenceDoubletons,
		ObservedDistinct:    observedDistinct,
	})
}

func (a *NDVAccumulator) SampleRows() uint64 {
	if a == nil {
		return 0
	}
	return a.sampleRows
}

func (a *NDVAccumulator) RowStateError() error {
	if a == nil {
		return ErrAccumulatorState
	}
	return a.rowErr
}

func (a *NDVAccumulator) IncidenceStateError() error {
	if a == nil {
		return ErrAccumulatorState
	}
	return a.incidenceErr
}
