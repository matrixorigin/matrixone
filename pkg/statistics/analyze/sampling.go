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

// Package analyze contains deterministic, storage-independent algorithms used
// by manual ANALYZE. Storage inventory, visibility, admission, and publication
// stay with their respective owners; this package only transforms explicit,
// bounded inputs.
package analyze

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/big"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	SampleAlgorithmV1 = "STRATIFIED_BLOCK_SRS_V1"
	MaxHashBits       = 128

	domainBlockSelection = "analyze/block/v1"
	domainIncidenceBlock = "analyze/incidence-block/v1"
	domainRowSelection   = "analyze/row/v1"
)

var (
	ErrInvalidPopulation = moerr.NewInvalidInputNoCtx("analyze: invalid population")
	ErrSampleBudget      = moerr.NewInvalidInputNoCtx("analyze: sample budget cannot cover one block per stratum")
)

// Fraction is an exact probability in [0, 1].
type Fraction struct {
	Numerator   uint64
	Denominator uint64
}

func NewFraction(numerator, denominator uint64) (Fraction, error) {
	if denominator == 0 || numerator > denominator {
		return Fraction{}, ErrInvalidPopulation
	}
	if numerator == 0 {
		return Fraction{Denominator: 1}, nil
	}
	gcd := greatestCommonDivisor(numerator, denominator)
	return Fraction{Numerator: numerator / gcd, Denominator: denominator / gcd}, nil
}

func MustFraction(numerator, denominator uint64) Fraction {
	f, err := NewFraction(numerator, denominator)
	if err != nil {
		panic(err)
	}
	return f
}

func (f Fraction) valid() bool {
	return f.Denominator != 0 && f.Numerator <= f.Denominator
}

func (f Fraction) Float64() float64 {
	if !f.valid() {
		return math.NaN()
	}
	return float64(f.Numerator) / float64(f.Denominator)
}

func (f Fraction) Compare(other Fraction) int {
	if !f.valid() || !other.valid() {
		panic(ErrInvalidPopulation)
	}
	var left, right big.Int
	left.Mul(new(big.Int).SetUint64(f.Numerator), new(big.Int).SetUint64(other.Denominator))
	right.Mul(new(big.Int).SetUint64(other.Numerator), new(big.Int).SetUint64(f.Denominator))
	return left.Cmp(&right)
}

func minFraction(left, right Fraction) Fraction {
	if left.Compare(right) <= 0 {
		return left
	}
	return right
}

// Divide returns f / divisor. The caller must provide f <= divisor so the
// result remains a probability.
func (f Fraction) Divide(divisor Fraction) (Fraction, error) {
	if !f.valid() || !divisor.valid() || divisor.Numerator == 0 || f.Compare(divisor) > 0 {
		return Fraction{}, ErrInvalidPopulation
	}
	leftGCD := greatestCommonDivisor(f.Numerator, divisor.Numerator)
	rightGCD := greatestCommonDivisor(divisor.Denominator, f.Denominator)
	numerator := new(big.Int).Mul(
		new(big.Int).SetUint64(f.Numerator/leftGCD),
		new(big.Int).SetUint64(divisor.Denominator/rightGCD),
	)
	denominator := new(big.Int).Mul(
		new(big.Int).SetUint64(f.Denominator/rightGCD),
		new(big.Int).SetUint64(divisor.Numerator/leftGCD),
	)
	if !numerator.IsUint64() || !denominator.IsUint64() {
		return Fraction{}, ErrInvalidPopulation
	}
	return NewFraction(numerator.Uint64(), denominator.Uint64())
}

// Threshold128 returns floor(f * 2^128), saturated to the all-inclusive
// sentinel for probability one. The boolean distinguishes that sentinel from
// the 128-bit zero value.
func (f Fraction) Threshold128() (threshold [16]byte, all bool, err error) {
	if !f.valid() {
		return threshold, false, ErrInvalidPopulation
	}
	if f.Numerator == f.Denominator {
		return threshold, true, nil
	}
	numerator := new(big.Int).Lsh(new(big.Int).SetUint64(f.Numerator), MaxHashBits)
	numerator.Div(numerator, new(big.Int).SetUint64(f.Denominator))
	b := numerator.Bytes()
	copy(threshold[len(threshold)-len(b):], b)
	return threshold, false, nil
}

func greatestCommonDivisor(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

type SampleConfig struct {
	TargetRows uint64
	MinBlocks  uint64
	MaxBlocks  uint64
	MaxStrata  uint32
}

const (
	DefaultTargetRows = uint64(300_000)
	DefaultMinBlocks  = uint64(512)
	DefaultMaxBlocks  = uint64(4_096)
	DefaultMaxStrata  = uint32(64)
)

func DefaultSampleConfig() SampleConfig {
	return SampleConfig{
		TargetRows: DefaultTargetRows,
		MinBlocks:  DefaultMinBlocks,
		MaxBlocks:  DefaultMaxBlocks,
		MaxStrata:  DefaultMaxStrata,
	}
}

type Stratum struct {
	ID         uint32
	FirstBlock uint64
	Blocks     uint64
	Selected   uint64
}

func (s Stratum) Probability() Fraction {
	return MustFraction(s.Selected, s.Blocks)
}

type SampledBlock struct {
	LogicalOrdinal     uint64
	StratumID          uint32
	Physical           Fraction
	RowThreshold       [16]byte
	RowThresholdAll    bool
	IncidenceThreshold [16]byte
	IncidenceAll       bool
}

type SamplePlan struct {
	Algorithm        string
	PopulationRows   uint64
	PopulationBlocks uint64
	TargetRows       uint64
	Strata           []Stratum
	Blocks           []SampledBlock
	Q                Fraction
	QBlocks          Fraction
}

// PlanBlockSample selects logical block ordinals without replacement. It does
// not enumerate physical objects and therefore cannot trigger metadata or data
// I/O. The storage adapter maps the returned ordinals in a separately metered
// inventory pass.
func PlanBlockSample(populationRows, populationBlocks uint64, cfg SampleConfig, seed [32]byte) (SamplePlan, error) {
	if populationBlocks == 0 {
		if populationRows != 0 {
			return SamplePlan{}, ErrInvalidPopulation
		}
		one := MustFraction(1, 1)
		return SamplePlan{Algorithm: SampleAlgorithmV1, Q: one, QBlocks: one}, nil
	}
	if cfg.TargetRows == 0 || cfg.MaxBlocks == 0 || cfg.MaxStrata == 0 {
		return SamplePlan{}, ErrSampleBudget
	}
	strataCount := uint64(cfg.MaxStrata)
	if strataCount > populationBlocks {
		strataCount = populationBlocks
	}
	if strataCount > cfg.MaxBlocks {
		strataCount = cfg.MaxBlocks
	}
	if strataCount == 0 {
		return SamplePlan{}, ErrSampleBudget
	}

	strata := equalBlockStrata(populationBlocks, uint32(strataCount))
	for i := range strata {
		strata[i].Selected = 1
	}
	selected := strataCount
	target := MustFraction(1, 1)
	if populationRows > cfg.TargetRows {
		target = MustFraction(cfg.TargetRows, populationRows)
	}
	minBlocks := min(cfg.MinBlocks, cfg.MaxBlocks, populationBlocks)
	for selected < cfg.MaxBlocks {
		minID := minimumCoverageStratum(strata)
		if selected >= minBlocks && strata[minID].Probability().Compare(target) >= 0 {
			break
		}
		if strata[minID].Selected == strata[minID].Blocks {
			break
		}
		strata[minID].Selected++
		selected++
	}

	qBlocks := strata[0].Probability()
	for i := 1; i < len(strata); i++ {
		qBlocks = minFraction(qBlocks, strata[i].Probability())
	}
	q := minFraction(qBlocks, target)
	blocks := make([]SampledBlock, 0, selected)
	for _, stratum := range strata {
		stream := newHashStream(seed, domainBlockSelection, uint64(stratum.ID))
		local := floydSample(stratum.Blocks, stratum.Selected, stream)
		physical := stratum.Probability()
		rowConditional, err := q.Divide(physical)
		if err != nil {
			return SamplePlan{}, err
		}
		incidenceConditional, err := qBlocks.Divide(physical)
		if err != nil {
			return SamplePlan{}, err
		}
		rowThreshold, rowAll, err := rowConditional.Threshold128()
		if err != nil {
			return SamplePlan{}, err
		}
		incidenceThreshold, incidenceAll, err := incidenceConditional.Threshold128()
		if err != nil {
			return SamplePlan{}, err
		}
		for _, ordinal := range local {
			blocks = append(blocks, SampledBlock{
				LogicalOrdinal:     stratum.FirstBlock + ordinal,
				StratumID:          stratum.ID,
				Physical:           physical,
				RowThreshold:       rowThreshold,
				RowThresholdAll:    rowAll,
				IncidenceThreshold: incidenceThreshold,
				IncidenceAll:       incidenceAll,
			})
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].LogicalOrdinal < blocks[j].LogicalOrdinal })
	return SamplePlan{
		Algorithm:        SampleAlgorithmV1,
		PopulationRows:   populationRows,
		PopulationBlocks: populationBlocks,
		TargetRows:       cfg.TargetRows,
		Strata:           strata,
		Blocks:           blocks,
		Q:                q,
		QBlocks:          qBlocks,
	}, nil
}

func equalBlockStrata(blocks uint64, count uint32) []Stratum {
	strata := make([]Stratum, count)
	base := blocks / uint64(count)
	remainder := blocks % uint64(count)
	var first uint64
	for i := range strata {
		size := base
		if uint64(i) < remainder {
			size++
		}
		strata[i] = Stratum{ID: uint32(i), FirstBlock: first, Blocks: size}
		first += size
	}
	return strata
}

func minimumCoverageStratum(strata []Stratum) int {
	best := -1
	for i := range strata {
		if strata[i].Selected == strata[i].Blocks {
			continue
		}
		if best == -1 || strata[i].Probability().Compare(strata[best].Probability()) < 0 {
			best = i
		}
	}
	if best == -1 {
		return 0
	}
	return best
}

type hashStream struct {
	seed    [32]byte
	domain  string
	scope   uint64
	counter uint64
}

func newHashStream(seed [32]byte, domain string, scope uint64) *hashStream {
	return &hashStream{seed: seed, domain: domain, scope: scope}
}

func (s *hashStream) next() uint64 {
	h := sha256.New()
	writeFramed(h, []byte(s.domain))
	writeFramed(h, s.seed[:])
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], s.scope)
	writeFramed(h, encoded[:])
	binary.BigEndian.PutUint64(encoded[:], s.counter)
	writeFramed(h, encoded[:])
	s.counter++
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func (s *hashStream) bounded(bound uint64) uint64 {
	if bound == 0 {
		panic(ErrInvalidPopulation)
	}
	threshold := -bound % bound
	for {
		value := s.next()
		if value >= threshold {
			return value % bound
		}
	}
}

func floydSample(population, count uint64, stream *hashStream) []uint64 {
	if count > population {
		panic(ErrInvalidPopulation)
	}
	selected := make(map[uint64]struct{}, count)
	for j := population - count; j < population; j++ {
		candidate := stream.bounded(j + 1)
		if _, exists := selected[candidate]; exists {
			selected[j] = struct{}{}
		} else {
			selected[candidate] = struct{}{}
		}
	}
	result := make([]uint64, 0, count)
	for ordinal := range selected {
		result = append(result, ordinal)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeFramed(w hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write(value)
}

func hash128(seed [32]byte, domain string, parts ...[]byte) [16]byte {
	h := sha256.New()
	writeFramed(h, []byte(domain))
	writeFramed(h, seed[:])
	for _, part := range parts {
		writeFramed(h, part)
	}
	var result [16]byte
	copy(result[:], h.Sum(nil)[:16])
	return result
}

func BelowThreshold(hash, threshold [16]byte, all bool) bool {
	return all || bytes.Compare(hash[:], threshold[:]) < 0
}

func RetainRow(seed [32]byte, rowIdentity []byte, threshold [16]byte, all bool) bool {
	if all {
		return true
	}
	return BelowThreshold(hash128(seed, domainRowSelection, rowIdentity), threshold, false)
}

func RetainIncidenceBlock(seed [32]byte, blockIdentity []byte, threshold [16]byte, all bool) bool {
	if all {
		return true
	}
	return BelowThreshold(hash128(seed, domainIncidenceBlock, blockIdentity), threshold, false)
}
