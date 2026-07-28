// Copyright 2021 - 2023 Matrix Origin
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

package statistic

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
)

type StatsArray [StatsArrayLength]float64

const (
	Decimal128ToFloat64Scale = 5
	Float64PrecForMemorySize = 3
	Float64PrecForCU         = 4
	Float64PrecForIOInput    = 6
)

const StatsArrayVersion = StatsArrayVersionLatest

const (
	StatsArrayVersion0 = iota // raw statistics

	StatsArrayVersion1 = 1 // float64 array
	StatsArrayVersion2 = 2 // float64 array + plus one elem OutTrafficBytes
	StatsArrayVersion3 = 3 // ... + 1 elem: ConnType
	StatsArrayVersion4 = 4 // ... + 2 elem: OutPacketCount, CU
	StatsArrayVersion5 = 5 // ... + 1 elem: S3IOListCount, S3IODeleteCount
	StatsArrayVersion6 = 6 // ... + 6 resource quality/byte/wait/retry fields

	StatsArrayVersionLatest // same value as last variable StatsArrayVersion#
)

const (
	StatsArrayIndexVersion = iota
	StatsArrayIndexTimeConsumed
	StatsArrayIndexMemorySize
	StatsArrayIndexS3IOInputCount
	StatsArrayIndexS3IOOutputCount // index: 4
	StatsArrayIndexOutTrafficBytes // index: 5
	StatsArrayIndexConnType        // index: 6
	StatsArrayIndexOutPacketCnt    // index: 7, version: 4
	StatsArrayIndexCU              // index: 8, version: 4
	StatsArrayIndexS3IOListCount   // index: 9, version: 5
	StatsArrayIndexS3IODeleteCount // index: 10, version: 5
	StatsArrayIndexQualityFlags    // index: 11, version: 6
	StatsArrayIndexS3ReadBytes     // index: 12, version: 6
	StatsArrayIndexS3WriteBytes    // index: 13, version: 6
	StatsArrayIndexSpillBytes      // index: 14, version: 6
	StatsArrayIndexTotalWaitNS     // index: 15, version: 6
	StatsArrayIndexAttemptCount    // index: 16, version: 6

	StatsArrayLength
)

const (
	StatsArrayLengthV1 = 5
	StatsArrayLengthV2 = 6
	StatsArrayLengthV3 = 7
	StatsArrayLengthV4 = 9
	StatsArrayLengthV5 = 11
	StatsArrayLengthV6 = 17
)

type ConnType float64

const (
	ConnTypeUnknown  ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

func NewStatsArray() *StatsArray {
	var s StatsArray
	return s.Init()
}

func NewStatsArrayV1() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion1)
}

func NewStatsArrayV2() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion2)
}

func NewStatsArrayV3() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion3)
}

func NewStatsArrayV4() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion4)
}

func NewStatsArrayV6() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion6)
}

func (s *StatsArray) Init() *StatsArray {
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) InitIfEmpty() *StatsArray {
	for i := 1; i < StatsArrayLength; i++ {
		if s[i] != 0 {
			return s
		}
	}
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) Reset() *StatsArray {
	*s = *initStatsArray
	return s
}

func (s *StatsArray) GetVersion() float64         { return (*s)[StatsArrayIndexVersion] }
func (s *StatsArray) GetTimeConsumed() float64    { return (*s)[StatsArrayIndexTimeConsumed] }    // unit: ns
func (s *StatsArray) GetMemorySize() float64      { return (*s)[StatsArrayIndexMemorySize] }      // unit: byte
func (s *StatsArray) GetS3IOInputCount() float64  { return (*s)[StatsArrayIndexS3IOInputCount] }  // unit: count
func (s *StatsArray) GetS3IOOutputCount() float64 { return (*s)[StatsArrayIndexS3IOOutputCount] } // unit: count
func (s *StatsArray) GetOutTrafficBytes() float64 { // unit: byte
	if s.GetVersion() < StatsArrayVersion2 {
		return 0
	}
	return (*s)[StatsArrayIndexOutTrafficBytes]
}
func (s *StatsArray) GetConnType() float64 {
	if s.GetVersion() < StatsArrayVersion3 {
		return 0
	}
	return (*s)[StatsArrayIndexConnType]
}
func (s *StatsArray) GetOutPacketCount() float64 {
	if s.GetVersion() < StatsArrayVersion4 {
		return 0
	}
	return s[StatsArrayIndexOutPacketCnt]
}
func (s *StatsArray) GetCU() float64 {
	if s.GetVersion() < StatsArrayVersion4 {
		return 0
	}
	return s[StatsArrayIndexCU]
}
func (s *StatsArray) GetS3IOListCount() float64 {
	if s.GetVersion() < StatsArrayVersion5 {
		return 0
	}
	return s[StatsArrayIndexS3IOListCount]
}
func (s *StatsArray) GetS3IODeleteCount() float64 {
	if s.GetVersion() < StatsArrayVersion5 {
		return 0
	}
	return s[StatsArrayIndexS3IODeleteCount]
}

func (s *StatsArray) GetQualityFlags() resource.QualityFlags {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return resource.QualityFlags((*s)[StatsArrayIndexQualityFlags])
}

func (s *StatsArray) GetS3ReadBytes() float64 {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return (*s)[StatsArrayIndexS3ReadBytes]
}

func (s *StatsArray) GetS3WriteBytes() float64 {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return (*s)[StatsArrayIndexS3WriteBytes]
}

func (s *StatsArray) GetSpillBytes() float64 {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return (*s)[StatsArrayIndexSpillBytes]
}

func (s *StatsArray) GetTotalWaitNS() float64 {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return (*s)[StatsArrayIndexTotalWaitNS]
}

func (s *StatsArray) GetAttemptCount() float64 {
	if s.GetVersion() < StatsArrayVersion6 {
		return 0
	}
	return (*s)[StatsArrayIndexAttemptCount]
}

func (s *StatsArray) IsAggregated() bool {
	return s.GetQualityFlags()&resource.QualityAggregated != 0
}

// WithVersion set the version array in StatsArray, please carefully to use.
func (s *StatsArray) WithVersion(v float64) *StatsArray { (*s)[StatsArrayIndexVersion] = v; return s }
func (s *StatsArray) WithTimeConsumed(v float64) *StatsArray {
	(*s)[StatsArrayIndexTimeConsumed] = v
	return s
}
func (s *StatsArray) WithMemorySize(v float64) *StatsArray {
	(*s)[StatsArrayIndexMemorySize] = v
	return s
}
func (s *StatsArray) WithS3IOInputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOInputCount] = v
	return s
}
func (s *StatsArray) WithS3IOOutputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOOutputCount] = v
	return s
}
func (s *StatsArray) WithS3IOListCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOListCount] = v
	return s
}
func (s *StatsArray) WithS3IODeleteCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IODeleteCount] = v
	return s
}

func (s *StatsArray) WithQualityFlags(v resource.QualityFlags) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexQualityFlags] = float64(v)
	}
	return s
}

func (s *StatsArray) WithS3ReadBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexS3ReadBytes] = v
	}
	return s
}

func (s *StatsArray) WithS3WriteBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexS3WriteBytes] = v
	}
	return s
}

func (s *StatsArray) WithSpillBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexSpillBytes] = v
	}
	return s
}

func (s *StatsArray) WithTotalWaitNS(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexTotalWaitNS] = v
	}
	return s
}

func (s *StatsArray) WithAttemptCount(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 {
		(*s)[StatsArrayIndexAttemptCount] = v
	}
	return s
}

func (s *StatsArray) WithOutTrafficBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion2 {
		(*s)[StatsArrayIndexOutTrafficBytes] = v
	}
	return s
}

func (s *StatsArray) WithConnType(v ConnType) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion3 {
		(*s)[StatsArrayIndexConnType] = float64(v)
	}
	return s
}

func (s *StatsArray) WithOutPacketCount(v float64) *StatsArray {
	s[StatsArrayIndexOutPacketCnt] = v
	return s
}

func (s *StatsArray) WithCU(v float64) *StatsArray {
	s[StatsArrayIndexCU] = v
	return s
}

func (s *StatsArray) ToJsonString() []byte {
	switch s.GetVersion() {
	case StatsArrayVersion0:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV5])
	case StatsArrayVersion1:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV1])
	case StatsArrayVersion2:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV2])
	case StatsArrayVersion3:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV3])
	case StatsArrayVersion4:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV4])
	case StatsArrayVersion5:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV5])
	case StatsArrayVersion6:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV6])
	default:
		return StatsArrayToJsonString((*s)[:])
	}
}

// Add do add two stats array together
// except for Element ConnType, which idx = StatsArrayIndexConnType, just keep s[StatsArrayIndexConnType] value.
func (s *StatsArray) Add(delta *StatsArray) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion6 && delta.GetVersion() >= StatsArrayVersion6 {
		return s.addV6(delta)
	}
	if s.GetVersion() < StatsArrayVersion6 && delta.GetVersion() >= StatsArrayVersion6 {
		// A legacy memory value is a statement-level sum, not a physical
		// memory-domain peak.  Promote it with that value deliberately omitted,
		// then merge the genuine v6 operand.
		legacy := *s
		s.promoteLegacy(legacy)
		return s.addV6(delta)
	}
	if s.GetVersion() >= StatsArrayVersion6 && delta.GetVersion() < StatsArrayVersion6 {
		// The legacy operand contributes all compatible counters, but its legacy
		// memory value must never become a v6 peak.
		s.WithQualityFlags(s.GetQualityFlags() | resource.QualityPartial | resource.QualityMissingMemoryDomain | resource.QualityAggregated)
		return s.addV6WithoutMemory(delta)
	}
	dstLen := len(*delta)
	if len(*s) < len(*delta) {
		dstLen = len(*s)
	}
	for idx := 1; idx < dstLen; idx++ {
		if idx == StatsArrayIndexConnType {
			continue
		}
		(*s)[idx] += (*delta)[idx]
	}
	return s
}

// promoteLegacy changes a v0-v5 receiver to v6 while retaining only fields whose
// meaning is unchanged between the schemas.  The caller supplies a copy so
// that promotion is safe even when the source aliases the receiver.
func (s *StatsArray) promoteLegacy(legacy StatsArray) {
	*s = StatsArray{}
	(*s)[StatsArrayIndexVersion] = StatsArrayVersion6
	for idx := StatsArrayIndexTimeConsumed; idx < StatsArrayLengthV5; idx++ {
		if idx != StatsArrayIndexMemorySize {
			(*s)[idx] = legacy[idx]
		}
	}
	s.WithQualityFlags(resource.QualityPartial | resource.QualityMissingMemoryDomain | resource.QualityAggregated)
}

func (s *StatsArray) addV6(delta *StatsArray) *StatsArray {
	return s.addV6Memory(delta, true)
}

func (s *StatsArray) addV6WithoutMemory(delta *StatsArray) *StatsArray {
	return s.addV6Memory(delta, false)
}

func (s *StatsArray) addV6Memory(delta *StatsArray, deltaHasMemory bool) *StatsArray {
	flags := s.GetQualityFlags() | delta.GetQualityFlags()
	for _, idx := range [...]int{
		StatsArrayIndexTimeConsumed,
		StatsArrayIndexS3IOInputCount,
		StatsArrayIndexS3IOOutputCount,
		StatsArrayIndexOutTrafficBytes,
		StatsArrayIndexOutPacketCnt,
		StatsArrayIndexCU,
		StatsArrayIndexS3IOListCount,
		StatsArrayIndexS3IODeleteCount,
		StatsArrayIndexS3ReadBytes,
		StatsArrayIndexS3WriteBytes,
		StatsArrayIndexSpillBytes,
		StatsArrayIndexTotalWaitNS,
		StatsArrayIndexAttemptCount,
	} {
		(*s)[idx] += (*delta)[idx]
		if idx != StatsArrayIndexCU && (*s)[idx] > float64(maxExactFloatInteger) {
			flags |= resource.QualityProjectionOverflow
		}
	}
	if deltaHasMemory && (*delta)[StatsArrayIndexMemorySize] > (*s)[StatsArrayIndexMemorySize] {
		(*s)[StatsArrayIndexMemorySize] = (*delta)[StatsArrayIndexMemorySize]
	}
	if s.GetConnType() == float64(ConnTypeUnknown) {
		(*s)[StatsArrayIndexConnType] = (*delta)[StatsArrayIndexConnType]
	} else if delta.GetConnType() != float64(ConnTypeUnknown) && s.GetConnType() != delta.GetConnType() {
		flags |= resource.QualityInvariantFailure
	}
	s.WithQualityFlags(flags)
	return s
}

// DecodeStatsArray validates and decodes every statement-stat schema emitted
// by MatrixOne. In particular, it rejects malformed JSON, non-arrays,
// non-finite numbers, unsupported versions, and version/length mismatches
// before exposing a StatsArray to aggregation.
func DecodeStatsArray(data []byte) (StatsArray, error) {
	var rawValues []json.RawMessage
	if err := json.Unmarshal(data, &rawValues); err != nil {
		return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: %v", err)
	}
	if rawValues == nil {
		return StatsArray{}, moerr.NewInvalidInputNoCtx("decode stats array: expected JSON array")
	}
	if len(rawValues) == 0 {
		return StatsArray{}, moerr.NewInvalidInputNoCtx("decode stats array: empty array")
	}
	values := make([]float64, len(rawValues))
	for idx, raw := range rawValues {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: null value at index %d", idx)
		}
		if err := json.Unmarshal(raw, &values[idx]); err != nil {
			return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: value at index %d: %v", idx, err)
		}
		value := values[idx]
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: non-finite value at index %d", idx)
		}
	}
	version := values[StatsArrayIndexVersion]
	if version != math.Trunc(version) {
		return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: version %v is not integral", version)
	}
	var wantLen int
	switch int(version) {
	case StatsArrayVersion0:
		wantLen = StatsArrayLengthV5
	case StatsArrayVersion1:
		wantLen = StatsArrayLengthV1
	case StatsArrayVersion2:
		wantLen = StatsArrayLengthV2
	case StatsArrayVersion3:
		wantLen = StatsArrayLengthV3
	case StatsArrayVersion4:
		wantLen = StatsArrayLengthV4
	case StatsArrayVersion5:
		wantLen = StatsArrayLengthV5
	case StatsArrayVersion6:
		wantLen = StatsArrayLengthV6
	default:
		return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: unsupported version %v", version)
	}
	if len(values) != wantLen {
		return StatsArray{}, moerr.NewInvalidInputNoCtxf("decode stats array: version %d requires %d values, got %d", int(version), wantLen, len(values))
	}
	var result StatsArray
	copy(result[:], values)
	return result, nil
}

const maxExactFloatInteger = uint64(1 << 53)

// FromResourceSummary is the single StatsArray v6 projection. CU is supplied
// by the caller because pricing remains downstream of resource accounting.
func FromResourceSummary(summary resource.StatementResourceSummary, cu float64) StatsArray {
	flags := summary.Quality
	totalWait, waitFlags := summary.Usage.TotalWaitNS()
	flags |= waitFlags
	getAndHead := projectionAdd(
		summary.Usage.S3Requests[resource.S3Head],
		summary.Usage.S3Requests[resource.S3Get],
		&flags,
	)
	deleteAll := projectionAdd(
		summary.Usage.S3Requests[resource.S3Delete],
		summary.Usage.S3Requests[resource.S3DeleteMulti],
		&flags,
	)
	values := [...]uint64{
		summary.Usage.ExclusiveActiveNS,
		summary.Memory.MaxDomainPeakLiveBytes,
		summary.Usage.S3Requests[resource.S3Put],
		getAndHead,
		summary.Usage.ClientEgressBytes,
		summary.OutputPacketCount,
		summary.Usage.S3Requests[resource.S3List],
		deleteAll,
		summary.Usage.S3ReadBytes,
		summary.Usage.S3WriteBytes,
		summary.Usage.SpillBytes,
		totalWait,
		summary.AttemptCount,
	}
	for _, value := range values {
		if value > maxExactFloatInteger {
			flags |= resource.QualityProjectionOverflow
			break
		}
	}

	stats := NewStatsArrayV6()
	stats.WithTimeConsumed(float64(summary.Usage.ExclusiveActiveNS)).
		WithMemorySize(float64(summary.Memory.MaxDomainPeakLiveBytes)).
		WithS3IOInputCount(float64(summary.Usage.S3Requests[resource.S3Put])).
		WithS3IOOutputCount(float64(getAndHead)).
		WithOutTrafficBytes(float64(summary.Usage.ClientEgressBytes)).
		WithConnType(resourceConnType(summary.ConnType)).
		WithOutPacketCount(float64(summary.OutputPacketCount)).
		WithCU(cu).
		WithS3IOListCount(float64(summary.Usage.S3Requests[resource.S3List])).
		WithS3IODeleteCount(float64(deleteAll)).
		WithQualityFlags(flags).
		WithS3ReadBytes(float64(summary.Usage.S3ReadBytes)).
		WithS3WriteBytes(float64(summary.Usage.S3WriteBytes)).
		WithSpillBytes(float64(summary.Usage.SpillBytes)).
		WithTotalWaitNS(float64(totalWait)).
		WithAttemptCount(float64(summary.AttemptCount))
	return *stats
}

func projectionAdd(a, b uint64, flags *resource.QualityFlags) uint64 {
	if math.MaxUint64-a < b {
		*flags |= resource.QualityInvariantFailure | resource.QualityProjectionOverflow
		return math.MaxUint64
	}
	return a + b
}

func resourceConnType(conn resource.ConnType) ConnType {
	switch conn {
	case resource.ConnInternal:
		return ConnTypeInternal
	case resource.ConnExternal:
		return ConnTypeExternal
	default:
		return ConnTypeUnknown
	}
}

// StatsArrayToJsonString return json arr format
// example:
// [1,0,0,0,0] got `[1,0,0,0,0]`
// [1,2,3,4,5] got `[1,2,3.000,4,5]`
// [2,1,2,3,4,5] got `[2,3.000,4,5,6.000,7]`
func StatsArrayToJsonString(arr []float64) []byte {
	// len([1,184467440737095516161,18446744073709551616,18446744073709551616,18446744073709551616]") = 88
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	for idx, v := range arr {
		if idx > 0 {
			buf = append(buf, ',')
		}
		if v == 0.0 {
			buf = append(buf, '0')
		} else if idx == StatsArrayIndexMemorySize {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForMemorySize, 64)
		} else if idx == StatsArrayIndexCU {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForCU, 64)
		} else if idx == StatsArrayIndexS3IOInputCount {
			if float64(int(v)) == v {
				buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
			} else {
				buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForIOInput, 64)
			}
		} else {
			buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
		}
	}
	buf = append(buf, ']')
	return buf
}

var initStatsArray = NewStatsArray()

var DefaultStatsArray = *initStatsArray

var DefaultStatsArrayJsonString = initStatsArray.ToJsonString()

type statsInfoKey struct{}

type StatsInfo struct {
	resourceClaimed uint32

	ParseStage struct {
		ParseDuration  time.Duration `json:"ParseDuration"`
		ParseStartTime time.Time     `json:"ParseStartTime"`
	}

	// Planning Phase Statistics
	PlanStage struct {
		PlanDuration                time.Duration `json:"PlanDuration"`
		PlanStartTime               time.Time     `json:"PlanStartTime"`
		BuildPlanS3Request          S3Request     `json:"BuildPlanS3Request"`
		BuildPlanStatsIOConsumption int64         `json:"BuildPlanStatsIOConsumption"` // unit: ns
		// The following attributes belong to independent statistics during the `buildPlan` stage, only for analysis reference.
		BuildPlanStatsS3              S3Request `json:"BuildPlanStatsS3"`
		BuildPlanStatsDuration        int64     `json:"BuildPlanStatsDuration"`        // unit: ns
		BuildPlanStatsInCacheDuration int64     `json:"BuildPlanStatsInCacheDuration"` // unit: ns
		BuildPlanResolveVarDuration   int64     `json:"BuildPlanResolveVarDuration"`   // unit: ns
	}

	// Compile phase statistics
	CompileStage struct {
		CompileDuration       time.Duration `json:"CompileDuration"`
		CompileStartTime      time.Time     `json:"CompileStartTime"`
		CompileS3Request      S3Request     `json:"CompileS3Request"`
		CompileExpandRangesS3 S3Request     `json:"CompileExpandRangesS3"`
		CompileIOConsumption  int64         `json:"CompileIOConsumption"`
		// It belongs to independent statistics, which occurs during the `CompileQuery` stage, only for analysis reference.
		CompileTableScanDuration int64 `json:"CompileTableScanDuration"` // unit: ns
	}

	// Prepare execution phase statistics
	PrepareRunStage struct {
		CompilePreRunOnceDuration int64 `json:"CompilePreRunOnceDuration"` // unit: ns
		// During Compile PreRun, wait for the lock time when executing `locktable` and `lockMetaTables`
		CompilePreRunOnceWaitLock int64 `json:"CompilePreRunOnceWaitLock"` // unit: ns

		// ScopePrepareDuration belongs to concurrent merge time
		ScopePrepareDuration  int64     `json:"ScopePrepareDuration"` // unit: ns
		ScopePrepareS3Request S3Request `json:"ScopePrepareS3Request"`
		// It belongs to independent statistics, which occurs during the `PrepareRun` stage, only for analysis reference.
		BuildReaderDuration int64 `json:"BuildReaderDuration"` // unit: ns
	}

	// Execution phase statistics
	ExecuteStage struct {
		ExecutionDuration  time.Duration `json:"ExecutionDuration"`
		ExecutionStartTime time.Time     `json:"ExecutionStartTime"`
		ExecutionEndTime   time.Time     `json:"ExecutionEndTime"`

		// time consumption of output operator response to the query result set
		OutputDuration int64 `json:"OutputDuration"` // unit: ns
	}

	// Used to record statistics of additional operations, which are not included in the above stages
	OtherStage struct {
		TxnIncrStatementS3 S3Request `json:"TxnIncrStatementS3"`
	}
	// stats: [5,241837539,5622976.000,0,0,149,0,1,8.4507,0,0]
	// stats: [5,241837539,5622976.000,0,0,149,0,1,8.4507,0,0]
	PermissionAuth StatsArray `json:"PermissionAuth"`

	// FileService(S3 or localFS) Read Data time Consumption
	IOAccessTimeConsumption int64
	// S3 FileService Prefetch File IOMerge time Consumption
	S3FSPrefetchFileIOMergerTimeConsumption int64

	// Local FileService blocking wait IOMerge time Consumption, which is included in IOAccessTimeConsumption
	LocalFSReadIOMergerTimeConsumption int64
	// S3 FileService blocking wait IOMerge time Consumption, which is included in IOAccessTimeConsumption
	S3FSReadIOMergerTimeConsumption int64

	WaitActiveCost time.Duration `json:"WaitActive"`
}

// S3Request structure is used to record the number of times each S3 operation is performed
type S3Request struct {
	List      int64 `json:"List,omitempty"`
	Head      int64 `json:"Head,omitempty"`
	Put       int64 `json:"Put,omitempty"`
	Get       int64 `json:"Get,omitempty"`
	Delete    int64 `json:"Delete,omitempty"`
	DeleteMul int64 `json:"DeleteMul,omitempty"`
}

// CountLIST return s.List.
// Diff: 1) aws/aliyun treats List as PUT; 2) tencent cloud/huaweicloud treats List as GET
// cc https://github.com/matrixorigin/MO-Cloud/issues/4175#issuecomment-2375813480
func (s S3Request) CountLIST() int64   { return s.List }
func (s S3Request) CountPUT() int64    { return s.Put }
func (s S3Request) CountGET() int64    { return s.Head + s.Get }
func (s S3Request) CountDELETE() int64 { return s.Delete + s.DeleteMul }

func NewStatsInfo() *StatsInfo {
	s := new(StatsInfo)
	s.PermissionAuth.Reset()
	return s
}

func (stats *StatsInfo) CompileStart() {
	if stats == nil {
		return
	}
	if !stats.CompileStage.CompileStartTime.IsZero() {
		return
	}
	stats.CompileStage.CompileStartTime = time.Now()
}

func (stats *StatsInfo) CompileEnd() {
	if stats == nil {
		return
	}
	stats.CompileStage.CompileDuration = time.Since(stats.CompileStage.CompileStartTime)
}

func (stats *StatsInfo) PlanStart() {
	if stats == nil {
		return
	}
	stats.PlanStage.PlanStartTime = time.Now()
}

func (stats *StatsInfo) PlanEnd() {
	if stats == nil {
		return
	}
	stats.PlanStage.PlanDuration = time.Since(stats.PlanStage.PlanStartTime)
}

func (stats *StatsInfo) ExecutionStart() {
	if stats == nil {
		return
	}
	stats.ExecuteStage.ExecutionStartTime = time.Now()
}

func (stats *StatsInfo) ExecutionEnd() {
	if stats == nil {
		return
	}
	stats.ExecuteStage.ExecutionEndTime = time.Now()
	stats.ExecuteStage.ExecutionDuration = stats.ExecuteStage.ExecutionEndTime.Sub(stats.ExecuteStage.ExecutionStartTime)
}

func (stats *StatsInfo) AddOutputTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.ExecuteStage.OutputDuration, int64(d))
}

func (stats *StatsInfo) AddBuildReaderTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.BuildReaderDuration, int64(d))
}

func (stats *StatsInfo) AddIOAccessTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.IOAccessTimeConsumption, int64(d))
}

func (stats *StatsInfo) AddLocalFSReadIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.LocalFSReadIOMergerTimeConsumption, int64(d))
}
func (stats *StatsInfo) AddS3FSPrefetchFileIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.S3FSPrefetchFileIOMergerTimeConsumption, int64(d))
}
func (stats *StatsInfo) AddS3FSReadIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.S3FSReadIOMergerTimeConsumption, int64(d))
}

func (stats *StatsInfo) ResetIOMergerTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.LocalFSReadIOMergerTimeConsumption, 0)
	atomic.StoreInt64(&stats.S3FSPrefetchFileIOMergerTimeConsumption, 0)
	atomic.StoreInt64(&stats.S3FSReadIOMergerTimeConsumption, 0)
}

func (stats *StatsInfo) ResetIOAccessTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.IOAccessTimeConsumption, 0)
}

func (stats *StatsInfo) ResetBuildReaderTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.BuildReaderDuration, 0)
}

func (stats *StatsInfo) ResetCompilePreRunOnceDuration() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceDuration, 0)
}

func (stats *StatsInfo) ResetCompilePreRunOnceWaitLock() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceWaitLock, 0)
}

func (stats *StatsInfo) ResetScopePrepareDuration() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.ScopePrepareDuration, 0)
}

func (stats *StatsInfo) IOMergerTimeConsumption() int64 {
	if stats == nil {
		return 0
	}
	return stats.LocalFSReadIOMergerTimeConsumption +
		stats.S3FSPrefetchFileIOMergerTimeConsumption +
		stats.S3FSReadIOMergerTimeConsumption
}

func (stats *StatsInfo) AddBuildPlanStatsConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsDuration, int64(d))
}

func (stats *StatsInfo) AddBuildPlanStatsIOConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsIOConsumption, int64(d))
}

func (stats *StatsInfo) AddStatsStatsInCacheDuration(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsInCacheDuration, int64(d))
}

func (stats *StatsInfo) AddBuildPlanResolveVarConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanResolveVarDuration, int64(d))
}

func (stats *StatsInfo) AddCompileTableScanConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileTableScanDuration, int64(d))
}

func (stats *StatsInfo) AddBuildPlanS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.List, sreq.List)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddBuildPlanStatsS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.List, sreq.List)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Head, sreq.Head)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Put, sreq.Put)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Get, sreq.Get)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddCompileS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.List, sreq.List)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddCompileIOConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileIOConsumption, int64(d))
}

// CompileExpandRangesS3Request
func (stats *StatsInfo) CompileExpandRangesS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.List, sreq.List)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Head, sreq.Head)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Put, sreq.Put)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Get, sreq.Get)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddScopePrepareS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.List, sreq.List)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddTxnIncrStatementS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.List, sreq.List)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Head, sreq.Head)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Put, sreq.Put)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Get, sreq.Get)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.DeleteMul, sreq.DeleteMul)
}

func (stats *StatsInfo) AddScopePrepareDuration(d int64) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareDuration, d)
}

// NOTE: CompilePreRunOnceDuration is a one-time statistic and does not require accumulation
func (stats *StatsInfo) StoreCompilePreRunOnceDuration(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceDuration, int64(d))
}

func (stats *StatsInfo) AddPreRunOnceWaitLockDuration(d int64) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.CompilePreRunOnceWaitLock, d)
}

//--------------------------------------------------------------------------------------------------------------

func (stats *StatsInfo) SetWaitActiveCost(cost time.Duration) {
	if stats == nil {
		return
	}
	stats.WaitActiveCost = cost
}

// reset StatsInfo into zero state
func (stats *StatsInfo) Reset() {
	if stats == nil {
		return
	}
	*stats = StatsInfo{}
	stats.PermissionAuth.Reset()
}

func ContextWithStatsInfo(requestCtx context.Context, stats *StatsInfo) context.Context {
	return context.WithValue(requestCtx, statsInfoKey{}, stats)
}

func StatsInfoFromContext(requestCtx context.Context) *StatsInfo {
	if requestCtx == nil {
		return nil
	}
	if stats, ok := requestCtx.Value(statsInfoKey{}).(*StatsInfo); ok {
		return stats
	}
	return nil
}

// EnsureStatsInfoCanBeFound ensure a statement statistic is set in context, if not, copy one from another context, this function is copied from EnsureStatementProfiler
func EnsureStatsInfoCanBeFound(ctx context.Context, from context.Context) context.Context {
	if v := ctx.Value(statsInfoKey{}); v != nil {
		// already set
		return ctx
	}
	v := from.Value(statsInfoKey{})
	if v == nil {
		// not set in from
		return ctx
	}
	ctx = context.WithValue(ctx, statsInfoKey{}, v)
	return ctx
}
