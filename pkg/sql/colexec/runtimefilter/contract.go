// Copyright 2026 Matrix Origin
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

// Package runtimefilter owns the execution-side contract for exact runtime
// filter payloads. Exact-filter producers must validate the complete
// probe/build/payload triangle here before publishing bytes.
package runtimefilter

import (
	"bytes"
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ExactKeyEncoding validates a versioned exact-filter contract against the
// vector an executor actually materialized. Legacy/default metadata and any
// disagreement fail open as Unsupported.
func ExactKeyEncoding(
	spec *plan.RuntimeFilterSpec,
	payloadType types.Type,
) keycodec.ExactRuntimeFilterEncoding {
	return ExactKeyEncodingWithComponents(spec, payloadType, nil)
}

// ExactKeyEncodingWithComponents additionally validates the materialized
// inputs of a versioned tuple encoder. componentPayloadTypes must be in the
// same order as spec.BuildExpr's column arguments. Direct-column contracts
// reject a non-empty component list.
func ExactKeyEncodingWithComponents(
	spec *plan.RuntimeFilterSpec,
	payloadType types.Type,
	componentPayloadTypes []types.Type,
) keycodec.ExactRuntimeFilterEncoding {
	buildExpr := BuildKeyExpr(spec)
	if buildExpr == nil ||
		spec.ProbeType == nil || spec.UseMembershipFilter {
		return keycodec.ExactRuntimeFilterUnsupported
	}

	var advertised keycodec.ExactRuntimeFilterEncoding
	switch spec.KeyEncoding {
	case plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1:
		if buildExpr.GetCol() == nil || buildExpr.GetCol().ColPos < 0 ||
			len(spec.KeyComponentProbeTypes) != 0 ||
			len(componentPayloadTypes) != 0 {
			return keycodec.ExactRuntimeFilterUnsupported
		}
		// prefix_in has a deliberately narrower execution contract than IN:
		// its direct-vector overload consumes VARCHAR only.  Do not let stale
		// metadata reach MakeInExpr, whose legacy fallback ID cannot prove that
		// an overload exists for an arbitrary physical type.
		if spec.MatchPrefix &&
			(types.T(spec.ProbeType.Id) != types.T_varchar ||
				types.T(buildExpr.Typ.Id) != types.T_varchar ||
				payloadType.Oid != types.T_varchar) {
			return keycodec.ExactRuntimeFilterUnsupported
		}
		advertised = keycodec.ExactRuntimeFilterRaw
	case plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1:
		if buildExpr.GetCol() == nil || buildExpr.GetCol().ColPos < 0 ||
			len(spec.KeyComponentProbeTypes) != 0 ||
			len(componentPayloadTypes) != 0 ||
			spec.MatchPrefix {
			return keycodec.ExactRuntimeFilterUnsupported
		}
		advertised = keycodec.ExactRuntimeFilterFloatZeroClosed
	case plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1,
		plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1:
		if types.T(spec.ProbeType.Id) != types.T_varchar ||
			types.T(buildExpr.Typ.Id) != types.T_varchar ||
			payloadType.Oid != types.T_varchar ||
			!validateTupleEncodingComponents(spec, componentPayloadTypes) {
			return keycodec.ExactRuntimeFilterUnsupported
		}
		advertised = keycodec.ExactRuntimeFilterRaw
	default:
		// Legacy plans did not carry the probe half of the SQL equality
		// contract. Runtime filters are optional, so never infer a contract
		// from the build expression alone.
		return keycodec.ExactRuntimeFilterUnsupported
	}

	probeType := types.New(
		types.T(spec.ProbeType.Id),
		spec.ProbeType.Width,
		spec.ProbeType.Scale,
	)
	declaredBuildType := types.New(
		types.T(buildExpr.Typ.Id),
		buildExpr.Typ.Width,
		buildExpr.Typ.Scale,
	)

	// The probe/build edge is the SQL equality contract. The two payload edges
	// defend stale plans and materialization drift. All three must advertise
	// exactly the closure which the producer promises to apply.
	if keycodec.ExactRuntimeFilterEncodingForPair(probeType, declaredBuildType) != advertised ||
		keycodec.ExactRuntimeFilterEncodingForPair(probeType, payloadType) != advertised ||
		keycodec.ExactRuntimeFilterEncodingForPair(declaredBuildType, payloadType) != advertised {
		return keycodec.ExactRuntimeFilterUnsupported
	}
	return advertised
}

// BuildKeyExpr returns the producer expression only for a versioned layout.
// Below the rollout gate, RAW_V1 may also carry an identical legacy Expr so
// older producers retain raw-safe filters. Versioned steady-state and
// transforming encodings keep Expr nil.
func BuildKeyExpr(spec *plan.RuntimeFilterSpec) *plan.Expr {
	if spec == nil || spec.BuildExpr == nil ||
		spec.KeyEncoding ==
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_UNSPECIFIED {
		return nil
	}
	if spec.Expr != nil &&
		(spec.KeyEncoding !=
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1 ||
			!keycodec.LegacyExactRawProducerSafe(
				types.T(spec.BuildExpr.Typ.Id)) ||
			!proto.Equal(spec.Expr, spec.BuildExpr)) {
		return nil
	}
	return spec.BuildExpr
}

func validateTupleEncodingComponents(
	spec *plan.RuntimeFilterSpec,
	componentPayloadTypes []types.Type,
) bool {
	buildExpr := BuildKeyExpr(spec)
	if buildExpr == nil {
		return false
	}
	function := buildExpr.GetF()
	if function == nil || function.Func == nil {
		return false
	}
	expectedFunction := ""
	var expectedFunctionID int64
	switch spec.KeyEncoding {
	case plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1:
		if spec.MatchPrefix {
			return false
		}
		expectedFunction = "serial"
		expectedFunctionID = planfunction.SerialFunctionEncodeID
	case plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1:
		if !spec.MatchPrefix {
			return false
		}
		expectedFunction = "serial_full"
		expectedFunctionID = planfunction.SerialFullFunctionEncodeID
	default:
		return false
	}
	if function.Func.ObjName != expectedFunction ||
		function.Func.Obj != expectedFunctionID ||
		len(function.Args) == 0 ||
		len(function.Args) != len(spec.KeyComponentProbeTypes) ||
		len(function.Args) != len(componentPayloadTypes) {
		return false
	}

	for i, arg := range function.Args {
		if arg == nil || arg.GetCol() == nil ||
			spec.KeyComponentProbeTypes[i].Id == int32(types.T_any) ||
			!planfunction.SerialTypeSupported(types.T(arg.Typ.Id)) ||
			!planfunction.SerialTypeSupported(
				types.T(spec.KeyComponentProbeTypes[i].Id),
			) {
			return false
		}
		probeType := planType(spec.KeyComponentProbeTypes[i])
		declaredBuildType := planType(arg.Typ)
		actualBuildType := componentPayloadTypes[i]
		if keycodec.ExactRuntimeFilterEncodingForPair(
			probeType, declaredBuildType,
		) != keycodec.ExactRuntimeFilterRaw ||
			keycodec.ExactRuntimeFilterEncodingForPair(
				probeType, actualBuildType,
			) != keycodec.ExactRuntimeFilterRaw ||
			keycodec.ExactRuntimeFilterEncodingForPair(
				declaredBuildType, actualBuildType,
			) != keycodec.ExactRuntimeFilterRaw {
			return false
		}
	}
	return true
}

func planType(typ plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

// CloseFloatSignedZero appends the complementary representation when an exact
// float payload contains only one of +0 and -0. beforeAppend lets budgeted
// producers reserve the vector-growth overlap before the allocation occurs.
func CloseFloatSignedZero(
	vec *vector.Vector,
	mp *mpool.MPool,
	beforeAppend func() (release func(), err error),
) error {
	if vec == nil || mp == nil {
		return moerr.NewInternalErrorNoCtx("invalid float runtime-filter vector")
	}
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx(
			"float runtime-filter signed-zero closure requires a flat vector")
	}

	var hasPositiveZero, hasNegativeZero bool
	nulls := vec.GetNulls()
	switch vec.GetType().Oid {
	case types.T_float32:
		for i, value := range vector.MustFixedColNoTypeCheck[float32](vec) {
			if nulls.Contains(uint64(i)) {
				continue
			}
			bits := math.Float32bits(value)
			if bits<<1 != 0 {
				continue
			}
			if bits>>31 == 0 {
				hasPositiveZero = true
			} else {
				hasNegativeZero = true
			}
		}
	case types.T_float64:
		for i, value := range vector.MustFixedColNoTypeCheck[float64](vec) {
			if nulls.Contains(uint64(i)) {
				continue
			}
			bits := math.Float64bits(value)
			if bits<<1 != 0 {
				continue
			}
			if bits>>63 == 0 {
				hasPositiveZero = true
			} else {
				hasNegativeZero = true
			}
		}
	default:
		return moerr.NewInternalErrorNoCtx("non-float runtime filter requested signed-zero closure")
	}
	if hasPositiveZero == hasNegativeZero {
		return nil
	}

	var release func()
	var err error
	if beforeAppend != nil {
		release, err = beforeAppend()
		if err != nil {
			return err
		}
	}
	if release != nil {
		defer release()
	}

	if vec.GetType().Oid == types.T_float32 {
		value := float32(0)
		if hasPositiveZero {
			value = math.Float32frombits(uint32(1) << 31)
		}
		return vector.AppendFixed(vec, value, false, mp)
	}
	value := float64(0)
	if hasPositiveZero {
		value = math.Float64frombits(uint64(1) << 63)
	}
	return vector.AppendFixed(vec, value, false, mp)
}

// MarshalExactFilterVector serializes an exact-filter vector under the
// statement/CN hash-build budget. Runtime-filter payloads live on the Go heap,
// outside mpool accounting, so every producer must retain this reservation
// until the MessageBoard destroys the message.
//
// Exact IN payloads have already discarded NULL. Requiring an empty null
// bitmap makes the wire size exact before allocation and avoids a second,
// unbudgeted roaring-bitmap serialization.
func MarshalExactFilterVector(
	vec *vector.Vector,
	budget *process.HashBuildBudgetGeneration,
) ([]byte, func(), error) {
	if vec == nil || budget == nil || vec.GetNulls().Any() {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}

	length := vec.Length()
	typeSize := vec.GetType().TypeSize()
	if length < 0 || uint64(length) > math.MaxUint32 || typeSize < 0 {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	dataBytes := uint64(typeSize)
	if !vec.IsConst() {
		if typeSize > 0 && uint64(length) > math.MaxUint64/uint64(typeSize) {
			return nil, nil, process.ErrHashBuildBudgetInvalid
		}
		dataBytes *= uint64(length)
	} else if vec.IsConstNull() {
		dataBytes = 0
	}
	areaBytes := uint64(len(vec.GetArea()))
	if dataBytes > math.MaxUint32 || areaBytes > math.MaxUint32 ||
		dataBytes > uint64(len(vec.GetData())) {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}

	// class + encoded type + length/data/area/null lengths + sorted flag.
	headerBytes := uint64(1 + len(types.EncodeType(vec.GetType())) + 4*4 + 1)
	if dataBytes > math.MaxUint64-headerBytes ||
		areaBytes > math.MaxUint64-headerBytes-dataBytes {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	wireBytes := headerBytes + dataBytes + areaBytes
	if wireBytes > uint64(math.MaxInt) {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}

	// bytes.Buffer's visible capacity may be rounded above Grow's request.
	// Reserve a bounded allocator overlap, verify it after allocation, then
	// reconcile to the capacity retained by the message.
	const allocationSlack = uint64(64 << 10)
	if wireBytes > math.MaxUint64-allocationSlack {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	projected := wireBytes + allocationSlack
	token, err := budget.Reserve(projected)
	if err != nil {
		return nil, nil, err
	}

	var buf bytes.Buffer
	buf.Grow(int(wireBytes))
	if uint64(buf.Cap()) > projected {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	if err = vec.MarshalBinaryWithBuffer(&buf); err != nil {
		token.Release()
		return nil, nil, err
	}
	data := buf.Bytes()
	if uint64(len(data)) != wireBytes || uint64(cap(data)) > projected {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	if _, err = token.ReconcileDown(uint64(cap(data))); err != nil {
		token.Release()
		return nil, nil, err
	}
	return data, func() { token.Release() }, nil
}
