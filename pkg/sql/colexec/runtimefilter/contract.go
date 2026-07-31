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
	"context"
	"errors"
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

// optionalAllocationError marks only recoverable allocation failures while
// materializing an optional runtime-filter vector or payload. Contract,
// cancellation, and budget lifecycle errors deliberately remain unmarked.
type optionalAllocationError struct {
	cause error
}

func (e *optionalAllocationError) Error() string { return e.cause.Error() }
func (e *optionalAllocationError) Unwrap() error { return e.cause }

// MarkOptionalAllocationError preserves the allocation error while giving a
// runtime-filter producer a narrow fail-open classification.
func MarkOptionalAllocationError(err error) error {
	if err == nil || IsOptionalAllocationError(err) {
		return err
	}
	return &optionalAllocationError{cause: err}
}

// IsOptionalAllocationError reports whether err came from an explicitly
// marked optional runtime-filter allocation boundary.
func IsOptionalAllocationError(err error) bool {
	var allocationErr *optionalAllocationError
	return errors.As(err, &allocationErr)
}

// OptionalFallbackKind identifies the only failures for which an optional
// runtime-filter producer may publish PASS and continue its primary work.
type OptionalFallbackKind uint8

const (
	OptionalFallbackNone OptionalFallbackKind = iota
	OptionalFallbackBudgetAdmission
	OptionalFallbackAllocation
)

// ClassifyOptionalFallback is deliberately fatal-first. Only a typed capacity
// admission rejection or an error marked at an exact optional allocation
// boundary may fail open. Cancellation, lifecycle, accounting, provider, raw
// sentinel, and invariant errors remain fatal.
func ClassifyOptionalFallback(err error) OptionalFallbackKind {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return OptionalFallbackNone
	}
	// Reject every known fatal branch before accepting an admission branch.
	// This also keeps errors.Join(admission, fatal) fatal regardless of the
	// traversal order chosen by errors.As below.
	if errors.Is(err, process.ErrHashBuildBudgetClosed) ||
		errors.Is(err, process.ErrHashBuildBudgetInvalid) ||
		errors.Is(err, process.ErrHashBuildCeilingMissing) {
		return OptionalFallbackNone
	}

	var budgetErr *process.HashBuildBudgetError
	if errors.As(err, &budgetErr) {
		if budgetErr != nil &&
			budgetErr.Kind == process.HashBuildBudgetErrorAdmission {
			return OptionalFallbackBudgetAdmission
		}
		return OptionalFallbackNone
	}

	// Raw sentinels cannot prove an ordinary capacity rejection and a marker
	// must never override a lifecycle or accounting failure wrapped below it.
	if errors.Is(err, process.ErrHashBuildBudgetAdmission) {
		return OptionalFallbackNone
	}

	if IsOptionalAllocationError(err) {
		return OptionalFallbackAllocation
	}
	return OptionalFallbackNone
}

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
		return MarkOptionalAllocationError(
			vector.AppendFixed(vec, value, false, mp))
	}
	value := float64(0)
	if hasPositiveZero {
		value = math.Float64frombits(uint64(1) << 63)
	}
	return MarkOptionalAllocationError(
		vector.AppendFixed(vec, value, false, mp))
}

// MarshalExactFilterVector serializes an exact-filter vector into physical
// MPool storage owned by the statement allocation account. The returned
// release closure transfers that storage lifetime to the MessageBoard.
func MarshalExactFilterVector(
	vec *vector.Vector,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	site mpool.AllocationSite,
) ([]byte, func(), error) {
	if vec == nil || mp == nil || account == nil || vec.GetNulls().Any() {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	plan, err := vec.PrepareMarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	buf, err := mpool.NewAccountedBuffer(mp, account, owner, site)
	if err != nil {
		return nil, nil, err
	}
	if err = buf.EnsureCapacity(plan.Size()); err != nil {
		buf.Free()
		if mpool.IsRetryableAllocationCapacity(err) {
			err = MarkOptionalAllocationError(err)
		}
		return nil, nil, err
	}
	if err = plan.MarshalTo(buf); err != nil {
		buf.Free()
		return nil, nil, err
	}
	if buf.Len() != plan.Size() {
		buf.Free()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	return buf.Bytes(), buf.Free, nil
}
