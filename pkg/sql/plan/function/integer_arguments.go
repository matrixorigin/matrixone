// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// integerParameter declares an integer evaluation context, not a preference in
// the global numeric conversion lattice. A count does not become an unsigned
// or floating-point contract merely because its source has that type.
type integerParameterMode uint8

const (
	fixedIntegerParameter integerParameterMode = iota
	// Preserve genuine signed/unsigned domains, including binary numeric literals.
	integerBitPatternParameter
	// HEX has distinct string/array behavior; only numeric sources are integers.
	numericOnlyIntegerParameter
)

type integerParameter struct {
	position int
	target   types.T
	// physicalTarget preserves a legacy executor signature after logical
	// parameter coercion. Zero means the executor consumes target directly.
	physicalTarget types.T
	mode           integerParameterMode
	variadic       bool
	// Only roles whose original signatures accepted temporal values opt in.
	temporal bool
	uuid     bool
}

func integerParameterForPosition(name string, position int) (integerParameter, bool) {
	id, ok := getFunctionIdByNameWithoutErr(name)
	if !ok {
		return integerParameter{}, false
	}
	for _, parameter := range allSupportedFunctions[id].integerParameters {
		if parameter.position == position || (parameter.variadic && position >= parameter.position) {
			return parameter, true
		}
	}
	return integerParameter{}, false
}

func (p integerParameter) sourceTarget(source types.T, binaryLiteral bool) (types.T, bool) {
	if p.mode == integerBitPatternParameter && source.IsMySQLString() {
		return types.T_uint64, true
	}
	if p.mode == numericOnlyIntegerParameter && !(source.IsInteger() || source.IsFloat() || source.IsDecimal() || source == types.T_bool || source == types.T_bit || source == types.T_year || source == types.T_enum) {
		return 0, false
	}
	if p.mode != fixedIntegerParameter && (source.IsUnsignedInt() || source == types.T_bit || binaryLiteral) {
		return types.T_uint64, true
	}
	return p.target, true
}

// IntegerArgumentSourceDependent identifies parameters whose real domain must
// be chosen at EXECUTE rather than fixed by a provisional TEXT marker.
func IntegerArgumentUsesBitSources(name string, position int) bool {
	parameter, ok := integerParameterForPosition(name, position)
	return ok && parameter.mode == integerBitPatternParameter
}

func IntegerArgumentSourceDependent(name string, position int) bool {
	parameter, ok := integerParameterForPosition(name, position)
	return ok && (parameter.mode != fixedIntegerParameter || parameter.temporal || parameter.uuid)
}

func IntegerArgumentTargetForSource(name string, position int, source types.T, binaryLiteral bool) (types.T, bool) {
	parameter, ok := integerParameterForPosition(name, position)
	if !ok {
		return 0, false
	}
	return parameter.sourceTarget(source, binaryLiteral)
}

// IntegerArgumentTarget exposes the same parameter contract to AST binding,
// expression rebinding and prepared specialization. Positions are zero-based.
func IntegerArgumentTarget(name string, position int) (types.T, bool) {
	parameter, ok := integerParameterForPosition(name, position)
	if !ok || parameter.mode != fixedIntegerParameter || parameter.temporal || parameter.uuid {
		return 0, false
	}
	return parameter.target, true
}

func IntegerArgumentPhysicalTarget(name string, position int) (types.T, bool) {
	parameter, ok := integerParameterForPosition(name, position)
	if !ok || parameter.physicalTarget == 0 {
		return 0, false
	}
	return parameter.physicalTarget, true
}

func IntegerArgumentUsesExtendedSources(name string, position int) bool {
	parameter, ok := integerParameterForPosition(name, position)
	return ok && (parameter.temporal || parameter.uuid)
}

func IntegerArgumentHasSourceRestrictions(name string, position int) bool {
	p, ok := integerParameterForPosition(name, position)
	return ok && (p.uuid || p.temporal)
}

func IntegerArgumentSourceAllowed(name string, position int, source types.T) bool {
	p, ok := integerParameterForPosition(name, position)
	return ok && p.supportsSource(source)
}

func (p integerParameter) supportsSource(source types.T) bool {
	if source == types.T_uuid {
		return p.uuid
	}
	return integerArgumentSourceSupported(source) || (p.temporal && integerArgumentTemporalSource(source))
}

func integerArgumentTemporalSource(source types.T) bool {
	return source == types.T_date || source == types.T_time || source == types.T_datetime || source == types.T_timestamp
}

func integerArgumentSourceSupported(source types.T) bool {
	return source == types.T_any || source.IsInteger() || source.IsFloat() || source.IsDecimal() ||
		source == types.T_bool || source == types.T_bit || source == types.T_year || source == types.T_enum || source.IsMySQLString()
}

func IsIntegerArgumentCastOverload(id int32) bool {
	return id == IntegerArgumentCastOverload || id == TruncatedIntegerArgumentCastOverload || id == TextIntegerBitsCastOverload || id == TemporalIntegerArgumentCastOverload
}

// Private parameter conversions have their own source contract, independent of
// public CAST support and conversion costs (notably UUID integer-prefix input).
func integerArgumentCastSignature(id int32, args []types.Type) bool {
	if len(args) != 2 {
		return false
	}
	source, target := args[0].Oid, args[1].Oid
	if id == TextIntegerBitsCastOverload {
		return target == types.T_uint64 && (source == types.T_any || source.IsMySQLString())
	}
	if id == TemporalIntegerArgumentCastOverload {
		return target == types.T_int64 && (integerArgumentSourceSupported(source) || integerArgumentTemporalSource(source) || source == types.T_uuid)
	}
	return (target == types.T_int64 || target == types.T_uint64) && integerArgumentSourceSupported(source)
}

func (fn FuncNew) bindsOverload(index int) bool {
	if len(fn.bindingOverloads) == 0 {
		return true
	}
	for _, allowed := range fn.bindingOverloads {
		if index == allowed {
			return true
		}
	}
	return false
}

func (fn FuncNew) checkArgumentTypes(inputs []types.Type, modes []StringDomainCheckMode) checkResult {
	normalized := inputs
	changed := false
	for _, parameter := range fn.integerParameters {
		end := min(parameter.position+1, len(inputs))
		if parameter.variadic {
			end = len(inputs)
		}
		for position := parameter.position; position < end; position++ {
			source := inputs[position]
			if parameter.physicalTarget != 0 && source.Oid == parameter.physicalTarget {
				continue
			}
			target, applies := parameter.sourceTarget(source.Oid, false)
			if !applies {
				continue
			}
			if !parameter.supportsSource(source.Oid) {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if source.Oid == target {
				continue
			}
			if !changed {
				normalized = append([]types.Type(nil), inputs...)
				changed = true
			}
			normalized[position] = target.ToType()
		}
	}
	overloads := fn.Overloads
	if len(fn.bindingOverloads) > 0 {
		overloads = make([]overload, len(fn.bindingOverloads))
		for i, index := range fn.bindingOverloads {
			overloads[i] = fn.Overloads[index]
		}
	}
	check := fn.checkFn(overloads, normalized)
	if fn.stringDomainCheckFn != nil && len(modes) > 0 {
		check = fn.stringDomainCheckFn(overloads, normalized, modes)
	}
	if check.status != succeedMatched && check.status != succeedWithCast {
		return check
	}
	// A checker returns an index into its input table. Only the stable execution
	// table's index may be encoded into a plan; declared overloadId fields are not
	// a reliable substitute (legacy registrations can have duplicate fields).
	if len(fn.bindingOverloads) > 0 {
		check.idx = fn.bindingOverloads[check.idx]
	}
	if check.status == succeedMatched && changed {
		return newCheckResultWithCast(check.idx, normalized)
	}
	return check
}
