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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// stringDomainMode resolves the uniform fast path without inspecting a data
// row. A row sidecar is the only reason an executor must branch per row.
func stringDomainMode(vec *vector.Vector) (binary, perRow bool) {
	if vec == nil {
		return false, false
	}
	if vec.HasBinaryStringRows() {
		return false, true
	}
	return types.StaticStringDomain(*vec.GetType()) == types.StringDomainBinary || vec.GetIsBinaryString(), false
}

func binaryStringAt(vec *vector.Vector, row int, uniformBinary, perRow bool) bool {
	if perRow {
		return vec.GetIsBinaryStringAt(row)
	}
	return uniformBinary
}

func stringCharsetAndCollationName(typ types.Type) (charset, collation string) {
	if !typ.Oid.IsMySQLString() || types.StaticStringDomain(typ) == types.StringDomainBinary {
		return "binary", "binary"
	}
	switch typ.Charset {
	case types.CharsetUTF8:
		return "utf8mb4", "utf8mb4_general_ci"
	case types.CharsetUTF8MB4Bin:
		return "utf8mb4", "utf8mb4_bin"
	default:
		// CharsetLegacy and unknown text identities retain the protocol's
		// compatibility fallback rather than inventing a new public identity.
		return "utf8", "utf8_general_ci"
	}
}

func charsetAndCollationTypeMatch(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func runtimeDomainForEffectiveType(resultType types.Type, binary bool) types.RuntimeStringDomain {
	resultIsBinary := types.StaticStringDomain(resultType) == types.StringDomainBinary
	if binary == resultIsBinary {
		return types.RuntimeStringInherit
	}
	if binary {
		return types.RuntimeStringBinary
	}
	return types.RuntimeStringText
}

// setSelectedStringResultDomain propagates only the subject's effective
// domain. The result's statically resolved type remains authoritative for
// protocol and materialization metadata.
func setSelectedStringResultDomain(
	subject *vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
) error {
	resultVec := result.GetResultVector()
	if subject == nil || resultVec == nil || !resultVec.GetType().Oid.IsMySQLString() {
		return nil
	}

	subjectStatic := types.StaticStringDomain(*subject.GetType())
	resultStatic := types.StaticStringDomain(*resultVec.GetType())
	if !subject.HasBinaryStringMetadata() && subjectStatic == resultStatic {
		return nil
	}

	rowCount := resultVec.Length()
	domains := make([]types.RuntimeStringDomain, rowCount)
	for row := 0; row < rowCount; row++ {
		if resultVec.IsNull(uint64(row)) {
			continue
		}
		domains[row] = runtimeDomainForEffectiveType(*resultVec.GetType(), subject.GetIsBinaryStringAt(row))
	}
	return resultVec.SetRuntimeStringDomainsWithMP(domains, proc.Mp())
}

// setContributingStringResultDomain merges the actual non-NULL contributors
// for CONCAT-like functions. Binary wins only among values which contribute
// to a non-NULL output row.
func setContributingStringResultDomain(
	contributors []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
) error {
	resultVec := result.GetResultVector()
	if resultVec == nil || !resultVec.GetType().Oid.IsMySQLString() {
		return nil
	}
	hasDynamicMetadata := false
	for _, contributor := range contributors {
		if contributor != nil && contributor.HasBinaryStringMetadata() {
			hasDynamicMetadata = true
			break
		}
	}
	if !hasDynamicMetadata {
		return nil
	}

	rowCount := resultVec.Length()
	domains := make([]types.RuntimeStringDomain, rowCount)
	for row := 0; row < rowCount; row++ {
		if resultVec.IsNull(uint64(row)) {
			continue
		}
		seen, binary := false, false
		for _, contributor := range contributors {
			if contributor == nil || vectorIsNullAt(contributor, row) {
				continue
			}
			seen = true
			if contributor.GetIsBinaryStringAt(row) {
				binary = true
				break
			}
		}
		if seen {
			domains[row] = runtimeDomainForEffectiveType(*resultVec.GetType(), binary)
		}
	}
	return resultVec.SetRuntimeStringDomainsWithMP(domains, proc.Mp())
}

func setConcatWsStringResultDomain(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
) error {
	resultVec := result.GetResultVector()
	if len(parameters) < 2 || resultVec == nil || !resultVec.GetType().Oid.IsMySQLString() {
		return nil
	}
	resultBinary := types.StaticStringDomain(*resultVec.GetType()) == types.StringDomainBinary
	needsPerRow := false
	for _, parameter := range parameters {
		parameterBinary := types.StaticStringDomain(*parameter.GetType()) == types.StringDomainBinary
		if parameter.HasBinaryStringMetadata() || parameterBinary != resultBinary {
			needsPerRow = true
			break
		}
	}
	if !needsPerRow {
		return nil
	}

	domains := make([]types.RuntimeStringDomain, resultVec.Length())
	for row := 0; row < resultVec.Length(); row++ {
		if resultVec.IsNull(uint64(row)) {
			continue
		}
		valueCount, binary := 0, false
		for _, value := range parameters[1:] {
			if vectorIsNullAt(value, row) {
				continue
			}
			valueCount++
			binary = binary || value.GetIsBinaryStringAt(row)
		}
		if valueCount > 1 && !vectorIsNullAt(parameters[0], row) {
			binary = binary || parameters[0].GetIsBinaryStringAt(row)
		}
		if valueCount > 0 {
			domains[row] = runtimeDomainForEffectiveType(*resultVec.GetType(), binary)
		}
	}
	return resultVec.SetRuntimeStringDomainsWithMP(domains, proc.Mp())
}

func vectorIsNullAt(vec *vector.Vector, row int) bool {
	if vec.IsConst() {
		row = 0
	}
	return row < 0 || row >= vec.Length() || vec.IsNull(uint64(row))
}

func opUnaryBytesToFixedByStringDomain[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	textFn func([]byte) T,
	binaryFn func([]byte) T,
	selectList *FunctionSelectList,
) error {
	uniformBinary, perRow := stringDomainMode(parameters[0])
	if !perRow {
		fn := textFn
		if uniformBinary {
			fn = binaryFn
		}
		return opUnaryBytesToFixed[T](parameters, result, proc, length, fn, selectList)
	}

	param := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[T](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.Append(*new(T), true); err != nil {
				return err
			}
			continue
		}
		value, isNull := param.GetStrValue(row)
		if isNull {
			if err := rs.Append(*new(T), true); err != nil {
				return err
			}
			continue
		}
		fn := textFn
		if parameters[0].GetIsBinaryStringAt(int(row)) {
			fn = binaryFn
		}
		if err := rs.Append(fn(value), false); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryBytesToBytesByStringDomain(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	textFn func([]byte) []byte,
	binaryFn func([]byte) []byte,
	selectList *FunctionSelectList,
) error {
	uniformBinary, perRow := stringDomainMode(parameters[0])
	if !perRow {
		fn := textFn
		if uniformBinary {
			fn = binaryFn
		}
		if err := opUnaryBytesToBytes(parameters, result, proc, length, fn, selectList); err != nil {
			return err
		}
		return setSelectedStringResultDomain(parameters[0], result, proc)
	}

	param := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		value, isNull := param.GetStrValue(row)
		if isNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		fn := textFn
		if parameters[0].GetIsBinaryStringAt(int(row)) {
			fn = binaryFn
		}
		if err := rs.AppendBytes(fn(value), false); err != nil {
			return err
		}
	}
	return setSelectedStringResultDomain(parameters[0], result, proc)
}
