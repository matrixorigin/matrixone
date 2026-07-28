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

package bytejson

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nestedJSONObject(depth int, leaf string) string {
	var builder strings.Builder
	for range depth {
		builder.WriteString(`{"a":`)
	}
	builder.WriteString(leaf)
	for range depth {
		builder.WriteByte('}')
	}
	return builder.String()
}

func TestByteJsonMergePatch(t *testing.T) {
	target, err := ParseFromString(`{"a":1,"nested":{"keep":1,"remove":2}}`)
	require.NoError(t, err)
	patch, err := ParseFromString(`{"b":2,"nested":{"remove":null,"add":3}}`)
	require.NoError(t, err)

	merged, err := target.MergePatch(patch)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1,"b":2,"nested":{"add":3,"keep":1}}`, merged.String())
}

func TestByteJsonMergePreserve(t *testing.T) {
	left, err := ParseFromString(`{"a":{"x":1},"array":[1]}`)
	require.NoError(t, err)
	right, err := ParseFromString(`{"a":{"y":2},"array":[2],"value":null}`)
	require.NoError(t, err)

	merged, err := left.MergePreserve(right)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":{"x":1,"y":2},"array":[1,2],"value":null}`, merged.String())
}

func TestByteJsonMergePreserveAutowrapsScalars(t *testing.T) {
	left, err := ParseFromString(`1`)
	require.NoError(t, err)
	right, err := ParseFromString(`null`)
	require.NoError(t, err)

	merged, err := left.MergePreserve(right)
	require.NoError(t, err)
	require.JSONEq(t, `[1,null]`, merged.String())
}

func TestByteJsonMergePatchMySQLRegressionCases(t *testing.T) {
	tests := []struct {
		target string
		patch  string
		want   string
	}{
		{`{"a":["b"]}`, `{"a":"c"}`, `{"a":"c"}`},
		{`{"a":"c"}`, `{"a":["b"]}`, `{"a":["b"]}`},
		{`{"a":[{"b":"c"}]}`, `{"a":[1]}`, `{"a":[1]}`},
		{`{"a":"foo"}`, `null`, `null`},
		{`[1,2]`, `{"a":"b","c":null}`, `{"a":"b"}`},
	}

	for _, tt := range tests {
		target, err := ParseFromString(tt.target)
		require.NoError(t, err)
		patch, err := ParseFromString(tt.patch)
		require.NoError(t, err)

		merged, err := target.MergePatch(patch)
		require.NoError(t, err)
		require.JSONEq(t, tt.want, merged.String())
	}
}

func TestByteJsonMergePreserveMySQLValueCombinations(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  string
	}{
		{`true`, `[1,2]`, `[true,1,2]`},
		{`[1,2]`, `true`, `[1,2,true]`},
		{`{"a":["x","y"]}`, `{"a":"b","c":"d"}`, `{"a":["x","y","b"],"c":"d"}`},
		{`{"a":"b","c":"d"}`, `{"a":["x","y"]}`, `{"a":["b","x","y"],"c":"d"}`},
		{`{"a":"b","c":"d"}`, `true`, `[{"a":"b","c":"d"},true]`},
	}

	for _, tt := range tests {
		left, err := ParseFromString(tt.left)
		require.NoError(t, err)
		right, err := ParseFromString(tt.right)
		require.NoError(t, err)

		merged, err := left.MergePreserve(right)
		require.NoError(t, err)
		require.JSONEq(t, tt.want, merged.String())
	}
}

func TestByteJsonMergePatchNestingDepth(t *testing.T) {
	target, err := ParseFromString(nestedJSONObject(100, `1`))
	require.NoError(t, err)
	patch, err := ParseFromString(nestedJSONObject(100, `2`))
	require.NoError(t, err)

	_, err = target.MergePatch(patch)
	require.NoError(t, err)

	tooDeep, err := ParseFromString(nestedJSONObject(101, `1`))
	require.NoError(t, err)
	_, err = tooDeep.MergePatch(patch)
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
}

func TestByteJsonMergePreserveNestingDepth(t *testing.T) {
	withinLimit, err := ParseFromString(nestedJSONObject(99, `1`))
	require.NoError(t, err)
	withinLimitOther, err := ParseFromString(nestedJSONObject(99, `2`))
	require.NoError(t, err)

	_, err = withinLimit.MergePreserve(withinLimitOther)
	require.NoError(t, err)

	atLimit, err := ParseFromString(nestedJSONObject(100, `1`))
	require.NoError(t, err)
	atLimitOther, err := ParseFromString(nestedJSONObject(100, `2`))
	require.NoError(t, err)
	_, err = atLimit.MergePreserve(atLimitOther)
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
}

func TestMergeBuilderPatchBuildsOwnedResult(t *testing.T) {
	target, err := ParseFromString(`{"a":1,"nested":{"keep":1,"remove":2}}`)
	require.NoError(t, err)
	patch, err := ParseFromString(`{"b":2,"nested":{"remove":null,"add":3}}`)
	require.NoError(t, err)

	builder := NewMergePatchBuilder()
	require.NoError(t, builder.BeginRow())
	require.NoError(t, builder.Reset(target))
	require.NoError(t, builder.Merge(patch))
	merged, err := builder.BuildOwned()
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1,"b":2,"nested":{"add":3,"keep":1}}`, merged.String())
}

func TestMergeBuilderPreserveRejectsDepthDuringMerge(t *testing.T) {
	left, err := ParseFromString(nestedJSONObject(100, `1`))
	require.NoError(t, err)
	right, err := ParseFromString(nestedJSONObject(100, `2`))
	require.NoError(t, err)

	builder := NewMergePreserveBuilder()
	require.NoError(t, builder.BeginRow())
	require.NoError(t, builder.Reset(left))
	err = builder.Merge(right)
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
	require.Error(t, builder.Finalize())
	require.Error(t, builder.BeginRow())
	builder.Clear()
	require.NoError(t, builder.BeginRow())
}

func TestMergeBuilderMatchesReferenceEncoding(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		mode  mergeMode
	}{
		{name: "patch nested object", left: `{"z":1,"a":{"x":1,"drop":2}}`, right: `{"a":{"drop":null,"y":3}}`, mode: mergeModePatch},
		{name: "patch scalar replacement", left: `{"a":1}`, right: `true`, mode: mergeModePatch},
		{name: "preserve objects", left: `{"a":{"x":1},"v":null}`, right: `{"a":{"y":2},"v":true}`, mode: mergeModePreserve},
		{name: "preserve arrays", left: `[1,true]`, right: `[null,{"a":1}]`, mode: mergeModePreserve},
		{name: "preserve scalar and object", left: `"x"`, right: `{"a":1}`, mode: mergeModePreserve},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left, err := ParseFromString(tt.left)
			require.NoError(t, err)
			right, err := ParseFromString(tt.right)
			require.NoError(t, err)

			var want, got ByteJson
			switch tt.mode {
			case mergeModePatch:
				want, err = referenceMergePatch(left, right)
				require.NoError(t, err)
				got, err = left.MergePatch(right)
			case mergeModePreserve:
				want, err = referenceMergePreserve(left, right)
				require.NoError(t, err)
				got, err = left.MergePreserve(right)
			}
			require.NoError(t, err)
			require.Equal(t, want.Type, got.Type)
			require.True(t, bytes.Equal(want.Data, got.Data),
				"binary mismatch\nwant=%x\n got=%x", want.Data, got.Data)
		})
	}
}

func TestMergeBuilderRandomizedReferenceEncoding(t *testing.T) {
	random := rand.New(rand.NewSource(20260717))
	for i := 0; i < 300; i++ {
		left, err := ParseFromString(randomMergeJSON(random, 0))
		require.NoError(t, err)
		right, err := ParseFromString(randomMergeJSON(random, 0))
		require.NoError(t, err)

		wantPatch, err := referenceMergePatch(left, right)
		require.NoError(t, err)
		gotPatch, err := left.MergePatch(right)
		require.NoError(t, err)
		require.Equal(t, wantPatch.Type, gotPatch.Type, "patch case %d", i)
		require.Equal(t, wantPatch.Data, gotPatch.Data, "patch case %d", i)

		wantPreserve, err := referenceMergePreserve(left, right)
		require.NoError(t, err)
		gotPreserve, err := left.MergePreserve(right)
		require.NoError(t, err)
		require.Equal(t, wantPreserve.Type, gotPreserve.Type, "preserve case %d", i)
		require.Equal(t, wantPreserve.Data, gotPreserve.Data, "preserve case %d", i)
	}
}

func TestMergeBuilderRejectsUnconfiguredUse(t *testing.T) {
	var builder MergeBuilder
	require.ErrorContains(t, builder.BeginRow(), "not configured")
	builder.Clear()
}

func TestMergeDocumentsRejectsUnconfiguredBuilder(t *testing.T) {
	_, err := mergeDocuments(&MergeBuilder{}, Null, Null)
	require.ErrorContains(t, err, "not configured")
}

func TestMergeBuilderLifecycleAndEncodingContract(t *testing.T) {
	document, err := ParseFromString(`{"a":1}`)
	require.NoError(t, err)

	builder := NewMergePatchBuilder()
	require.Zero(t, builder.TypeCode())
	require.Zero(t, builder.DataSize())
	_, err = builder.EncodeDataInto(nil)
	require.ErrorContains(t, err, "not finalized")
	require.ErrorContains(t, builder.ResetUnknown(), "not building")

	require.NoError(t, builder.BeginRow())
	require.NoError(t, builder.ResetUnknown())
	require.ErrorContains(t, builder.Merge(document), "no current value")
	require.ErrorContains(t, builder.Finalize(), "not building")

	builder.Clear()
	require.NoError(t, builder.BeginRow())
	require.NoError(t, builder.Reset(document))
	require.NoError(t, builder.Finalize())
	require.NoError(t, builder.Finalize())
	require.Equal(t, document.Type, builder.TypeCode())
	require.Equal(t, uint32(len(document.Data)), builder.DataSize())

	encoded := make([]byte, builder.DataSize())
	n, err := builder.EncodeDataInto(encoded)
	require.NoError(t, err)
	require.Equal(t, len(encoded), n)
	require.Equal(t, document.Data, encoded)

	_, err = builder.EncodeDataInto(encoded[:len(encoded)-1])
	require.ErrorContains(t, err, "encoded size mismatch")
}

func TestMergeBuilderResetRejectsOverDepthDocument(t *testing.T) {
	document, err := ParseFromString(nestedJSONObject(101, `1`))
	require.NoError(t, err)

	builder := NewMergePatchBuilder()
	require.NoError(t, builder.BeginRow())
	require.ErrorContains(t, builder.Reset(document), "json document nesting depth exceeds 100")
}

func TestValidateJSONMergeDocumentDoesNotAllocate(t *testing.T) {
	document, err := ParseFromString(nestedJSONObject(100, `1`))
	require.NoError(t, err)

	var validateErr error
	allocs := testing.AllocsPerRun(100, func() {
		validateErr = ValidateJSONMergeDocument(document)
	})
	require.NoError(t, validateErr)
	require.Zero(t, allocs)
}

func TestMergeValueDepthAndArrayOverflow(t *testing.T) {
	nested, err := ParseFromString(`{"x":1}`)
	require.NoError(t, err)
	rawArray, err := ParseFromString(`[{"x":1}]`)
	require.NoError(t, err)

	object := mergeValue{
		kind: mergeValueObject,
		object: &mergeObjectPlan{entries: map[string]*mergeObjectEntry{
			"nested": {key: "nested", value: newRawMergeValue(nested)},
		}},
	}
	depth, err := mergeValueDepth(&object)
	require.NoError(t, err)
	require.Equal(t, 2, depth)

	array := mergeValue{
		kind: mergeValueArray,
		array: &mergeArrayPlan{segments: []mergeArraySegment{
			{rawArray: rawArray, isRaw: true},
			{single: newRawMergeValue(Null)},
		}},
	}
	depth, err = mergeValueDepth(&array)
	require.NoError(t, err)
	require.Equal(t, 2, depth)

	invalid := mergeValue{kind: mergeValueInvalid}
	_, err = mergeValueDepth(&invalid)
	require.ErrorContains(t, err, "invalid merge value")
	object.object.entries["invalid"] = &mergeObjectEntry{value: invalid}
	_, err = mergeValueDepth(&object)
	require.ErrorContains(t, err, "invalid merge value")

	full := mergeArrayPlan{elemCount: ^uint32(0)}
	require.ErrorContains(t, full.appendValue(newRawMergeValue(Null), 1), "result is too large")
}

func BenchmarkByteJsonMergeDeepWide(b *testing.B) {
	leaf := `"` + strings.Repeat("x", 256<<10) + `"`
	left, err := ParseFromString(nestedJSONObject(100, leaf))
	require.NoError(b, err)
	right, err := ParseFromString(nestedJSONObject(100, leaf))
	require.NoError(b, err)

	b.Run("patch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := left.MergePatch(right)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("preserve-depth-error", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := left.MergePreserve(right)
			if err == nil {
				b.Fatal("expected depth error")
			}
		}
	})
}

func BenchmarkMergeBuilderManyArguments(b *testing.B) {
	for _, argumentCount := range []int{256, 512, 1024} {
		b.Run(strconv.Itoa(argumentCount), func(b *testing.B) {
			documents := make([]ByteJson, argumentCount)
			for i := range documents {
				key := strconv.Quote("k" + strconv.Itoa(i))
				var err error
				documents[i], err = ParseFromString(`{` + key + `:1}`)
				require.NoError(b, err)
			}
			empty, err := ParseFromString(`{}`)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				builder := NewMergePatchBuilder()
				if err := builder.BeginRow(); err != nil {
					b.Fatal(err)
				}
				if err := builder.Reset(empty); err != nil {
					b.Fatal(err)
				}
				for _, document := range documents {
					if err := builder.Merge(document); err != nil {
						b.Fatal(err)
					}
				}
				if err := builder.Finalize(); err != nil {
					b.Fatal(err)
				}
				builder.Clear()
			}
		})
	}
}

func referenceMergePatch(target, patch ByteJson) (ByteJson, error) {
	if patch.Type != TpCodeObject {
		return ByteJson{Type: patch.Type, Data: append([]byte(nil), patch.Data...)}, nil
	}
	if target.Type != TpCodeObject {
		var err error
		target, err = buildJsonObject(nil, nil)
		if err != nil {
			return Null, err
		}
	}
	keys := make([][]byte, 0, target.GetElemCnt()+patch.GetElemCnt())
	values := make([]ByteJson, 0, target.GetElemCnt()+patch.GetElemCnt())
	for targetIdx, patchIdx := 0, 0; targetIdx < target.GetElemCnt() || patchIdx < patch.GetElemCnt(); {
		switch {
		case patchIdx == patch.GetElemCnt():
			keys = append(keys, target.GetObjectKey(targetIdx))
			values = append(values, target.GetObjectVal(targetIdx))
			targetIdx++
		case targetIdx == target.GetElemCnt():
			patchValue := patch.GetObjectVal(patchIdx)
			if !referenceJSONNull(patchValue) {
				value, err := referenceMergePatch(Null, patchValue)
				if err != nil {
					return Null, err
				}
				keys = append(keys, patch.GetObjectKey(patchIdx))
				values = append(values, value)
			}
			patchIdx++
		default:
			targetKey := target.GetObjectKey(targetIdx)
			patchKey := patch.GetObjectKey(patchIdx)
			switch cmp := bytes.Compare(targetKey, patchKey); {
			case cmp < 0:
				keys = append(keys, targetKey)
				values = append(values, target.GetObjectVal(targetIdx))
				targetIdx++
			case cmp > 0:
				patchValue := patch.GetObjectVal(patchIdx)
				if !referenceJSONNull(patchValue) {
					value, err := referenceMergePatch(Null, patchValue)
					if err != nil {
						return Null, err
					}
					keys = append(keys, patchKey)
					values = append(values, value)
				}
				patchIdx++
			default:
				patchValue := patch.GetObjectVal(patchIdx)
				if !referenceJSONNull(patchValue) {
					value, err := referenceMergePatch(target.GetObjectVal(targetIdx), patchValue)
					if err != nil {
						return Null, err
					}
					keys = append(keys, targetKey)
					values = append(values, value)
				}
				targetIdx++
				patchIdx++
			}
		}
	}
	return buildJsonObject(keys, values)
}

func referenceMergePreserve(left, right ByteJson) (ByteJson, error) {
	if left.Type == TpCodeObject && right.Type == TpCodeObject {
		keys := make([][]byte, 0, left.GetElemCnt()+right.GetElemCnt())
		values := make([]ByteJson, 0, left.GetElemCnt()+right.GetElemCnt())
		for leftIdx, rightIdx := 0, 0; leftIdx < left.GetElemCnt() || rightIdx < right.GetElemCnt(); {
			switch {
			case rightIdx == right.GetElemCnt():
				keys = append(keys, left.GetObjectKey(leftIdx))
				values = append(values, left.GetObjectVal(leftIdx))
				leftIdx++
			case leftIdx == left.GetElemCnt():
				keys = append(keys, right.GetObjectKey(rightIdx))
				values = append(values, right.GetObjectVal(rightIdx))
				rightIdx++
			default:
				leftKey := left.GetObjectKey(leftIdx)
				rightKey := right.GetObjectKey(rightIdx)
				switch cmp := bytes.Compare(leftKey, rightKey); {
				case cmp < 0:
					keys = append(keys, leftKey)
					values = append(values, left.GetObjectVal(leftIdx))
					leftIdx++
				case cmp > 0:
					keys = append(keys, rightKey)
					values = append(values, right.GetObjectVal(rightIdx))
					rightIdx++
				default:
					value, err := referenceMergePreserve(left.GetObjectVal(leftIdx), right.GetObjectVal(rightIdx))
					if err != nil {
						return Null, err
					}
					keys = append(keys, leftKey)
					values = append(values, value)
					leftIdx++
					rightIdx++
				}
			}
		}
		return buildJsonObject(keys, values)
	}

	elems := make([]ByteJson, 0, 2)
	if left.Type == TpCodeArray {
		for i := 0; i < left.GetElemCnt(); i++ {
			elems = append(elems, left.GetArrayElem(i))
		}
	} else {
		elems = append(elems, left)
	}
	if right.Type == TpCodeArray {
		for i := 0; i < right.GetElemCnt(); i++ {
			elems = append(elems, right.GetArrayElem(i))
		}
	} else {
		elems = append(elems, right)
	}
	return buildBinaryJSONArray(elems), nil
}

func referenceJSONNull(value ByteJson) bool {
	return value.Type == TpCodeLiteral && len(value.Data) == 1 && value.Data[0] == LiteralNull
}

func randomMergeJSON(random *rand.Rand, depth int) string {
	if depth >= 3 {
		return []string{`null`, `true`, `false`, `0`, `"x"`}[random.Intn(5)]
	}
	switch random.Intn(7) {
	case 0:
		return `null`
	case 1:
		return []string{`true`, `false`}[random.Intn(2)]
	case 2:
		return strconv.Itoa(random.Intn(11) - 5)
	case 3:
		return strconv.Quote([]string{"", "x", "wide-value"}[random.Intn(3)])
	case 4:
		count := random.Intn(4)
		values := make([]string, count)
		for i := range values {
			values[i] = randomMergeJSON(random, depth+1)
		}
		return `[` + strings.Join(values, `,`) + `]`
	default:
		keys := []string{"a", "b", "c"}
		random.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		count := random.Intn(len(keys) + 1)
		fields := make([]string, count)
		for i := range fields {
			fields[i] = strconv.Quote(keys[i]) + `:` + randomMergeJSON(random, depth+1)
		}
		return `{` + strings.Join(fields, `,`) + `}`
	}
}
