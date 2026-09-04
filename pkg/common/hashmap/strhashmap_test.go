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

package hashmap

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

type floatHashMapShape struct {
	name        string
	vectorKind  string
	hasNull     bool
	start       int
	count       int
	wantValues  []uint64
	wantZValues []int64
	wantGroups  uint64
}

func mustEncodeHashMapJSON(t *testing.T, text string) []byte {
	t.Helper()
	value, err := types.ParseStringToByteJson(text)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(value)
	require.NoError(t, err)
	return encoded
}

func TestStrHashMapPreviewInsertMatchesPublication(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer func() {
		hashMap.Free()
		require.Zero(t, mp.CurrNB())
	}()

	existing := vector.NewVec(types.T_varchar.ToType())
	input := vector.NewVec(types.T_varchar.ToType())
	defer existing.Free(mp)
	defer input.Free(mp)
	require.NoError(t, vector.AppendBytes(existing, []byte("seven"), false, mp))
	for _, value := range []string{"seven", "eight", "eight", "nine"} {
		require.NoError(t, vector.AppendBytes(input, []byte(value), false, mp))
	}

	iterator := hashMap.NewTransactionalIterator()
	_, _, err = iterator.Insert(0, 1, []*vector.Vector{existing})
	require.NoError(t, err)
	var plan InsertPlan
	err = iterator.PreviewInsert(
		0, input.Length(), []*vector.Vector{input},
		hashMap.GroupCount(), &plan)
	require.NoError(t, err)
	previewValues := append([]uint64(nil), plan.Values()...)
	require.Equal(t, []uint64{1, 2, 2, 3}, previewValues)
	require.Equal(t, []uint8{0, 1, 0, 1}, plan.Inserted())
	require.Equal(t, uint64(2), plan.NewGroups())
	require.Equal(t, uint64(1), hashMap.GroupCount())

	values, _, err := iterator.CommitPreview(&plan)
	require.NoError(t, err)
	require.Equal(t, previewValues, values)
	require.Equal(t, uint64(3), hashMap.GroupCount())

	err = iterator.PreviewInsert(
		0, input.Length(), []*vector.Vector{input},
		hashMap.GroupCount(), &plan)
	require.NoError(t, err)
	require.Zero(t, plan.NewGroups())
	plan.complete = false
	_, _, err = iterator.CommitPreview(&plan)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	err = iterator.PreviewInsert(
		0, input.Length(), []*vector.Vector{input},
		hashMap.GroupCount(), &plan)
	require.NoError(t, err)
	values, _, err = iterator.CommitPreview(&plan)
	require.NoError(t, err)
	require.Equal(t, previewValues, values)
	require.Equal(t, uint64(3), hashMap.GroupCount())
}

func TestStrHashMapPreviewInsertGrowsBeforePublication(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer func() {
		hashMap.Free()
		require.Zero(t, mp.CurrNB())
	}()

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(mp)
	for i := 0; i < UnitLimit; i++ {
		require.NoError(t, vector.AppendBytes(
			input, []byte(fmt.Sprintf("preview-key-%03d", i)), false, mp))
	}
	iterator := hashMap.NewTransactionalIterator()
	var plan InsertPlan
	err = iterator.PreviewInsert(
		0, input.Length(), []*vector.Vector{input}, 0, &plan)
	require.NoError(t, err)
	preview := append([]uint64(nil), plan.Values()...)
	require.Equal(t, uint64(input.Length()), plan.NewGroups())
	expected := make([]uint64, input.Length())
	for i := range expected {
		expected[i] = uint64(i + 1)
	}
	require.Equal(t, expected, append([]uint64(nil), preview...))
	require.Zero(t, hashMap.GroupCount())
	published, _, err := iterator.CommitPreview(&plan)
	require.NoError(t, err)
	require.Equal(t, preview, published)
	require.Equal(t, uint64(input.Length()), hashMap.GroupCount())
}

func TestStrHashMapPreviewInsertNullableGrouping(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(true, mp)
	require.NoError(t, err)
	require.NoError(t, hashMap.SetGroupingAware())
	defer func() {
		hashMap.Free()
		require.Zero(t, mp.CurrNB())
	}()

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(mp)
	require.NoError(t, vector.AppendBytes(input, []byte("x"), false, mp))
	require.NoError(t, vector.AppendBytes(input, nil, true, mp))
	require.NoError(t, vector.AppendBytes(input, []byte("x"), false, mp))
	require.NoError(t, vector.AppendBytes(input, nil, true, mp))
	input.GetGrouping().Add(2)

	iterator := hashMap.NewTransactionalIterator()
	var plan InsertPlan
	err = iterator.PreviewInsert(
		0, input.Length(), []*vector.Vector{input}, 0, &plan)
	require.NoError(t, err)
	preview := append([]uint64(nil), plan.Values()...)
	require.Equal(t, []uint64{1, 2, 3, 2}, append([]uint64(nil), preview...))
	require.Equal(t, []uint8{1, 1, 1, 0}, plan.Inserted())
	require.Equal(t, uint64(3), plan.NewGroups())
	published, _, err := iterator.CommitPreview(&plan)
	require.NoError(t, err)
	require.Equal(t, preview, published)
	require.Equal(t, uint64(3), hashMap.GroupCount())
}

func TestStrHashMapRejectsCompositeJoinNaN(t *testing.T) {
	mp := mpool.MustNewZero()
	m, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, m.SetRejectNaN())
	defer func() {
		m.Free()
		require.Zero(t, mp.CurrNB())
	}()

	floats := vector.NewVec(types.T_float32.ToType())
	discriminators := vector.NewVec(types.T_int32.ToType())
	defer floats.Free(mp)
	defer discriminators.Free(mp)
	for _, value := range []float32{float32(math.NaN()), 7, float32(math.NaN())} {
		require.NoError(t, vector.AppendFixed(floats, value, false, mp))
	}
	require.NoError(t, vector.AppendFixed(discriminators, int32(1), false, mp))
	require.NoError(t, vector.AppendFixed(discriminators, int32(1), false, mp))
	require.NoError(t, vector.AppendFixed(discriminators, int32(0), true, mp))
	keys := []*vector.Vector{floats, discriminators}

	values, zValues, err := m.NewIterator().Insert(0, 3, keys)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 0}, values)
	require.Equal(t, []int64{1, 1, 0}, zValues)

	encoded, err := m.MarshalBinary()
	require.NoError(t, err)
	restored := &StrHashMap{}
	require.NoError(t, restored.UnmarshalBinary(encoded, mp))
	defer restored.Free()
	require.True(t, restored.rejectNaN)

	values, zValues, err = restored.NewIterator().Find(0, 3, keys)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 0}, values)
	require.Equal(t, []int64{1, 1, 0}, zValues)
}

func TestStrHashMapRejectsNaNWhenNullKeysParticipate(t *testing.T) {
	mp := mpool.MustNewZero()
	m, err := NewStrHashMap(true, mp)
	require.NoError(t, err)
	require.NoError(t, m.SetRejectNaN())
	defer func() {
		m.Free()
		require.Zero(t, mp.CurrNB())
	}()

	floats := vector.NewVec(types.T_float64.ToType())
	discriminators := vector.NewVec(types.T_int32.ToType())
	defer floats.Free(mp)
	defer discriminators.Free(mp)
	require.NoError(t, vector.AppendFixed(floats, math.NaN(), false, mp))
	require.NoError(t, vector.AppendFixed(floats, float64(8), false, mp))
	require.NoError(t, vector.AppendFixed(discriminators, int32(0), true, mp))
	require.NoError(t, vector.AppendFixed(discriminators, int32(0), true, mp))
	keys := []*vector.Vector{floats, discriminators}

	values, zValues, err := m.NewIterator().Insert(0, 2, keys)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, values)
	require.Equal(t, []int64{0, 1}, zValues)
	require.Equal(t, uint64(1), m.GroupCount())
}

func TestStrHashMapCanonicalVarlenaSerialization(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	negativeZero64 := math.Copysign(0, -1)
	tests := []struct {
		name  string
		typ   types.Type
		build []byte
		probe []byte
	}{
		{
			name:  "json",
			typ:   types.T_json.ToType(),
			build: mustEncodeHashMapJSON(t, `[1,{"n":2.0}]`),
			probe: mustEncodeHashMapJSON(t, `[1.0,{"n":2}]`),
		},
		{
			name:  "vecf32",
			typ:   types.T_array_float32.ToType(),
			build: types.ArrayToBytes([]float32{1, 0, 3}),
			probe: types.ArrayToBytes([]float32{1, negativeZero, 3}),
		},
		{
			name:  "vecf64",
			typ:   types.T_array_float64.ToType(),
			build: types.ArrayToBytes([]float64{1, 0, 3}),
			probe: types.ArrayToBytes([]float64{1, negativeZero64, 3}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()) }()
			build := vector.NewVec(tt.typ)
			probe := vector.NewVec(tt.typ)
			defer build.Free(mp)
			defer probe.Free(mp)
			require.NoError(t, vector.AppendBytes(build, tt.build, false, mp))
			require.NoError(t, vector.AppendBytes(probe, tt.probe, false, mp))

			m, err := NewStrHashMap(false, mp)
			require.NoError(t, err)
			values, zValues, err := m.NewIterator().Insert(
				0, 1, []*vector.Vector{build},
			)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, values)
			require.Equal(t, []int64{1}, zValues)
			encoded, err := m.MarshalBinary()
			require.NoError(t, err)
			m.Free()

			restored := &StrHashMap{}
			require.NoError(t, restored.UnmarshalBinary(encoded, mp))
			defer restored.Free()
			values, zValues, err = restored.NewIterator().Find(
				0, 1, []*vector.Vector{probe},
			)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, values)
			require.Equal(t, []int64{1}, zValues)
		})
	}
}

func TestStrHashMapCharKeysUsePadSpaceSemantics(t *testing.T) {
	for _, test := range []struct {
		name       string
		typ        types.Type
		wantValues []uint64
		wantGroups uint64
		wantProbe  uint64
	}{
		{
			name:       "char",
			typ:        types.New(types.T_char, 8, 0),
			wantValues: []uint64{1, 1, 2, 3, 3},
			wantGroups: 3,
			wantProbe:  1,
		},
		{
			name:       "varchar control",
			typ:        types.New(types.T_varchar, 8, 0),
			wantValues: []uint64{1, 2, 3, 4, 5},
			wantGroups: 5,
			wantProbe:  0,
		},
	} {
		for _, hasNull := range []bool{false, true} {
			t.Run(test.name+"/has_null="+strconv.FormatBool(hasNull), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()

				build := vector.NewVec(test.typ)
				defer build.Free(mp)
				for _, value := range []string{"MO      ", "MO  ", "NO      ", "        ", ""} {
					require.NoError(t, vector.AppendBytes(build, []byte(value), false, mp))
				}
				probe, err := vector.NewConstBytes(test.typ, []byte("MO"), 3, mp)
				require.NoError(t, err)
				defer probe.Free(mp)

				hashMap, err := NewStrHashMap(hasNull, mp)
				require.NoError(t, err)
				defer hashMap.Free()

				values, zValues, err := hashMap.NewIterator().Insert(
					0, build.Length(), []*vector.Vector{build},
				)
				require.NoError(t, err)
				require.Equal(t, test.wantValues, values)
				require.Equal(t, []int64{1, 1, 1, 1, 1}, zValues)
				require.Equal(t, test.wantGroups, hashMap.GroupCount())
				require.Equal(t, "MO      ", build.GetStringAt(0))
				require.Equal(t, "MO  ", build.GetStringAt(1))
				require.Equal(t, "        ", build.GetStringAt(3))

				values, zValues, err = hashMap.NewIterator().Find(
					0, probe.Length(), []*vector.Vector{probe},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{test.wantProbe, test.wantProbe, test.wantProbe}, values)
				require.Equal(t, []int64{1, 1, 1}, zValues)
			})
		}
	}
}

func TestStrHashMapCanonicalVarlenaVectorShapes(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	negativeZero64 := math.Copysign(0, -1)
	tests := []struct {
		name       string
		typ        types.Type
		build      []byte
		probe      []byte
		buildOther []byte
		probeOther []byte
	}{
		{
			name:       "json",
			typ:        types.T_json.ToType(),
			build:      mustEncodeHashMapJSON(t, `[1,{"n":2.0}]`),
			probe:      mustEncodeHashMapJSON(t, `[1.0,{"n":2}]`),
			buildOther: mustEncodeHashMapJSON(t, `{"build":[10,20,30]}`),
			probeOther: mustEncodeHashMapJSON(t, `{"probe":[40,50,60]}`),
		},
		{
			name:       "vecf32",
			typ:        types.T_array_float32.ToType(),
			build:      types.ArrayToBytes([]float32{1, 2, 3, 0, 5, 6, 7, 8}),
			probe:      types.ArrayToBytes([]float32{1, 2, 3, negativeZero, 5, 6, 7, 8}),
			buildOther: types.ArrayToBytes([]float32{10, 20, 30, 40, 50, 60, 70, 80}),
			probeOther: types.ArrayToBytes([]float32{40, 50, 60, 70, 80, 90, 100, 110}),
		},
		{
			name:       "vecf64",
			typ:        types.T_array_float64.ToType(),
			build:      types.ArrayToBytes([]float64{1, 2, 3, 0}),
			probe:      types.ArrayToBytes([]float64{1, 2, 3, negativeZero64}),
			buildOther: types.ArrayToBytes([]float64{10, 20, 30, 40}),
			probeOther: types.ArrayToBytes([]float64{40, 50, 60, 70}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("flat-null-offset", func(t *testing.T) {
				mp := mpool.MustNewZero()
				build := vector.NewVec(test.typ)
				probe := vector.NewVec(test.typ)
				defer func() {
					build.Free(mp)
					probe.Free(mp)
					require.Zero(t, mp.CurrNB())
				}()
				require.NoError(t, vector.AppendBytes(build, test.buildOther, false, mp))
				require.NoError(t, vector.AppendBytes(build, test.build, false, mp))
				require.NoError(t, vector.AppendBytes(build, test.buildOther, true, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probeOther, false, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probe, false, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probeOther, true, mp))

				m, err := NewStrHashMap(false, mp)
				require.NoError(t, err)
				values, zValues, err := m.NewIterator().Insert(
					1, 2, []*vector.Vector{build},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 0}, values)
				require.Equal(t, []int64{1, 0}, zValues)
				values, zValues, err = m.NewIterator().Find(
					1, 2, []*vector.Vector{probe},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 0}, values)
				require.Equal(t, []int64{1, 0}, zValues)
				values, zValues, err = m.NewIterator().Find(
					0, 1, []*vector.Vector{probe},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{0}, values)
				require.Equal(t, []int64{1}, zValues)
				m.Free()
			})

			t.Run("const", func(t *testing.T) {
				mp := mpool.MustNewZero()
				build, err := vector.NewConstBytes(test.typ, test.build, 2, mp)
				require.NoError(t, err)
				probe, err := vector.NewConstBytes(test.typ, test.probe, 2, mp)
				require.NoError(t, err)
				defer func() {
					build.Free(mp)
					probe.Free(mp)
					require.Zero(t, mp.CurrNB())
				}()

				m, err := NewStrHashMap(false, mp)
				require.NoError(t, err)
				values, zValues, err := m.NewIterator().Insert(
					0, 2, []*vector.Vector{build},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 1}, values)
				require.Equal(t, []int64{1, 1}, zValues)
				require.Equal(t, uint64(1), m.GroupCount())
				values, zValues, err = m.NewIterator().Find(
					0, 2, []*vector.Vector{probe},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 1}, values)
				require.Equal(t, []int64{1, 1}, zValues)
				m.Free()
			})

			t.Run("grouping-aware", func(t *testing.T) {
				mp := mpool.MustNewZero()
				build := vector.NewVec(test.typ)
				probe := vector.NewVec(test.typ)
				defer func() {
					build.Free(mp)
					probe.Free(mp)
					require.Zero(t, mp.CurrNB())
				}()
				require.NoError(t, vector.AppendBytes(build, test.build, false, mp))
				require.NoError(t, vector.AppendBytes(build, test.buildOther, true, mp))
				require.NoError(t, vector.AppendBytes(build, test.buildOther, false, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probe, false, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probeOther, true, mp))
				require.NoError(t, vector.AppendBytes(probe, test.probeOther, false, mp))
				build.GetGrouping().Add(2)
				probe.GetGrouping().Add(2)

				m, err := NewStrHashMap(true, mp)
				require.NoError(t, err)
				require.NoError(t, m.SetGroupingAware())
				values, zValues, err := m.NewIterator().Insert(
					0, 3, []*vector.Vector{build},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 2, 3}, values)
				require.Equal(t, []int64{1, 1, 1}, zValues)
				values, zValues, err = m.NewIterator().Find(
					0, 3, []*vector.Vector{probe},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 2, 3}, values)
				require.Equal(t, []int64{1, 1, 1}, zValues)
				m.Free()
			})

			t.Run("const-null-domain", func(t *testing.T) {
				mp := mpool.MustNewZero()
				keys := vector.NewConstNull(test.typ, 2, mp)
				defer func() {
					keys.Free(mp)
					require.Zero(t, mp.CurrNB())
				}()

				filtered, err := NewStrHashMap(false, mp)
				require.NoError(t, err)
				values, zValues, err := filtered.NewIterator().Insert(
					0, 2, []*vector.Vector{keys},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{0, 0}, values)
				require.Equal(t, []int64{0, 0}, zValues)
				filtered.Free()

				retained, err := NewStrHashMap(true, mp)
				require.NoError(t, err)
				values, zValues, err = retained.NewIterator().Insert(
					0, 2, []*vector.Vector{keys},
				)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 1}, values)
				require.Equal(t, []int64{1, 1}, zValues)
				retained.Free()
			})
		})
	}
}

type floatHashMapVectorFactory func(
	t *testing.T,
	m *mpool.MPool,
	vectorKind string,
) (*vector.Vector, *vector.Vector)

func TestHashMapsFloat64SignedZeroContract(t *testing.T) {
	// Vector representation and NULL policy must not change the signed-zero
	// equality contract. Exercise both the single-key IntHashMap and a
	// composite-key StrHashMap through their public iterator API.
	runFloatHashMapContract(t, makeFloat64HashMapShapeVectors)
}

func TestHashMapsScaledFloat32Contract(t *testing.T) {
	// Scaled FLOAT32 keys must use the same normalization in every hashmap
	// representation and NULL policy.
	runFloatHashMapContract(t, makeFloat32HashMapShapeVectors)
}

func TestIntHashMapFloat32NullableOffset(t *testing.T) {
	for _, hasNull := range []bool{false, true} {
		shape := floatHashMapShape{
			name:        "flat-null-offset-unscaled",
			vectorKind:  "flat-null-offset-unscaled",
			hasNull:     hasNull,
			start:       1,
			count:       2,
			wantValues:  []uint64{1, 0},
			wantZValues: []int64{1, 0},
			wantGroups:  1,
		}
		if hasNull {
			shape.wantValues = []uint64{1, 2}
			shape.wantZValues = []int64{1, 1}
			shape.wantGroups = 2
		}
		t.Run("has-null-"+strconv.FormatBool(hasNull), func(t *testing.T) {
			runFloatHashMapShape(t, false, shape, makeFloat32HashMapShapeVectors)
		})
	}
}

func TestIntHashMapFloat32CompositeFloatLast(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	for _, scaleCase := range []struct {
		name       string
		scale      int32
		buildValue float32
		probeValue float32
	}{
		{name: "unscaled", buildValue: 0, probeValue: negativeZero},
		{name: "scaled", scale: 2, buildValue: 1.234, probeValue: 1.23},
	} {
		t.Run(scaleCase.name, func(t *testing.T) {
			runIntHashMapFloat32CompositeFloatLastCase(
				t,
				scaleCase.scale,
				scaleCase.buildValue,
				scaleCase.probeValue,
			)
		})
	}
}

func runIntHashMapFloat32CompositeFloatLastCase(
	t *testing.T,
	scale int32,
	buildValue, probeValue float32,
) {
	m := mpool.MustNewZero()
	floatType := types.T_float32.ToType()
	floatType.Scale = scale
	buildFloat := vector.NewVec(floatType)
	probeFloat := vector.NewVec(floatType)
	buildDiscriminator := vector.NewVec(types.T_int32.ToType())
	probeDiscriminator := vector.NewVec(types.T_int32.ToType())
	vectors := []*vector.Vector{buildFloat, probeFloat, buildDiscriminator, probeDiscriminator}
	defer func() {
		for _, vec := range vectors {
			vec.Free(m)
		}
		require.Zero(t, m.Stats().NumCurrBytes.Load())
	}()
	require.NoError(t, vector.AppendFixed(buildFloat, buildValue, false, m))
	require.NoError(t, vector.AppendFixed(probeFloat, probeValue, false, m))
	require.NoError(t, vector.AppendFixed(buildDiscriminator, int32(7), false, m))
	require.NoError(t, vector.AppendFixed(probeDiscriminator, int32(7), false, m))

	buildKeys := []*vector.Vector{buildDiscriminator, buildFloat}
	probeKeys := []*vector.Vector{probeDiscriminator, probeFloat}
	hashMap, err := NewIntHashMap(false, m)
	require.NoError(t, err)
	defer hashMap.Free()
	itr := hashMap.NewIterator()
	values, zValues, err := itr.Insert(0, 1, buildKeys)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
	values, zValues, err = itr.Find(0, 1, probeKeys)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
}

func runFloatHashMapContract(t *testing.T, makeVectors floatHashMapVectorFactory) {
	mapKinds := []struct {
		name      string
		composite bool
	}{
		{name: "int"},
		{name: "composite-str", composite: true},
	}
	shapes := []floatHashMapShape{
		{
			name:        "flat",
			vectorKind:  "flat",
			start:       1,
			count:       1,
			wantValues:  []uint64{1},
			wantZValues: []int64{1},
			wantGroups:  1,
		},
		{
			name:        "flat-nullable-map",
			vectorKind:  "flat",
			hasNull:     true,
			start:       1,
			count:       1,
			wantValues:  []uint64{1},
			wantZValues: []int64{1},
			wantGroups:  1,
		},
		{
			name:        "flat-null-filter",
			vectorKind:  "flat-null",
			count:       2,
			wantValues:  []uint64{1, 0},
			wantZValues: []int64{1, 0},
			wantGroups:  1,
		},
		{
			name:        "flat-null-key",
			vectorKind:  "flat-null",
			hasNull:     true,
			count:       2,
			wantValues:  []uint64{1, 2},
			wantZValues: []int64{1, 1},
			wantGroups:  2,
		},
		{
			name:        "const",
			vectorKind:  "const",
			count:       2,
			wantValues:  []uint64{1, 1},
			wantZValues: []int64{1, 1},
			wantGroups:  1,
		},
		{
			name:        "const-nullable-map",
			vectorKind:  "const",
			hasNull:     true,
			count:       2,
			wantValues:  []uint64{1, 1},
			wantZValues: []int64{1, 1},
			wantGroups:  1,
		},
		{
			name:        "const-null-filter",
			vectorKind:  "const-null",
			count:       2,
			wantValues:  []uint64{0, 0},
			wantZValues: []int64{0, 0},
			wantGroups:  0,
		},
		{
			name:        "const-null-key",
			vectorKind:  "const-null",
			hasNull:     true,
			count:       2,
			wantValues:  []uint64{1, 1},
			wantZValues: []int64{1, 1},
			wantGroups:  1,
		},
	}

	for _, mapKind := range mapKinds {
		for _, shape := range shapes {
			t.Run(mapKind.name+"/"+shape.name, func(t *testing.T) {
				runFloatHashMapShape(t, mapKind.composite, shape, makeVectors)
			})
		}
	}
}

func TestStringHashMapCanonicalizesFullyGroupedKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, hashMap.SetGroupingAware())
	defer hashMap.Free()

	build := vector.NewVec(types.T_int32.ToType())
	probe := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(build, int32(111), false, mp))
	require.NoError(t, vector.AppendFixed(probe, int32(222), false, mp))
	build.GetGrouping().Add(0)
	probe.GetGrouping().Add(0)
	defer build.Free(mp)
	defer probe.Free(mp)

	iterator := hashMap.NewIterator()
	values, zValues, err := iterator.Insert(0, 1, []*vector.Vector{build})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
	values, zValues, err = iterator.Find(0, 1, []*vector.Vector{probe})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
}

func TestStringHashIteratorAccountedGrowthCapacityBoundary(t *testing.T) {
	const (
		oldCapacity = 10_240
		required    = oldCapacity + 1
		payloadSize = required - 4
	)
	newCapacity, ok := mpool.GrowCapacity(oldCapacity, required)
	require.True(t, ok)
	exactLimit := uint64(oldCapacity) + uint64(newCapacity)

	for _, testCase := range []struct {
		name      string
		limit     uint64
		wantError bool
	}{
		{name: "exact-old-plus-rounded-new", limit: exactLimit},
		{name: "one-byte-short", limit: exactLimit - 1, wantError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			registry, err := mpool.NewAllocationAccountRegistry(1, 2)
			require.NoError(t, err)
			account, err := registry.Open(testCase.limit)
			require.NoError(t, err)
			allocation, err := NewIteratorAllocation(
				account,
				mpool.AllocationOwnerMin,
				mpool.AllocationSiteMin,
			)
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			hashMap, err := NewStrHashMapWithAllocations(
				false,
				mp,
				nil,
				allocation,
			)
			require.NoError(t, err)
			iterator := hashMap.NewIterator().(*strHashmapIterator)
			iterator.keyBuffer, err = mp.AllocAccounted(
				oldCapacity,
				account,
				mpool.AllocationOwnerMin,
				mpool.AllocationSiteMin,
			)
			require.NoError(t, err)
			iterator.keyBufferMP = mp
			iterator.keyBufferAllocation = allocation
			iterator.keyBuffer = iterator.keyBuffer[:0]
			vec := vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(
				vec,
				make([]byte, payloadSize),
				false,
				mp,
			))

			err = iterator.prepareHashKeys([]*vector.Vector{vec}, 0, 1)
			if testCase.wantError {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Equal(t, oldCapacity, cap(iterator.keyBuffer))
				require.Equal(t, uint64(oldCapacity), account.Snapshot().Used)
			} else {
				require.NoError(t, err)
				require.Equal(t, int(newCapacity), cap(iterator.keyBuffer))
				require.Equal(t, uint64(newCapacity), account.Snapshot().Used)
				require.Equal(t, exactLimit, account.Snapshot().Peak)
			}

			iterator.releaseScratch()
			hashMap.Free()
			vec.Free(mp)
			require.Zero(t, mp.Stats().NumCurrBytes.Load())
			require.Zero(t, account.Seal().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			_, err = registry.Finalize(account)
			require.NoError(t, err)
		})
	}
}

func TestGroupingAwareStringHashMapSeparatesRawSentinelBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, hashMap.SetGroupingAware())
	defer hashMap.Free()

	raw := vector.NewVec(types.T_uint8.ToType())
	require.NoError(t, vector.AppendFixed(raw, uint8(2), false, mp))
	grouping := vector.NewRollupConst(types.T_uint8.ToType(), 1, mp)
	defer raw.Free(mp)
	defer grouping.Free(mp)

	iterator := hashMap.NewIterator()
	values, zValues, err := iterator.Insert(0, 1, []*vector.Vector{raw})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
	values, zValues, err = iterator.Insert(0, 1, []*vector.Vector{grouping})
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, values)
	require.Equal(t, []int64{1}, zValues)
	require.Equal(t, uint64(2), hashMap.GroupCount())
}

func TestGroupingAwareStringHashMapFlatFixedFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, hasNull := range []bool{false, true} {
		hashMap, err := NewStrHashMap(hasNull, mp)
		require.NoError(t, err)
		require.NoError(t, hashMap.SetGroupingAware())

		vec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixedList(
			vec, []int32{7, 11, 13}, nil, mp))
		itr := hashMap.NewIterator().(*strHashmapIterator)
		require.NoError(t, itr.prepareHashKeys(
			[]*vector.Vector{vec}, 1, 2))
		itr.encodeHashKeys([]*vector.Vector{vec}, 1, 2)

		for i, value := range []int32{11, 13} {
			expected := append([]byte{0}, types.EncodeInt32(&value)...)
			require.Equal(t, expected, itr.keys[i][:len(expected)])
			require.Len(t, itr.keys[i], 16)
		}

		vec.Free(mp)
		hashMap.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func TestStringHashMapRejectsShortConstRowMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, hashMap.SetGroupingAware())
	defer hashMap.Free()
	grouping := vector.NewRollupConst(types.T_uint8.ToType(), 1, mp)
	defer grouping.Free(mp)

	_, _, err = hashMap.NewIterator().Insert(
		0, 2, []*vector.Vector{grouping},
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
}

func TestNullableStringHashMapTreatsGroupingRowsAsSentinel(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(true, mp)
	require.NoError(t, err)
	defer hashMap.Free()

	partial := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(
		partial,
		[]int32{7, 7, 9},
		nil,
		mp,
	))
	partial.GetGrouping().Add(0)
	partial.GetGrouping().Add(2)
	partial.GetNulls().Add(2)
	defer partial.Free(mp)

	iterator := hashMap.NewIterator()
	values, zValues, err := iterator.Insert(0, 3, []*vector.Vector{partial})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 1}, values)
	require.Equal(t, []int64{1, 1, 1}, zValues)

	fullGrouping := vector.NewRollupConst(types.T_int32.ToType(), 1, mp)
	defer fullGrouping.Free(mp)
	values, zValues, err = iterator.Insert(0, 1, []*vector.Vector{fullGrouping})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
	require.Equal(t, uint64(2), hashMap.GroupCount())
}

func TestGroupingAwareStringHashMapConstNullUsesRowwiseGrouping(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, hasNull := range []bool{false, true} {
		hashMap, err := NewStrHashMap(hasNull, mp)
		require.NoError(t, err)
		require.NoError(t, hashMap.SetGroupingAware())
		vec := vector.NewConstNull(types.T_int32.ToType(), 2, mp)
		vec.GetGrouping().Add(0)

		values, zValues, err := hashMap.NewIterator().Insert(
			0, 2, []*vector.Vector{vec},
		)
		require.NoError(t, err)
		if hasNull {
			require.Equal(t, []uint64{1, 2}, values)
			require.Equal(t, []int64{1, 1}, zValues)
		} else {
			require.Equal(t, []uint64{1, 0}, values)
			require.Equal(t, []int64{1, 0}, zValues)
		}

		vec.Free(mp)
		hashMap.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func TestStringHashMapProbeGroupingDoesNotMatchRawKey(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	raw := vector.NewVec(types.T_uint8.ToType())
	require.NoError(t, vector.AppendFixed(raw, uint8(2), false, mp))
	grouping := vector.NewRollupConst(types.T_uint8.ToType(), 1, mp)
	defer raw.Free(mp)
	defer grouping.Free(mp)
	iterator := hashMap.NewIterator()
	_, _, err = iterator.Insert(0, 1, []*vector.Vector{raw})
	require.NoError(t, err)
	values, zValues, err := iterator.Find(0, 1, []*vector.Vector{grouping})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, values)
	require.Equal(t, []int64{0}, zValues)
}

func TestGroupingAwareStringHashMapRoundTripRetainsKeyGrammar(t *testing.T) {
	mp := mpool.MustNewZero()
	original, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	require.NoError(t, original.SetGroupingAware())
	raw := vector.NewVec(types.T_uint8.ToType())
	require.NoError(t, vector.AppendFixed(raw, uint8(2), false, mp))
	defer raw.Free(mp)
	_, _, err = original.NewIterator().Insert(0, 1, []*vector.Vector{raw})
	require.NoError(t, err)
	require.ErrorIs(t, original.SetGroupingAware(), mpool.ErrAllocationAccountInvalid)

	encoded, err := original.MarshalBinary()
	require.NoError(t, err)
	original.Free()
	restored := &StrHashMap{}
	require.NoError(t, restored.UnmarshalBinary(encoded, mp))
	values, zValues, err := restored.NewIterator().Find(0, 1, []*vector.Vector{raw})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, values)
	require.Equal(t, []int64{1}, zValues)
	restored.Free()
}

func TestStringHashMapWideVarlenaLengthsDoNotCollide(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()

	large := make([]byte, 1<<16)
	left := vector.NewVec(types.T_binary.ToType())
	right := vector.NewVec(types.T_binary.ToType())
	require.NoError(t, vector.AppendBytesList(left, [][]byte{large, nil}, nil, mp))
	require.NoError(t, vector.AppendBytesList(right, [][]byte{nil, large}, nil, mp))
	defer left.Free(mp)
	defer right.Free(mp)

	values, zValues, err := hashMap.NewIterator().Insert(
		0, 2, []*vector.Vector{left, right},
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, values)
	require.Equal(t, []int64{1, 1}, zValues)
}

func TestStringHashMapConstAndFlatVarlenaUseSameKey(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, buildConst := range []bool{false, true} {
		hashMap, err := NewStrHashMap(true, mp)
		require.NoError(t, err)
		build := vector.NewVec(types.T_varchar.ToType())
		probe := vector.NewVec(types.T_varchar.ToType())
		if buildConst {
			build, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("abc"), 1, mp)
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(probe, []byte("abc"), false, mp))
		} else {
			require.NoError(t, vector.AppendBytes(build, []byte("abc"), false, mp))
			probe, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("abc"), 1, mp)
			require.NoError(t, err)
		}
		iterator := hashMap.NewIterator()
		_, _, err = iterator.Insert(0, 1, []*vector.Vector{build})
		require.NoError(t, err)
		values, zValues, err := iterator.Find(0, 1, []*vector.Vector{probe})
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, values)
		require.Equal(t, []int64{1}, zValues)
		build.Free(mp)
		probe.Free(mp)
		hashMap.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func TestHashMapIteratorsRejectMalformedRowShapes(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, makeIterator := range []func() (HashMap, Iterator){
		func() (HashMap, Iterator) {
			m, err := NewIntHashMap(false, mp)
			require.NoError(t, err)
			return m, m.NewIterator()
		},
		func() (HashMap, Iterator) {
			m, err := NewStrHashMap(false, mp)
			require.NoError(t, err)
			return m, m.NewIterator()
		},
	} {
		m, iterator := makeIterator()
		short := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(short, int32(1), false, mp))
		for _, vecs := range [][]*vector.Vector{
			nil,
			{nil},
			{short},
		} {
			_, _, err := iterator.Insert(0, 2, vecs)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
			_, _, err = iterator.Find(1, 1, vecs)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
		}
		require.Zero(t, m.GroupCount())
		short.Free(mp)
		m.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func TestHashMapIteratorsBroadcastConstVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, makeIterator := range []func() (HashMap, Iterator){
		func() (HashMap, Iterator) {
			m, err := NewIntHashMap(true, mp)
			require.NoError(t, err)
			return m, m.NewIterator()
		},
		func() (HashMap, Iterator) {
			m, err := NewStrHashMap(true, mp)
			require.NoError(t, err)
			return m, m.NewIterator()
		},
	} {
		m, iterator := makeIterator()
		constant, err := vector.NewConstFixed(types.T_int32.ToType(), int32(7), 1, mp)
		require.NoError(t, err)

		values, zValues, err := iterator.Insert(UnitLimit, 2, []*vector.Vector{constant})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 1}, values)
		require.Equal(t, []int64{1, 1}, zValues)
		require.Equal(t, uint64(1), m.GroupCount())

		values, zValues, err = iterator.Find(UnitLimit*2, 2, []*vector.Vector{constant})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 1}, values)
		require.Equal(t, []int64{1, 1}, zValues)

		constantNull := vector.NewConstNull(types.T_int32.ToType(), 1, mp)
		values, zValues, err = iterator.Insert(UnitLimit*3, 2, []*vector.Vector{constantNull})
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 2}, values)
		require.Equal(t, []int64{1, 1}, zValues)
		require.Equal(t, uint64(2), m.GroupCount())

		emptyConstant := vector.NewConstNull(types.T_int32.ToType(), 0, mp)
		_, _, err = iterator.Insert(UnitLimit*4, 1, []*vector.Vector{emptyConstant})
		require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

		emptyConstant.Free(mp)
		constantNull.Free(mp)
		constant.Free(mp)
		m.Free()
	}
	require.Zero(t, mp.CurrNB())
}

func runFloatHashMapShape(
	t *testing.T,
	composite bool,
	shape floatHashMapShape,
	makeVectors floatHashMapVectorFactory,
) {
	m := mpool.MustNewZero()
	var (
		hashMap HashMap
		itr     Iterator
		build   []*vector.Vector
		probe   []*vector.Vector
	)
	if composite {
		mp, err := NewStrHashMap(shape.hasNull, m)
		require.NoError(t, err)
		hashMap = mp
		itr = mp.NewIterator()
	} else {
		mp, err := NewIntHashMap(shape.hasNull, m)
		require.NoError(t, err)
		hashMap = mp
		itr = mp.NewIterator()
	}
	defer func() {
		hashMap.Free()
		for _, vec := range build {
			vec.Free(m)
		}
		for _, vec := range probe {
			vec.Free(m)
		}
		require.Zero(t, m.Stats().NumCurrBytes.Load())
	}()
	buildFloat, probeFloat := makeVectors(t, m, shape.vectorKind)
	build = makeFloatHashMapKeys(t, m, buildFloat, composite)
	probe = makeFloatHashMapKeys(t, m, probeFloat, composite)

	values, zValues, err := itr.Insert(shape.start, shape.count, build)
	require.NoError(t, err)
	require.Equal(t, shape.wantZValues, zValues)
	require.Equal(t, shape.wantValues, liveHashMapValues(values, zValues))
	require.Equal(t, shape.wantGroups, hashMap.GroupCount())

	values, zValues, err = itr.Find(shape.start, shape.count, probe)
	require.NoError(t, err)
	require.Equal(t, shape.wantZValues, zValues)
	require.Equal(t, shape.wantValues, liveHashMapValues(values, zValues))
	require.Equal(t, shape.wantGroups, hashMap.GroupCount())
}

func liveHashMapValues(values []uint64, zValues []int64) []uint64 {
	live := append([]uint64(nil), values...)
	for row := range live {
		// Iterator values are unspecified when zValues marks the row filtered.
		if zValues[row] == 0 {
			live[row] = 0
		}
	}
	return live
}

func makeFloat64HashMapShapeVectors(
	t *testing.T,
	m *mpool.MPool,
	vectorKind string,
) (*vector.Vector, *vector.Vector) {
	floatType := types.T_float64.ToType()
	switch vectorKind {
	case "flat":
		build := vector.NewVec(floatType)
		probe := vector.NewVec(floatType)
		require.NoError(t, vector.AppendFixed(build, float64(123), false, m))
		require.NoError(t, vector.AppendFixed(build, float64(0), false, m))
		require.NoError(t, vector.AppendFixed(probe, float64(456), false, m))
		require.NoError(t, vector.AppendFixed(probe, math.Copysign(0, -1), false, m))
		return build, probe
	case "flat-null":
		build := vector.NewVec(floatType)
		probe := vector.NewVec(floatType)
		require.NoError(t, vector.AppendFixed(build, float64(0), false, m))
		require.NoError(t, vector.AppendFixed(build, float64(99), true, m))
		require.NoError(t, vector.AppendFixed(probe, math.Copysign(0, -1), false, m))
		require.NoError(t, vector.AppendFixed(probe, float64(77), true, m))
		return build, probe
	case "const":
		build, err := vector.NewConstFixed(floatType, float64(0), 2, m)
		require.NoError(t, err)
		probe, err := vector.NewConstFixed(floatType, math.Copysign(0, -1), 2, m)
		require.NoError(t, err)
		return build, probe
	case "const-null":
		return vector.NewConstNull(floatType, 2, m), vector.NewConstNull(floatType, 2, m)
	default:
		t.Fatalf("unknown float64 hashmap vector kind %q", vectorKind)
		return nil, nil
	}
}

func makeFloat32HashMapShapeVectors(
	t *testing.T,
	m *mpool.MPool,
	vectorKind string,
) (*vector.Vector, *vector.Vector) {
	floatType := types.T_float32.ToType()
	floatType.Scale = 2
	switch vectorKind {
	case "flat":
		build := vector.NewVec(floatType)
		probe := vector.NewVec(floatType)
		require.NoError(t, vector.AppendFixed(build, float32(123), false, m))
		require.NoError(t, vector.AppendFixed(build, float32(1.234), false, m))
		require.NoError(t, vector.AppendFixed(probe, float32(456), false, m))
		require.NoError(t, vector.AppendFixed(probe, float32(1.23), false, m))
		return build, probe
	case "flat-null":
		build := vector.NewVec(floatType)
		probe := vector.NewVec(floatType)
		require.NoError(t, vector.AppendFixed(build, float32(1.234), false, m))
		require.NoError(t, vector.AppendFixed(build, float32(99), true, m))
		require.NoError(t, vector.AppendFixed(probe, float32(1.23), false, m))
		require.NoError(t, vector.AppendFixed(probe, float32(77), true, m))
		return build, probe
	case "flat-null-offset-unscaled":
		floatType.Scale = 0
		build := vector.NewVec(floatType)
		probe := vector.NewVec(floatType)
		require.NoError(t, vector.AppendFixed(build, float32(123), false, m))
		require.NoError(t, vector.AppendFixed(build, float32(1.25), false, m))
		require.NoError(t, vector.AppendFixed(build, float32(99), true, m))
		require.NoError(t, vector.AppendFixed(probe, float32(456), false, m))
		require.NoError(t, vector.AppendFixed(probe, float32(1.25), false, m))
		require.NoError(t, vector.AppendFixed(probe, float32(77), true, m))
		return build, probe
	case "const":
		build, err := vector.NewConstFixed(floatType, float32(1.234), 2, m)
		require.NoError(t, err)
		probe, err := vector.NewConstFixed(floatType, float32(1.23), 2, m)
		require.NoError(t, err)
		return build, probe
	case "const-null":
		return vector.NewConstNull(floatType, 2, m), vector.NewConstNull(floatType, 2, m)
	default:
		t.Fatalf("unknown float32 hashmap vector kind %q", vectorKind)
		return nil, nil
	}
}

func makeFloatHashMapKeys(
	t *testing.T,
	m *mpool.MPool,
	floatVec *vector.Vector,
	composite bool,
) []*vector.Vector {
	if !composite {
		return []*vector.Vector{floatVec}
	}

	// FLOAT32 plus INT64 is wider than eight bytes, matching production's
	// composite StrHashMap selection.
	intType := types.T_int64.ToType()
	var discriminator *vector.Vector
	if floatVec.IsConst() {
		var err error
		discriminator, err = vector.NewConstFixed(intType, int64(7), floatVec.Length(), m)
		require.NoError(t, err)
	} else {
		discriminator = vector.NewVec(intType)
		for row := 0; row < floatVec.Length(); row++ {
			require.NoError(t, vector.AppendFixed(discriminator, int64(7), false, m))
		}
	}
	return []*vector.Vector{floatVec, discriminator}
}

func TestInsert(t *testing.T) {
	m := mpool.MustNewZero()
	mp, err := NewStrHashMap(false, m)
	itr := mp.NewIterator()
	require.NoError(t, err)
	ts := []types.Type{
		types.New(types.T_int8, 0, 0),
		types.New(types.T_int16, 0, 0),
		types.New(types.T_int32, 0, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_decimal64, 0, 0),
		types.New(types.T_char, 0, 0),
	}
	vecs := newVectors(ts, false, Rows, m)
	for i := 0; i < Rows; i++ {
		ok, err := itr.DetectDup(vecs, i)
		require.NoError(t, err)
		require.Equal(t, true, ok)
	}
	for _, vec := range vecs {
		vec.Free(m)
	}
	mp.Free()
	require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
}

func TestIterator(t *testing.T) {
	{
		m := mpool.MustNewZero()
		mp, err := NewStrHashMap(false, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0),
			types.New(types.T_int16, 0, 0),
			types.New(types.T_int32, 0, 0),
			types.New(types.T_int64, 0, 0),
			types.New(types.T_decimal64, 0, 0),
			types.New(types.T_char, 0, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewStrHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0),
			types.New(types.T_int16, 0, 0),
			types.New(types.T_int32, 0, 0),
			types.New(types.T_int64, 0, 0),
			types.New(types.T_decimal64, 0, 0),
			types.New(types.T_char, 0, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewStrHashMap(true, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0),
			types.New(types.T_int16, 0, 0),
			types.New(types.T_int32, 0, 0),
			types.New(types.T_int64, 0, 0),
			types.New(types.T_decimal64, 0, 0),
			types.New(types.T_char, 0, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _, err = itr.Find(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
}

func newVectors(ts []types.Type, random bool, n int, m *mpool.MPool) []*vector.Vector {
	vecs := make([]*vector.Vector, len(ts))
	for i := range vecs {
		vecs[i] = newVector(n, ts[i], m, random, nil)
		vecs[i].GetNulls().InitWithSize(n)
	}
	return vecs
}

func newVectorsWithNull(ts []types.Type, random bool, n int, m *mpool.MPool) []*vector.Vector {
	vecs := make([]*vector.Vector, len(ts))
	for i := range vecs {
		vecs[i] = newVector(n, ts[i], m, random, nil)
		nsp := vecs[i].GetNulls()
		nsp.InitWithSize(n)
		for j := 0; j < n; j++ {
			if j%2 == 0 {
				nsp.Set(uint64(j))
			}
		}
	}
	return vecs
}

func newVector(n int, typ types.Type, m *mpool.MPool, random bool, Values interface{}) *vector.Vector {
	switch typ.Oid {
	case types.T_int8:
		if vs, ok := Values.([]int8); ok {
			return newInt8Vector(n, typ, m, random, vs)
		}
		return newInt8Vector(n, typ, m, random, nil)
	case types.T_int16:
		if vs, ok := Values.([]int16); ok {
			return newInt16Vector(n, typ, m, random, vs)
		}
		return newInt16Vector(n, typ, m, random, nil)
	case types.T_int32:
		if vs, ok := Values.([]int32); ok {
			return newInt32Vector(n, typ, m, random, vs)
		}
		return newInt32Vector(n, typ, m, random, nil)
	case types.T_int64:
		if vs, ok := Values.([]int64); ok {
			return newInt64Vector(n, typ, m, random, vs)
		}
		return newInt64Vector(n, typ, m, random, nil)
	case types.T_uint32:
		if vs, ok := Values.([]uint32); ok {
			return newUInt32Vector(n, typ, m, random, vs)
		}
		return newUInt32Vector(n, typ, m, random, nil)
	case types.T_decimal64:
		if vs, ok := Values.([]types.Decimal64); ok {
			return newDecimal64Vector(n, typ, m, random, vs)
		}
		return newDecimal64Vector(n, typ, m, random, nil)
	case types.T_decimal128:
		if vs, ok := Values.([]types.Decimal128); ok {
			return newDecimal128Vector(n, typ, m, random, vs)
		}
		return newDecimal128Vector(n, typ, m, random, nil)
	case types.T_char, types.T_varchar:
		if vs, ok := Values.([]string); ok {
			return newStringVector(n, typ, m, random, vs)
		}
		return newStringVector(n, typ, m, random, nil)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupport vector's type '%v", typ))
	}
}

func newInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int8) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newUInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, uint32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d := types.Decimal64(v)
		if err := vector.AppendFixed(vec, d, false, m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal128Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d := types.Decimal128{B0_63: uint64(v), B64_127: 0}
		if err := vector.AppendFixed(vec, d, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newStringVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendBytes(vec, []byte(vs[i]), false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendBytes(vec, []byte(strconv.Itoa(v)), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func TestStrHashMap_MarshalUnmarshal(t *testing.T) {
	m := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}()

	t.Run("Empty Map", func(t *testing.T) {
		mp, err := NewStrHashMap(false, m)
		require.NoError(t, err)
		defer mp.Free()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)
		size, err := mp.MarshalBinarySize()
		require.NoError(t, err)
		require.Equal(t, int64(len(data)), size)

		unmarshaledMp := &StrHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, uint64(0), unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())
	})

	t.Run("Single Element (No Nulls)", func(t *testing.T) {
		mp, err := NewStrHashMap(false, m)
		require.NoError(t, err)
		defer mp.Free()

		rowCount := 1
		vecs := []*vector.Vector{
			newVector(rowCount, types.T_varchar.ToType(), m, false, []string{"hello"}),
		}
		defer func() {
			for _, vec := range vecs {
				vec.Free(m)
			}
		}()

		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, rowCount, vecs)
		require.NoError(t, err)
		expectedMappedValue := vs
		expectedGroupCount := mp.GroupCount()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)
		size, err := mp.MarshalBinarySize()
		require.NoError(t, err)
		require.Equal(t, int64(len(data)), size)

		unmarshaledMp := &StrHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, _, err := unmarshaledMp.NewIterator().Find(0, rowCount, vecs)
		require.NoError(t, err)
		require.Equal(t, expectedMappedValue, foundVs)
	})

	t.Run("Reject Mismatched Row Count", func(t *testing.T) {
		original, err := NewStrHashMap(false, m)
		require.NoError(t, err)
		defer original.Free()
		vec := newVector(1, types.T_varchar.ToType(), m, false, []string{"hello"})
		defer vec.Free(m)
		_, _, err = original.NewIterator().Insert(0, 1, []*vector.Vector{vec})
		require.NoError(t, err)

		data, err := original.MarshalBinary()
		require.NoError(t, err)
		invalidRows := uint64(2)
		copy(data[1:9], types.EncodeUint64(&invalidRows))
		restored := &StrHashMap{}
		err = restored.UnmarshalBinary(data, m)
		require.ErrorContains(t, err, "does not match cardinality")
		restored.Free()
	})

	t.Run("Multiple Elements (With Resize, With Nulls, Mixed Types)", func(t *testing.T) {
		mp, err := NewStrHashMap(true, m) // Test with nulls enabled
		require.NoError(t, err)
		defer mp.Free()

		numElements := 128
		ts := []types.Type{
			types.New(types.T_int32, 0, 0),
			types.New(types.T_varchar, 50, 0),
		}
		vecs := newVectorsWithNull(ts, true, numElements, m) // Random data with nulls
		defer func() {
			for _, vec := range vecs {
				vec.Free(m)
			}
		}()

		itr := mp.NewIterator()
		originalVs, originalZvs, err := itr.Insert(0, numElements, vecs)
		require.NoError(t, err)
		expectedGroupCount := mp.GroupCount()

		data, err := mp.MarshalBinary()
		require.NoError(t, err)
		size, err := mp.MarshalBinarySize()
		require.NoError(t, err)
		require.Equal(t, int64(len(data)), size)

		unmarshaledMp := &StrHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, foundZvs, err := unmarshaledMp.NewIterator().Find(0, numElements, vecs)
		require.NoError(t, err)
		for i := 0; i < numElements; i++ {
			require.Equal(t, originalVs[i], foundVs[i], "Mismatch at index %d for mapped value", i)
			require.Equal(t, originalZvs[i], foundZvs[i], "Mismatch at index %d for zValue", i)
		}
	})

	t.Run("bad input", func(t *testing.T) {
		var m StrHashMap
		err := m.UnmarshalBinary(nil, nil)
		if err != io.EOF {
			t.Fatal()
		}
		err = m.UnmarshalBinary([]byte{1, 0}, nil)
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("got %v", err)
		}
		err = m.UnmarshalBinary([]byte{1, 1, 2, 3, 4, 5, 6, 7, 8, 0}, nil)
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("got %v", err)
		}
	})

}
