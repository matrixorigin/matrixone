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
	values, zValues = itr.Find(0, 1, probeKeys)
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

	values, zValues = itr.Find(shape.start, shape.count, probe)
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
		vs, _ = itr.Find(0, Rows, vecs)
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
		vs, _ = itr.Find(0, Rows, vecs)
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
		vs, _ = itr.Find(0, Rows, vecs)
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

		unmarshaledMp := &StrHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, _ := unmarshaledMp.NewIterator().Find(0, rowCount, vecs)
		require.Equal(t, expectedMappedValue, foundVs)
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

		unmarshaledMp := &StrHashMap{}
		err = unmarshaledMp.UnmarshalBinary(data, m)
		require.NoError(t, err)
		defer unmarshaledMp.Free()

		require.Equal(t, expectedGroupCount, unmarshaledMp.GroupCount())
		require.Equal(t, mp.HasNull(), unmarshaledMp.HasNull())

		foundVs, foundZvs := unmarshaledMp.NewIterator().Find(0, numElements, vecs)
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
