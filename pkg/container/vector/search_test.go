package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestVarlenBinarySearchOffsetByValFactorySmallSet(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	input := []string{"aa", "aa", "ab", "ac", "ac", "ad"}
	for _, v := range input {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	vals := [][]byte{[]byte("aa"), []byte("ac"), []byte("ac"), []byte("az")}
	fn := VarlenBinarySearchOffsetByValFactory(vals)

	require.Equal(t, []int64{0, 1, 3, 4}, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryLargeSet(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	values := make([]string, 0, 400)
	for i := 0; i < 200; i++ {
		s := fmt.Sprintf("%04d", i)
		values = append(values, s, s)
	}
	for _, v := range values {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	var targets [][]byte
	targets = append(targets, []byte("0000"))
	for i := 0; i < 200; i += 3 {
		targets = append(targets, []byte(fmt.Sprintf("%04d", i)))
	}
	targets = append(targets, []byte("0199"), []byte("9999"), []byte("9999"))

	expected := make([]int64, 0, vec.Length())
	targetSet := make(map[string]struct{})
	for _, v := range targets {
		targetSet[string(v)] = struct{}{}
	}
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	fn := VarlenBinarySearchOffsetByValFactory(targets)
	require.Equal(t, expected, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryEmptyInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	fn := VarlenBinarySearchOffsetByValFactory(nil)
	require.Nil(t, fn(vec))

	require.NoError(t, AppendBytes(vec, []byte("a"), false, mp))
	fn = VarlenBinarySearchOffsetByValFactory(nil)
	require.Nil(t, fn(vec))

	fn = VarlenBinarySearchOffsetByValFactory([][]byte{[]byte("b")})
	require.Len(t, fn(vec), 0)
}

func BenchmarkVarlenBinarySearchOffsetByValFactory(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())

	zeroPadded := func(i int) []byte { return []byte(fmt.Sprintf("%05d", i)) }
	for i := 0; i < 8192; i++ {
		require.NoError(b, AppendBytes(vec, zeroPadded(i), false, mp))
	}

	toBytes := func(ss []string) [][]byte {
		res := make([][]byte, len(ss))
		for i := range ss {
			res[i] = []byte(ss[i])
		}
		return res
	}

	cases := []struct {
		name string
		vals [][]byte
	}{
		{
			name: "single_hit",
			vals: [][]byte{zeroPadded(4096)},
		},
		{
			name: "single_miss",
			vals: [][]byte{[]byte("99999")},
		},
		{
			name: "few_hits",
			vals: toBytes([]string{"01000", "02000", "03000"}),
		},
		{
			name: "many_hits",
			vals: func() [][]byte {
				vals := make([][]byte, 0, 4096)
				for i := 0; i < 8192; i += 2 {
					vals = append(vals, zeroPadded(i))
				}
				return vals
			}(),
		},
		{
			name: "all_hits",
			vals: func() [][]byte {
				vals := make([][]byte, 0, 8192)
				for i := 0; i < 8192; i++ {
					vals = append(vals, zeroPadded(i))
				}
				return vals
			}(),
		},
	}

	for _, tc := range cases {
		fn := VarlenBinarySearchOffsetByValFactory(tc.vals)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = fn(vec)
			}
		})
	}

	b.Cleanup(func() {
		vec.Free(mp)
		require.Equal(b, int64(0), mp.CurrNB())
	})
}

func TestOrderedBinarySearchOffsetByValFactoryWithDuplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)

	base := []int64{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]int64, 0, 12*16)
	for i := 0; i < 16; i++ {
		for _, v := range base {
			values = append(values, v+int64(i*1000))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendFixed(vec, v, false, mp))
	}

	vals := []int64{1, 2, 55, 55, 301, 301, 1034, 50000}
	fn := OrderedBinarySearchOffsetByValFactory(vals)

	targetSet := make(map[int64]struct{})
	for _, v := range vals {
		targetSet[v] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}

func TestFixedSizedBinarySearchOffsetByValFactoryWithDuplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)

	base := []int64{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]int64, 0, 12*12)
	for i := 0; i < 12; i++ {
		for _, v := range base {
			values = append(values, v+int64(i*500))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendFixed(vec, v, false, mp))
	}

	vals := []int64{1, 2, 55, 55, 279, 301, 301, 1500, 6000}
	cmp := func(a, b int64) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	}
	fn := FixedSizedBinarySearchOffsetByValFactory(vals, cmp)

	targetSet := make(map[int64]struct{})
	for _, v := range vals {
		targetSet[v] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryWithDuplicateNumbers(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	base := []int{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]string, 0, 12*10)
	for i := 0; i < 10; i++ {
		for _, v := range base {
			values = append(values, fmt.Sprintf("%06d", v+i*700))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	vals := [][]byte{
		[]byte("000001"),
		[]byte("000002"),
		[]byte("000055"),
		[]byte("000055"),
		[]byte("000279"),
		[]byte("000301"),
		[]byte("000301"),
		[]byte("003500"),
		[]byte("010000"),
	}
	fn := VarlenBinarySearchOffsetByValFactory(vals)

	targetSet := make(map[string]struct{})
	for _, v := range vals {
		targetSet[string(v)] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}
