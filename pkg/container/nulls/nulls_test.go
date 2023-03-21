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

package nulls

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/stretchr/testify/assert"
)

func TestOr(t *testing.T) {
	t.Run("or test", func(t *testing.T) {
		n := &Nulls{Np: bitmap.New(4000)}
		for k := uint64(0); k < 4000; k++ {
			n.Np.Add(k)
		}
		m := &Nulls{Np: bitmap.New(8000)}
		for k := uint64(4000); k < 8000; k++ {
			m.Np.Add(k)
		}
		result := &Nulls{}
		Or(n, m, result)
		assert.Equal(t, Length(n)+Length(m), Length(result))
		m1 := &Nulls{}
		result = &Nulls{}
		Or(n, m1, result)
		assert.Equal(t, Length(n), Length(result))
		result = &Nulls{}
		Or(m1, n, result)
		assert.Equal(t, Length(n), Length(result))
	})
}

func TestReset(t *testing.T) {
	t.Run("reset test", func(t *testing.T) {
		n := &Nulls{Np: bitmap.New(2000)}
		for i := uint64(0); i < 2000; i += 7 {
			n.Np.Add(i)
		}
		Reset(n)
		assert.EqualValues(t, 0, Length(n))
	})
}

func TestAny(t *testing.T) {
	t.Run("Any test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, Any(&n))
		n = Nulls{Np: bitmap.New(2000)}
		for i := uint64(0); i < 2000; i += 7 {
			n.Np.Add(i)
		}
		assert.EqualValues(t, true, Any(&n))
	})
}

func TestSize(t *testing.T) {
	t.Run("Size test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, 0, Size(&n))
		n = Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i++ {
			n.Np.Add(i)
		}
	})
}

func TestLength(t *testing.T) {
	t.Run("Length test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, 0, Length(&n))
		n = Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i += 2 {
			n.Np.Add(i)
		}
		assert.EqualValues(t, 8, Length(&n))
	})
}

func TestString(t *testing.T) {
	t.Run("String test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, "[]", String(&n))
		n = Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i += 2 {
			n.Np.Add(i)
		}
		assert.EqualValues(t, "[0 2 4 6 8 10 12 14]", String(&n))
	})
}

func TestContains(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, Contains(&n, 2))
		n = Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i += 2 {
			n.Np.Add(i)
		}
		assert.EqualValues(t, true, Contains(&n, 2))
	})
}

func TestAdd(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, Contains(&n, 2))
		Add(&n, 2)
		assert.EqualValues(t, true, Contains(&n, 2))
	})
}

func TestDel(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := &Nulls{}
		Add(n, 2)
		assert.EqualValues(t, true, Contains(n, 2))
		Del(n, 2)
		assert.EqualValues(t, false, Contains(n, 2))
	})
}

func TestSet(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{}
		Add(n, 2)
		m := &Nulls{}
		Add(m, 5)
		Set(n, m)
		assert.Equal(t, 2, Length(n))
	})
}

func TestFilterCount(t *testing.T) {
	t.Run("FilterCount test", func(t *testing.T) {
		n := Nulls{Np: bitmap.New(9)}
		Add(&n, 2, 3, 5, 8)
		assert.Equal(t, 2, FilterCount(&n, []int64{2, 3}))
	})
}

func TestRemoveRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i++ {
			n.Np.Add(i)
		}
		assert.Equal(t, 16, Length(n))
		RemoveRange(n, 10, 16)
		assert.Equal(t, 10, Length(n))
	})
}

func TestRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{}
		m := &Nulls{}
		Range(n, 0, 16, 0, m)
		n = &Nulls{Np: bitmap.New(16)}
		for i := uint64(0); i < 16; i++ {
			n.Np.Add(i)
		}
		Range(n, 0, 16, 0, m)
		assert.Equal(t, Length(n), Length(m))
		Range(n, 10, 16, 0, m)
		correctM := &Nulls{Np: bitmap.New(16)}
		for i := uint64(10); i < 16; i++ {
			correctM.Np.Add(i)
		}
		assert.Equal(t, Length(correctM), Length(m))
	})
}

func TestFilter(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := &Nulls{}
		for i := uint64(0); i < 16; i++ {
			Add(n, i)
		}
		assert.Equal(t, 16, Length(n))
		sels := []int64{1, 3, 5}
		Filter(n, sels, false)
		assert.Equal(t, 3, Length(n))
	})
}
