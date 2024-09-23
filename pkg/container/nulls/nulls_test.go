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

	"github.com/stretchr/testify/assert"
)

func TestOr(t *testing.T) {
	t.Run("or test", func(t *testing.T) {
		var m, m1, n, result Nulls
		n.InitWithSize(4000)
		for k := uint64(0); k < 4000; k++ {
			n.Add(k)
		}

		m.InitWithSize(8000)
		for k := uint64(4000); k < 8000; k++ {
			m.Add(k)
		}

		Or(&n, &m, &result)
		assert.Equal(t, n.Count()+m.Count(), result.Count())
		result.Reset()
		Or(&n, &m1, &result)
		assert.Equal(t, n.Count(), result.Count())
		result.Reset()
		Or(&m1, &n, &result)
		assert.Equal(t, n.Count(), result.Count())
	})
}

func TestReset(t *testing.T) {
	t.Run("reset test", func(t *testing.T) {
		var n Nulls
		n.InitWithSize(2000)
		for i := uint64(0); i < 2000; i += 7 {
			n.Add(i)
		}
		n.Reset()
		assert.EqualValues(t, 0, n.Count())
	})
}

func TestAny(t *testing.T) {
	t.Run("Any test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, false, Any(&n))
		n.InitWithSize(2000)
		for i := uint64(0); i < 2000; i += 7 {
			n.Add(i)
		}
		assert.EqualValues(t, true, Any(&n))
	})
}

func TestSize(t *testing.T) {
	t.Run("Size test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, 0, Size(&n))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.EqualValues(t, 8, Size(&n))
	})
}

func TestLength(t *testing.T) {
	t.Run("Length test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, 0, n.Count())
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, 8, n.Count())
	})
}

func TestString(t *testing.T) {
	t.Run("String test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, "[]", String(&n))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
		}
		assert.EqualValues(t, "[0 2 4 6 8 10 12 14]", String(&n))
	})
}

func TestContains(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		var n Nulls
		assert.EqualValues(t, false, Contains(&n, 2))
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i += 2 {
			n.Add(i)
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
		assert.Equal(t, 2, n.Count())
	})
}

func TestFilterCount(t *testing.T) {
	t.Run("FilterCount test", func(t *testing.T) {
		var n Nulls
		Add(&n, 2, 3, 5, 8)
		assert.Equal(t, 2, FilterCount(&n, []int64{2, 3}))
	})
}

func TestRemoveRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		RemoveRange(&n, 10, 16)
		assert.Equal(t, 10, n.Count())
	})
}

func TestRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var m, n, correctM Nulls
		Range(&n, 0, 16, 0, &m)
		n.InitWithSize(16)
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		Range(&n, 0, 16, 0, &m)
		assert.Equal(t, n.Count(), m.Count())
		Range(&n, 10, 16, 0, &m)
		correctM.InitWithSize(16)
		for i := uint64(10); i < 16; i++ {
			correctM.Add(i)
		}
		assert.Equal(t, correctM.Count(), m.Count())
	})
}

func TestFilter(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		Filter(&n, sels, false)
		assert.Equal(t, 3, n.Count())
	})

	t.Run("set test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		sels := []int64{1, 3, 5}
		Filter(&n, sels, true)
		assert.Equal(t, 13, n.Count())
	})
}

func TestMerge(t *testing.T) {
	t.Run("merge test", func(t *testing.T) {
		var n, m Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		for i := uint64(8); i < 24; i++ {
			m.Add(i)
		}
		assert.Equal(t, 16, n.Count())
		assert.Equal(t, 16, m.Count())
		n.Merge(&m)
		assert.Equal(t, 24, n.Count())
	})
}

func TestIsSame(t *testing.T) {
	t.Run("IsSame test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, true, n.IsSame(&n))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		assert.Equal(t, false, n.IsSame(nil))
	})
	t.Run("IsSame test", func(t *testing.T) {
		var n, m Nulls
		for i := uint64(0); i < 16; i++ {
			n.Add(i)
		}
		for i := uint64(0); i < 16; i++ {
			m.Add(i)
		}
		assert.Equal(t, true, n.IsSame(&m))
	})
}
