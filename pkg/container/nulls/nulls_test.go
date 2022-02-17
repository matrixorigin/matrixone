package nulls

import (
	"github.com/stretchr/testify/assert"
	"testing"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func TestOr(t *testing.T) {
	t.Run("or test", func(t *testing.T) {
		n := Nulls{Np: roaring.New()}
		for k := 0; k < 4000; k++ {
			n.Np.AddInt(k)
		}
		m := Nulls{Np: roaring.New()}
		for k := 4000; k < 8000; k++ {
			m.Np.AddInt(k)
		}
		result := Nulls{}
		Or(&n, &m, &result)
		assert.Equal(t, n.Np.GetCardinality()+m.Np.GetCardinality(), result.Np.GetCardinality())
		m1 := Nulls{}
		result = Nulls{}
		Or(&n, &m1, &result)
		assert.Equal(t, n.Np.GetCardinality(), result.Np.GetCardinality())
		result = Nulls{}
		Or(&m1, &n, &result)
		assert.Equal(t, n.Np.GetCardinality(), result.Np.GetCardinality())
	})
}

func TestReset(t *testing.T) {
	t.Run("reset test", func(t *testing.T) {
		n := Nulls{Np: roaring.New()}
		for i := 0; i < 2000; i += 7 {
			n.Np.AddInt(i)
		}
		Reset(&n)
		assert.EqualValues(t, 0, n.Np.GetCardinality())
	})
}

func TestAny(t *testing.T) {
	t.Run("Any test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, Any(&n))
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 2000; i += 7 {
			n.Np.AddInt(i)
		}
		assert.EqualValues(t, true, Any(&n))
	})
}

func TestSize(t *testing.T) {
	t.Run("Size test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, 0, Size(&n))
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i++ {
			n.Np.AddInt(i)
		}
		assert.EqualValues(t, 52, Size(&n))
	})
}

func TestLength(t *testing.T) {
	t.Run("Length test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, 0, Length(&n))
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i += 2 {
			n.Np.AddInt(i)
		}
		assert.EqualValues(t, 8, Length(&n))
	})
}

func TestString(t *testing.T) {
	t.Run("String test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, "[]", String(&n))
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i += 2 {
			n.Np.AddInt(i)
		}
		assert.EqualValues(t, "[0 2 4 6 8 10 12 14]", String(&n))
	})
}

func TestContains(t *testing.T) {
	t.Run("Contains test", func(t *testing.T) {
		n := Nulls{}
		assert.EqualValues(t, false, Contains(&n, 2))
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i += 2 {
			n.Np.AddInt(i)
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
		n := Nulls{}
		Add(&n, 2)
		assert.EqualValues(t, true, Contains(&n, 2))
		Del(&n, 2)
		assert.EqualValues(t, false, Contains(&n, 2))
	})
}

func TestSet(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := Nulls{Np: roaring.New()}
		n.Np.AddInt(2)
		m := Nulls{Np: roaring.New()}
		m.Np.AddInt(5)
		Set(&n, &m)
		assert.Equal(t, uint64(2), n.Np.GetCardinality())
	})
}

func TestFilterCount(t *testing.T) {
	t.Run("FilterCount test", func(t *testing.T) {
		n := Nulls{Np: roaring.New()}
		Add(&n, 2, 3, 5, 8)
		assert.Equal(t, 2, FilterCount(&n, []int64{2, 3}))
	})
}

func TestRemoveRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := Nulls{Np: roaring.New()}
		for i := 0; i < 16; i++ {
			n.Np.AddInt(i)
		}
		assert.Equal(t, uint64(16), n.Np.GetCardinality())
		RemoveRange(&n, 10, 16)
		assert.Equal(t, uint64(10), n.Np.GetCardinality())
	})
}

func TestRange(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := Nulls{}
		m := Nulls{}
		Range(&n, 0, 16, &m)
		assert.Equal(t, Nulls{}, m)
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i++ {
			n.Np.AddInt(i)
		}
		Range(&n, 0, 16, &m)
		assert.Equal(t, n, m)
		Range(&n, 10, 16, &m)
		correctM := Nulls{Np: roaring.New()}
		for i := 10; i < 16; i++ {
			correctM.Np.AddInt(i)
		}
		assert.Equal(t, correctM, m)
	})
}

func TestFilter(t *testing.T) {
	t.Run("set test", func(t *testing.T) {
		n := Nulls{}
		n = Nulls{Np: roaring.New()}
		for i := 0; i < 16; i++ {
			n.Np.AddInt(i)
		}
		assert.Equal(t, uint64(16), n.Np.GetCardinality())
		sels := []int64{1, 3, 5}
		Filter(&n, sels)
		assert.Equal(t, uint64(3), n.Np.GetCardinality())
	})
}
