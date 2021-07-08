package bsi

import (
	"fmt"
	"log"
	"testing"
)

func TestIntBSI(t *testing.T) {
	mp := NewNumericBSI(27, SignedInt)
	{
		xs := []int64{
			10, 3, -7, 9, 0, 1, 9, -8, 2, -1, 12, -35435, 6545654, 2332, 2,
		}
		for i, x := range xs {
			if err := mp.Set(uint64(i), x); err != nil {
				log.Fatal(err)
			}
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mp.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			mq, err := mp.Eq(int64(3), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= 3) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Lt(int64(-1), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< -1) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Le(int64(-1), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= -1) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Gt(int64(-1), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> -1) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Ge(int64(-1), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= -1) -> %v\n", mq.ToArray())
		}
		{
			mq, count := mp.Sum(nil)
			fmt.Printf("\tsum, count = %v, %v\n", mq, count)
		}
		{
			mq := mp.Top(5, nil)
			fmt.Printf("\ttop 5 = %v\n", mq)
		}
	}
	data, err := mp.Marshall()
	if err != nil {
		log.Fatal(err)
	}
	mq := NewNumericBSI(0, SignedInt)
	if err := mq.Unmarshall(data); err != nil {
		log.Fatal(err)
	}
	{
		xs := []int64{
			10, 3, -7, 9, 0, 1, 9, -8, 2, -1, 12, -35435, 6545654, 2332, 2,
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mq.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			m, err := mq.Eq(int64(-35435), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= -35435) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Lt(int64(-7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< -7) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Le(int64(-7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= -7) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Gt(int64(0), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> 0) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Ge(int64(0), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= 0) -> %v\n", m.ToArray())
		}
		{
			m := mq.Bottom(5, nil)
			fmt.Printf("\tbottom 5 = %v\n", m)
		}
	}
}

func TestUIntBSI(t *testing.T) {
	mp := NewNumericBSI(45, UnsignedInt)
	{
		xs := []uint64{
			10, 3, 7, 9, 0, 1, 9, 8, 2, 12, 35435, 6545654, 2332, 2,
		}
		for i, x := range xs {
			if err := mp.Set(uint64(i), x); err != nil {
				log.Fatal(err)
			}
		}
		{
			if err := mp.Del(uint64(9)); err != nil {
				log.Fatal(err)
			}
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mp.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			mq, err := mp.Eq(uint64(3), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= 3) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Lt(uint64(10), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< 10) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Le(uint64(10), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= 10) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Gt(uint64(7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> 7) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Ge(uint64(7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= 7) -> %v\n", mq.ToArray())
		}
		{
			sum, count := mp.Sum(nil)
			fmt.Printf("\tsum, count = %v, %v\n", sum, count)
		}
		{
			mq := mp.Top(5, nil)
			fmt.Printf("\ttop 5 = %v\n", mq)
		}
	}
	data, err := mp.Marshall()
	if err != nil {
		log.Fatal(err)
	}
	mq := NewNumericBSI(0, UnsignedInt)
	if err := mq.Unmarshall(data); err != nil {
		log.Fatal(err)
	}
	{
		xs := []uint64{
			10, 3, 7, 9, 0, 1, 9, 8, 2, 12, 35435, 6545654, 2332, 2,
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mq.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			m, err := mq.Eq(uint64(3), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= 3) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Lt(uint64(10), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< 10) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Le(uint64(10), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= 10) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Gt(uint64(7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> 7) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Ge(uint64(7), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= 7) -> %v\n", m.ToArray())
		}
		{
			m := mq.Bottom(5, nil)
			fmt.Printf("\tbottom 5 = %v\n", m)
		}
	}
}

func TestFloatBsi(t *testing.T) {
	mp := NewNumericBSI(64, Float)
	{
		xs := []float64{
			10.9375, 3.125, -35435.15625, -7.875, 9.0625, 0.3125, 9.5, 1.25, 9.5, -8.75, 2.4375, -1.6875, 12.65625, -35435.15625, 6545654.25, 2332.875, 2.515625,
		}
		for i, x := range xs {
			if err := mp.Set(uint64(i), x); err != nil {
				log.Fatal(err)
			}
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mp.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			mq, err := mp.Eq(float64(-35435.15625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= -35435.15625) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Lt(float64(-1.6875), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< -1.6875) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Le(float64(-1.6875), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= -1.6875) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Gt(float64(12.65625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> 12.65625) -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Ge(float64(12.65625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= 12.65625) -> %v\n", mq.ToArray())
		}
		{
			mq := mp.Top(5, nil)
			fmt.Printf("\ttop 5 = %v\n", mq)
		}
		{
			mq, c := mp.Max(nil)
			fmt.Printf("\tmax = %v %v\n", mq, c)
		}
	}
	data, err := mp.Marshall()
	if err != nil {
		log.Fatal(err)
	}
	mq := NewNumericBSI(0, Float)
	if err := mq.Unmarshall(data); err != nil {
		log.Fatal(err)
	}
	{
		xs := []float64{
			10.9375, 3.125, -35435.15625, -7.875, 9.0625, 0.3125, 9.5, 1.25, 9.5, -8.75, 2.4375, -1.6875, 12.65625, -35435.15625, 6545654.25, 2332.875, 2.515625,
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mq.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = %v, %v\n", i, x, v)
				}
			}
		}
		{
			m, err := mq.Eq(float64(-35435.15625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= -35435.15625) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Lt(float64(-1.6875), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< -1.6875) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Le(float64(-1.6875), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= -1.6875) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Gt(float64(12.65625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> 12.65625) -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Ge(float64(12.65625), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= 12.65625) -> %v\n", m.ToArray())
		}
		{
			m := mq.Bottom(5, nil)
			fmt.Printf("\tbottom 5 = %v\n", m)
		}
		{
			m, c := mq.Min(nil)
			fmt.Printf("\tmin = %v %v\n", m, c)
		}
	}
}

func TestStringBSI(t *testing.T) {
	mp := NewStringBSI(8, 8)
	{
		xs := [][]byte{
			[]byte("asdf"), []byte("a"), []byte("asd"), []byte("as"), []byte("bat"), []byte("basket"), []byte("asd"), []byte("a"),
		}
		for i, x := range xs {
			if err := mp.Set(uint64(i), x); err != nil {
				log.Fatal(err)
			}
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mp.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = \"%v\", \"%v\"\n", i, string(x), string(v.([]byte)))
				}
			}
		}
		{
			mq, err := mp.Eq([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= \"asd\") -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Lt([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< \"asd\") -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Le([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= \"asd\") -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Gt([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> \"asd\") -> %v\n", mq.ToArray())
		}
		{
			mq, err := mp.Ge([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= \"asd\") -> %v\n", mq.ToArray())
		}
		{
			mq := mp.Top(5, nil)
			fmt.Printf("\ttop 5 = %v\n", mq)
		}
		{
			mq, c := mp.Max(nil)
			fmt.Printf("\tmax = \"%v\" %v\n", string(mq.([]byte)), c)
		}
	}
	data, err := mp.Marshall()
	if err != nil {
		log.Fatal(err)
	}
	mq := NewStringBSI(0, 0)
	if err := mq.Unmarshall(data); err != nil {
		log.Fatal(err)
	}
	{
		xs := [][]byte{
			[]byte("asdf"), []byte("a"), []byte("asd"), []byte("as"), []byte("bat"), []byte("basket"), []byte("asd"), []byte("a"),
		}
		{
			fmt.Printf("\tlist\n")
			for i, x := range xs {
				v, ok := mq.Get(uint64(i))
				if ok {
					fmt.Printf("\t\t[%v] = \"%v\", \"%v\"\n", i, string(x), string(v.([]byte)))
				}
			}
		}
		{
			m, err := mq.Eq([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(= \"asd\") -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Lt([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(< \"asd\") -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Le([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(<= \"asd\") -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Gt([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(> \"asd\") -> %v\n", m.ToArray())
		}
		{
			m, err := mq.Ge([]byte("asd"), nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\t(>= \"asd\") -> %v\n", m.ToArray())
		}
		{
			m := mq.Bottom(5, nil)
			fmt.Printf("\tbottom 5 = %v\n", m)
		}
		{
			m, c := mq.Min(nil)
			fmt.Printf("\tmin = \"%v\" %v\n", string(m.([]byte)), c)
		}
	}
}
