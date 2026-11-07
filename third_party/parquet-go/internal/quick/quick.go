package quick

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"
)

var DefaultConfig = Config{
	Sizes: []int{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
		30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
		99, 100, 101,
		127, 128, 129,
		255, 256, 257,
		1000, 1023, 1024, 1025,
		2000, 2095, 2048, 2049,
		4000, 4095, 4096, 4097,
	},
	Seed: 0,
}

// Check is inspired by the standard quick.Check package, but enhances the
// API and tests arrays of larger sizes than the maximum of 50 hardcoded in
// testing/quick.
func Check(f any) error {
	return DefaultConfig.Check(f)
}

type Config struct {
	Sizes []int
	Seed  int64
}

func (c *Config) Check(f any) error {
	v := reflect.ValueOf(f)
	r := rand.New(rand.NewSource(c.Seed))
	t := v.Type().In(0)

	makeValue := MakeValueFuncOf(t.Elem())
	makeArray := func(n int) reflect.Value {
		array := reflect.MakeSlice(t, n, n)
		for i := range n {
			makeValue(array.Index(i), r)
		}
		return array
	}

	if makeArray == nil {
		panic("cannot run quick check on function with input of type " + v.Type().In(0).String())
	}

	for _, n := range c.Sizes {
		for i := range 3 {
			in := makeArray(n)
			ok := v.Call([]reflect.Value{in})
			if !ok[0].Bool() {
				return fmt.Errorf("test #%d: failed on input of size %d: %#v\n", i+1, n, in.Interface())
			}
		}
	}
	return nil

}

type MakeValueFunc func(reflect.Value, *rand.Rand)

func MakeValueFuncOf(t reflect.Type) MakeValueFunc {
	switch t {
	case reflect.TypeOf(time.Time{}):
		return func(v reflect.Value, r *rand.Rand) {
			// TODO: This is a hack to support the matching of times in a precision
			// other than nanosecond by generating times rounded to the second. A
			// better solution would be to update columns types to add a compare
			// function.
			sec := r.Int63n(2524608000) // 2050-01-01
			v.Set(reflect.ValueOf(time.Unix(sec, 0).UTC()))
		}
	}

	switch t.Kind() {
	case reflect.Bool:
		return func(v reflect.Value, r *rand.Rand) {
			v.SetBool((r.Int() % 2) != 0)
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(v reflect.Value, r *rand.Rand) {
			v.SetInt(r.Int63n(math.MaxInt32))
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return func(v reflect.Value, r *rand.Rand) {
			v.SetUint(r.Uint64())
		}

	case reflect.Float32, reflect.Float64:
		return func(v reflect.Value, r *rand.Rand) {
			v.SetFloat(r.Float64())
		}

	case reflect.String:
		return func(v reflect.Value, r *rand.Rand) {
			const characters = "1234567890qwertyuiopasdfghjklzxcvbnm"
			s := new(strings.Builder)
			n := r.Intn(10)
			for i := range n {
				s.WriteByte(characters[i])
			}
			v.SetString(s.String())
		}

	case reflect.Array:
		makeElem := MakeValueFuncOf(t.Elem())
		return func(v reflect.Value, r *rand.Rand) {
			for i, n := 0, v.Len(); i < n; i++ {
				makeElem(v.Index(i), r)
			}
		}

	case reflect.Slice:
		switch e := t.Elem(); e.Kind() {
		case reflect.Uint8:
			return func(v reflect.Value, r *rand.Rand) {
				b := make([]byte, r.Intn(50))
				r.Read(b)
				v.SetBytes(b)
			}
		default:
			makeElem := MakeValueFuncOf(t.Elem())
			return func(v reflect.Value, r *rand.Rand) {
				n := r.Intn(10)
				s := reflect.MakeSlice(t, n, n)
				for i := range n {
					makeElem(s.Index(i), r)
				}
				v.Set(s)
			}
		}

	case reflect.Map:
		makeKey := MakeValueFuncOf(t.Key())
		makeElem := MakeValueFuncOf(t.Elem())
		return func(v reflect.Value, r *rand.Rand) {
			m := reflect.MakeMap(t)
			n := r.Intn(10)
			k := reflect.New(t.Key()).Elem()
			e := reflect.New(t.Elem()).Elem()
			for range n {
				makeKey(k, r)
				makeElem(e, r)
				m.SetMapIndex(k, e)
			}
			v.Set(m)
		}

	case reflect.Struct:
		fields := make([]reflect.StructField, 0, t.NumField())
		makeValues := make([]MakeValueFunc, 0, cap(fields))
		for i, n := 0, cap(fields); i < n; i++ {
			if f := t.Field(i); f.PkgPath == "" { // skip unexported fields
				fields = append(fields, f)
				makeValues = append(makeValues, MakeValueFuncOf(f.Type))
			}
		}
		return func(v reflect.Value, r *rand.Rand) {
			for i := range fields {
				makeValues[i](v.FieldByIndex(fields[i].Index), r)
			}
		}

	case reflect.Ptr:
		t = t.Elem()
		makeValue := MakeValueFuncOf(t)
		return func(v reflect.Value, r *rand.Rand) {
			v.Set(reflect.New(t))
			makeValue(v.Elem(), r)
		}

	default:
		panic("quick.Check does not support test values of type " + t.String())
	}
}
