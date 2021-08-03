// +build arm64

package min

func init() {
	BoolMin = boolMin
	Int8Min = int8Min
	Int16Min = int16Min
	Int32Min = int32Min
	Int64Min = int64Min
	Uint8Min = uint8Min
	Uint16Min = uint16Min
	Uint32Min = uint32Min
	Uint64Min = uint64Min
	Float32Min = float32Min
	Float64Min = float64Min
	StrMin = strMin

	BoolMinSels = boolMinSels
	Int8MinSels = int8MinSels
	Int16MinSels = int16MinSels
	Int32MinSels = int32MinSels
	Int64MinSels = int64MinSels
	Uint8MinSels = uint8MinSels
	Uint16MinSels = uint16MinSels
	Uint32MinSels = uint32MinSels
	Uint64MinSels = uint64MinSels
	Float32MinSels = float32MinSels
	Float64MinSels = float64MinSels
	StrMinSels = strMinSels
}
