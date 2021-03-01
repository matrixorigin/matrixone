package typecast

import "matrixbase/pkg/container/types"

var (
	i8ToI8       func([]int8, []int8) []int8
	i16ToI8      func([]int16, []int8) []int8
	i32ToI8      func([]int32, []int8) []int8
	i64ToI8      func([]int64, []int8) []int8
	u8ToI8       func([]uint8, []int8) []int8
	u16ToI8      func([]uint16, []int8) []int8
	u32ToI8      func([]uint32, []int8) []int8
	u64ToI8      func([]uint64, []int8) []int8
	f32ToI8      func([]float32, []int8) []int8
	f64ToI8      func([]float64, []int8) []int8
	dateToI8     func([]types.Date, []int8) []int8
	decimalToI8  func([]types.Decimal, []int8) []int8
	dateTimeToI8 func([]types.Datetime, []int8) []int8
	bytesToI8    func(*types.Bytes, []int8) []int8

	i8ToI16       func([]int8, []int16) []int16
	i16ToI16      func([]int16, []int16) []int16
	i32ToI16      func([]int32, []int16) []int16
	i64ToI16      func([]int64, []int16) []int16
	u8ToI16       func([]uint8, []int16) []int16
	u16ToI16      func([]uint16, []int16) []int16
	u32ToI16      func([]uint32, []int16) []int16
	u64ToI16      func([]uint64, []int16) []int16
	f32ToI16      func([]float32, []int16) []int16
	f64ToI16      func([]float64, []int16) []int16
	dateToI16     func([]types.Date, []int16) []int16
	decimalToI16  func([]types.Decimal, []int16) []int16
	dateTimeToI16 func([]types.Datetime, []int16) []int16
	bytesToI16    func(*types.Bytes, []int16) []int16

	i8ToI32       func([]int8, []int32) []int32
	i16ToI32      func([]int16, []int32) []int32
	i32ToI32      func([]int32, []int32) []int32
	i64ToI32      func([]int64, []int32) []int32
	u8ToI32       func([]uint8, []int32) []int32
	u16ToI32      func([]uint16, []int32) []int32
	u32ToI32      func([]uint32, []int32) []int32
	u64ToI32      func([]uint64, []int32) []int32
	f32ToI32      func([]float32, []int32) []int32
	f64ToI32      func([]float64, []int32) []int32
	dateToI32     func([]types.Date, []int32) []int32
	decimalToI32  func([]types.Decimal, []int32) []int32
	dateTimeToI32 func([]types.Datetime, []int32) []int32
	bytesToI32    func(*types.Bytes, []int32) []int32

	i8ToI64       func([]int8, []int64) []int64
	i16ToI64      func([]int16, []int64) []int64
	i32ToI64      func([]int32, []int64) []int64
	i64ToI64      func([]int64, []int64) []int64
	u8ToI64       func([]uint8, []int64) []int64
	u16ToI64      func([]uint16, []int64) []int64
	u32ToI64      func([]uint32, []int64) []int64
	u64ToI64      func([]uint64, []int64) []int64
	f32ToI64      func([]float32, []int64) []int64
	f64ToI64      func([]float64, []int64) []int64
	dateToI64     func([]types.Date, []int64) []int64
	decimalToI64  func([]types.Decimal, []int64) []int64
	dateTimeToI64 func([]types.Datetime, []int64) []int64
	bytesToI64    func(*types.Bytes, []int64) []int64

	i8ToU8       func([]int8, []uint8) []uint8
	i16ToU8      func([]int16, []uint8) []uint8
	i32ToU8      func([]int32, []uint8) []uint8
	i64ToU8      func([]int64, []uint8) []uint8
	u8ToU8       func([]uint8, []uint8) []uint8
	u16ToU8      func([]uint16, []uint8) []uint8
	u32ToU8      func([]uint32, []uint8) []uint8
	u64ToU8      func([]uint64, []uint8) []uint8
	f32ToU8      func([]float32, []uint8) []uint8
	f64ToU8      func([]float64, []uint8) []uint8
	dateToU8     func([]types.Date, []uint8) []uint8
	decimalToU8  func([]types.Decimal, []uint8) []uint8
	dateTimeToU8 func([]types.Datetime, []uint8) []uint8
	bytesToU8    func(*types.Bytes, []uint8) []uint8

	i8ToU16       func([]int8, []uint16) []uint16
	i16ToU16      func([]int16, []uint16) []uint16
	i32ToU16      func([]int32, []uint16) []uint16
	i64ToU16      func([]int64, []uint16) []uint16
	u8ToU16       func([]uint8, []uint16) []uint16
	u16ToU16      func([]uint16, []uint16) []uint16
	u32ToU16      func([]uint32, []uint16) []uint16
	u64ToU16      func([]uint64, []uint16) []uint16
	f32ToU16      func([]float32, []uint16) []uint16
	f64ToU16      func([]float64, []uint16) []uint16
	dateToU16     func([]types.Date, []uint16) []uint16
	decimalToU16  func([]types.Decimal, []uint16) []uint16
	dateTimeToU16 func([]types.Datetime, []uint16) []uint16
	bytesToU16    func(*types.Bytes, []uint16) []uint16

	i8ToU32       func([]int8, []uint32) []uint32
	i16ToU32      func([]int16, []uint32) []uint32
	i32ToU32      func([]int32, []uint32) []uint32
	i64ToU32      func([]int64, []uint32) []uint32
	u8ToU32       func([]uint8, []uint32) []uint32
	u16ToU32      func([]uint16, []uint32) []uint32
	u32ToU32      func([]uint32, []uint32) []uint32
	u64ToU32      func([]uint64, []uint32) []uint32
	f32ToU32      func([]float32, []uint32) []uint32
	f64ToU32      func([]float64, []uint32) []uint32
	dateToU32     func([]types.Date, []uint32) []uint32
	decimalToU32  func([]types.Decimal, []uint32) []uint32
	dateTimeToU32 func([]types.Datetime, []uint32) []uint32
	bytesToU32    func(*types.Bytes, []uint32) []uint32

	i8ToU64       func([]int8, []uint64) []uint64
	i16ToU64      func([]int16, []uint64) []uint64
	i32ToU64      func([]int32, []uint64) []uint64
	i64ToU64      func([]int64, []uint64) []uint64
	u8ToU64       func([]uint8, []uint64) []uint64
	u16ToU64      func([]uint16, []uint64) []uint64
	u32ToU64      func([]uint32, []uint64) []uint64
	u64ToU64      func([]uint64, []uint64) []uint64
	f32ToU64      func([]float32, []uint64) []uint64
	f64ToU64      func([]float64, []uint64) []uint64
	dateToU64     func([]types.Date, []uint64) []uint64
	decimalToU64  func([]types.Decimal, []uint64) []uint64
	dateTimeToU64 func([]types.Datetime, []uint64) []uint64
	bytesToU64    func(*types.Bytes, []uint64) []uint64

	i8ToF32       func([]int8, []float32) []float32
	i16ToF32      func([]int16, []float32) []float32
	i32ToF32      func([]int32, []float32) []float32
	i64ToF32      func([]int64, []float32) []float32
	u8ToF32       func([]uint8, []float32) []float32
	u16ToF32      func([]uint16, []float32) []float32
	u32ToF32      func([]uint32, []float32) []float32
	u64ToF32      func([]uint64, []float32) []float32
	f32ToF32      func([]float32, []float32) []float32
	f64ToF32      func([]float64, []float32) []float32
	dateToF32     func([]types.Date, []float32) []float32
	decimalToF32  func([]types.Decimal, []float32) []float32
	dateTimeToF32 func([]types.Datetime, []float32) []float32
	bytesToF32    func(*types.Bytes, []float32) []float32

	i8ToF64       func([]int8, []float64) []float64
	i16ToF64      func([]int16, []float64) []float64
	i32ToF64      func([]int32, []float64) []float64
	i64ToF64      func([]int64, []float64) []float64
	u8ToF64       func([]uint8, []float64) []float64
	u16ToF64      func([]uint16, []float64) []float64
	u32ToF64      func([]uint32, []float64) []float64
	u64ToF64      func([]uint64, []float64) []float64
	f32ToF64      func([]float32, []float64) []float64
	f64ToF64      func([]float64, []float64) []float64
	dateToF64     func([]types.Date, []float64) []float64
	decimalToF64  func([]types.Decimal, []float64) []float64
	dateTimeToF64 func([]types.Datetime, []float64) []float64
	bytesToF64    func(*types.Bytes, []float64) []float64

	i8ToDate       func([]int8, []types.Date) []types.Date
	i16ToDate      func([]int16, []types.Date) []types.Date
	i32ToDate      func([]int32, []types.Date) []types.Date
	i64ToDate      func([]int64, []types.Date) []types.Date
	u8ToDate       func([]uint8, []types.Date) []types.Date
	u16ToDate      func([]uint16, []types.Date) []types.Date
	u32ToDate      func([]uint32, []types.Date) []types.Date
	u64ToDate      func([]uint64, []types.Date) []types.Date
	f32ToDate      func([]float32, []types.Date) []types.Date
	f64ToDate      func([]float64, []types.Date) []types.Date
	dateToDate     func([]types.Date, []types.Date) []types.Date
	decimalToDate  func([]types.Decimal, []types.Date) []types.Date
	dateTimeToDate func([]types.Datetime, []types.Date) []types.Date
	bytesToDate    func(*types.Bytes, []types.Date) []types.Date

	i8ToDatetime       func([]int8, []types.Datetime) []types.Datetime
	i16ToDatetime      func([]int16, []types.Datetime) []types.Datetime
	i32ToDatetime      func([]int32, []types.Datetime) []types.Datetime
	i64ToDatetime      func([]int64, []types.Datetime) []types.Datetime
	u8ToDatetime       func([]uint8, []types.Datetime) []types.Datetime
	u16ToDatetime      func([]uint16, []types.Datetime) []types.Datetime
	u32ToDatetime      func([]uint32, []types.Datetime) []types.Datetime
	u64ToDatetime      func([]uint64, []types.Datetime) []types.Datetime
	f32ToDatetime      func([]float32, []types.Datetime) []types.Datetime
	f64ToDatetime      func([]float64, []types.Datetime) []types.Datetime
	dateToDatetime     func([]types.Date, []types.Datetime) []types.Datetime
	decimalToDatetime  func([]types.Decimal, []types.Datetime) []types.Datetime
	dateTimeToDatetime func([]types.Datetime, []types.Datetime) []types.Datetime
	bytesToDatetime    func(*types.Bytes, []types.Datetime) []types.Datetime

	i8ToBytes       func([]int8, *types.Bytes) *types.Bytes
	i16ToBytes      func([]int16, *types.Bytes) *types.Bytes
	i32ToBytes      func([]int32, *types.Bytes) *types.Bytes
	i64ToBytes      func([]int64, *types.Bytes) *types.Bytes
	u8ToBytes       func([]uint8, *types.Bytes) *types.Bytes
	u16ToBytes      func([]uint16, *types.Bytes) *types.Bytes
	u32ToBytes      func([]uint32, *types.Bytes) *types.Bytes
	u64ToBytes      func([]uint64, *types.Bytes) *types.Bytes
	f32ToBytes      func([]float32, *types.Bytes) *types.Bytes
	f64ToBytes      func([]float64, *types.Bytes) *types.Bytes
	dateToBytes     func([]types.Date, *types.Bytes) *types.Bytes
	decimalToBytes  func([]types.Decimal, *types.Bytes) *types.Bytes
	dateTimeToBytes func([]types.Datetime, *types.Bytes) *types.Bytes
	bytesToBytes    func(*types.Bytes, *types.Bytes) *types.Bytes
)

func I8ToI8(xs []int8, rs []int8) []int8 {
	return rs
}

func I16ToI8(xs []int16, rs []int8) []int8 {
	return rs
}

func I32ToI8(xs []int32, rs []int8) []int8 {
	return rs
}

func I64ToI8(xs []int64, rs []int8) []int8 {
	return rs
}

func U8ToI8(xs []uint8, rs []int8) []int8 {
	return rs
}

func U16ToI8(xs []uint16, rs []int8) []int8 {
	return rs
}

func U32ToI8(xs []uint32, rs []int8) []int8 {
	return rs
}

func U64ToI8(xs []uint64, rs []int8) []int8 {
	return rs
}

func F32ToI8(xs []float32, rs []int8) []int8 {
	return rs
}

func F64ToI8(xs []float64, rs []int8) []int8 {
	return rs
}

func DateToI8(xs []types.Date, rs []int8) []int8 {
	return rs
}

func DatetimeToI8(xs []types.Datetime, rs []int8) []int8 {
	return rs
}

func DecimalToI8(xs []types.Decimal, rs []int8) []int8 {
	return rs
}

func BytesToI8(xs *types.Bytes, rs []int8) []int8 {
	return rs
}

func I8ToI16(xs []int8, rs []int16) []int16 {
	return rs
}

func I16ToI16(xs []int16, rs []int16) []int16 {
	return rs
}

func I32ToI16(xs []int32, rs []int16) []int16 {
	return rs
}

func I64ToI16(xs []int64, rs []int16) []int16 {
	return rs
}

func U8ToI16(xs []uint8, rs []int16) []int16 {
	return rs
}

func U16ToI16(xs []uint16, rs []int16) []int16 {
	return rs
}

func U32ToI16(xs []uint32, rs []int16) []int16 {
	return rs
}

func U64ToI16(xs []uint64, rs []int16) []int16 {
	return rs
}

func F32ToI16(xs []float32, rs []int16) []int16 {
	return rs
}

func F64ToI16(xs []float64, rs []int16) []int16 {
	return rs
}

func DateToI16(xs []types.Date, rs []int16) []int16 {
	return rs
}

func DatetimeToI16(xs []types.Datetime, rs []int16) []int16 {
	return rs
}

func DecimalToI16(xs []types.Decimal, rs []int16) []int16 {
	return rs
}

func BytesToI16(xs *types.Bytes, rs []int16) []int16 {
	return rs
}

func I8ToI32(xs []int8, rs []int32) []int32 {
	return rs
}

func I16ToI32(xs []int16, rs []int32) []int32 {
	return rs
}

func I32ToI32(xs []int32, rs []int32) []int32 {
	return rs
}

func I64ToI32(xs []int64, rs []int32) []int32 {
	return rs
}

func U8ToI32(xs []uint8, rs []int32) []int32 {
	return rs
}

func U16ToI32(xs []uint16, rs []int32) []int32 {
	return rs
}

func U32ToI32(xs []uint32, rs []int32) []int32 {
	return rs
}

func U64ToI32(xs []uint64, rs []int32) []int32 {
	return rs
}

func F32ToI32(xs []float32, rs []int32) []int32 {
	return rs
}

func F64ToI32(xs []float64, rs []int32) []int32 {
	return rs
}

func DateToI32(xs []types.Date, rs []int32) []int32 {
	return rs
}

func DatetimeToI32(xs []types.Datetime, rs []int32) []int32 {
	return rs
}

func DecimalToI32(xs []types.Decimal, rs []int32) []int32 {
	return rs
}

func BytesToI32(xs *types.Bytes, rs []int32) []int32 {
	return rs
}

func I8ToI64(xs []int8, rs []int64) []int64 {
	return rs
}

func I16ToI64(xs []int16, rs []int64) []int64 {
	return rs
}

func I32ToI64(xs []int32, rs []int64) []int64 {
	return rs
}

func I64ToI64(xs []int64, rs []int64) []int64 {
	return rs
}

func U8ToI64(xs []uint8, rs []int64) []int64 {
	return rs
}

func U16ToI64(xs []uint16, rs []int64) []int64 {
	return rs
}

func U32ToI64(xs []uint32, rs []int64) []int64 {
	return rs
}

func U64ToI64(xs []uint64, rs []int64) []int64 {
	return rs
}

func F32ToI64(xs []float32, rs []int64) []int64 {
	return rs
}

func F64ToI64(xs []float64, rs []int64) []int64 {
	return rs
}

func DateToI64(xs []types.Date, rs []int64) []int64 {
	return rs
}

func DatetimeToI64(xs []types.Datetime, rs []int64) []int64 {
	return rs
}

func DecimalToI64(xs []types.Decimal, rs []int64) []int64 {
	return rs
}

func BytesToI64(xs *types.Bytes, rs []int64) []int64 {
	return rs
}

func I8ToU8(xs []int8, rs []uint8) []uint8 {
	return rs
}

func I16ToU8(xs []int16, rs []uint8) []uint8 {
	return rs
}

func I32ToU8(xs []int32, rs []uint8) []uint8 {
	return rs
}

func I64ToU8(xs []int64, rs []uint8) []uint8 {
	return rs
}

func U8ToU8(xs []uint8, rs []uint8) []uint8 {
	return rs
}

func U16ToU8(xs []uint16, rs []uint8) []uint8 {
	return rs
}

func U32ToU8(xs []uint32, rs []uint8) []uint8 {
	return rs
}

func U64ToU8(xs []uint64, rs []uint8) []uint8 {
	return rs
}

func F32ToU8(xs []float32, rs []uint8) []uint8 {
	return rs
}

func F64ToU8(xs []float64, rs []uint8) []uint8 {
	return rs
}

func DateToU8(xs []types.Date, rs []uint8) []uint8 {
	return rs
}

func DatetimeToU8(xs []types.Datetime, rs []uint8) []uint8 {
	return rs
}

func DecimalToU8(xs []types.Decimal, rs []uint8) []uint8 {
	return rs
}

func BytesToU8(xs *types.Bytes, rs []uint8) []uint8 {
	return rs
}

func I8ToU16(xs []int8, rs []uint16) []uint16 {
	return rs
}

func I16ToU16(xs []int16, rs []uint16) []uint16 {
	return rs
}

func I32ToU16(xs []int32, rs []uint16) []uint16 {
	return rs
}

func I64ToU16(xs []int64, rs []uint16) []uint16 {
	return rs
}

func U8ToU16(xs []uint8, rs []uint16) []uint16 {
	return rs
}

func U16ToU16(xs []uint16, rs []uint16) []uint16 {
	return rs
}

func U32ToU16(xs []uint32, rs []uint16) []uint16 {
	return rs
}

func U64ToU16(xs []uint64, rs []uint16) []uint16 {
	return rs
}

func F32ToU16(xs []float32, rs []uint16) []uint16 {
	return rs
}

func F64ToU16(xs []float64, rs []uint16) []uint16 {
	return rs
}

func DateToU16(xs []types.Date, rs []uint16) []uint16 {
	return rs
}

func DatetimeToU16(xs []types.Datetime, rs []uint16) []uint16 {
	return rs
}

func DecimalToU16(xs []types.Decimal, rs []uint16) []uint16 {
	return rs
}

func BytesToU16(xs *types.Bytes, rs []uint16) []uint16 {
	return rs
}

func I8ToU32(xs []int8, rs []uint32) []uint32 {
	return rs
}

func I16ToU32(xs []int16, rs []uint32) []uint32 {
	return rs
}

func I32ToU32(xs []int32, rs []uint32) []uint32 {
	return rs
}

func I64ToU32(xs []int64, rs []uint32) []uint32 {
	return rs
}

func U8ToU32(xs []uint8, rs []uint32) []uint32 {
	return rs
}

func U16ToU32(xs []uint16, rs []uint32) []uint32 {
	return rs
}

func U32ToU32(xs []uint32, rs []uint32) []uint32 {
	return rs
}

func U64ToU32(xs []uint64, rs []uint32) []uint32 {
	return rs
}

func F32ToU32(xs []float32, rs []uint32) []uint32 {
	return rs
}

func F64ToU32(xs []float64, rs []uint32) []uint32 {
	return rs
}

func DateToU32(xs []types.Date, rs []uint32) []uint32 {
	return rs
}

func DatetimeToU32(xs []types.Datetime, rs []uint32) []uint32 {
	return rs
}

func DecimalToU32(xs []types.Decimal, rs []uint32) []uint32 {
	return rs
}

func BytesToU32(xs *types.Bytes, rs []uint32) []uint32 {
	return rs
}

func I8ToU64(xs []int8, rs []uint64) []uint64 {
	return rs
}

func I16ToU64(xs []int16, rs []uint64) []uint64 {
	return rs
}

func I32ToU64(xs []int32, rs []uint64) []uint64 {
	return rs
}

func I64ToU64(xs []int64, rs []uint64) []uint64 {
	return rs
}

func U8ToU64(xs []uint8, rs []uint64) []uint64 {
	return rs
}

func U16ToU64(xs []uint16, rs []uint64) []uint64 {
	return rs
}

func U32ToU64(xs []uint32, rs []uint64) []uint64 {
	return rs
}

func U64ToU64(xs []uint64, rs []uint64) []uint64 {
	return rs
}

func F32ToU64(xs []float32, rs []uint64) []uint64 {
	return rs
}

func F64ToU64(xs []float64, rs []uint64) []uint64 {
	return rs
}

func DateToU64(xs []types.Date, rs []uint64) []uint64 {
	return rs
}

func DatetimeToU64(xs []types.Datetime, rs []uint64) []uint64 {
	return rs
}

func DecimalToU64(xs []types.Decimal, rs []uint64) []uint64 {
	return rs
}

func BytesToU64(xs *types.Bytes, rs []uint64) []uint64 {
	return rs
}

func I8ToF32(xs []int8, rs []float32) []float32 {
	return rs
}

func I16ToF32(xs []int16, rs []float32) []float32 {
	return rs
}

func I32ToF32(xs []int32, rs []float32) []float32 {
	return rs
}

func I64ToF32(xs []int64, rs []float32) []float32 {
	return rs
}

func U8ToF32(xs []uint8, rs []float32) []float32 {
	return rs
}

func U16ToF32(xs []uint16, rs []float32) []float32 {
	return rs
}

func U32ToF32(xs []uint32, rs []float32) []float32 {
	return rs
}

func U64ToF32(xs []uint64, rs []float32) []float32 {
	return rs
}

func F32ToF32(xs []float32, rs []float32) []float32 {
	return rs
}

func F64ToF32(xs []float64, rs []float32) []float32 {
	return rs
}

func DateToF32(xs []types.Date, rs []float32) []float32 {
	return rs
}

func DatetimeToF32(xs []types.Datetime, rs []float32) []float32 {
	return rs
}

func DecimalToF32(xs []types.Decimal, rs []float32) []float32 {
	return rs
}

func BytesToF32(xs *types.Bytes, rs []float32) []float32 {
	return rs
}

func I8ToF64(xs []int8, rs []float64) []float64 {
	return rs
}

func I16ToF64(xs []int16, rs []float64) []float64 {
	return rs
}

func I32ToF64(xs []int32, rs []float64) []float64 {
	return rs
}

func I64ToF64(xs []int64, rs []float64) []float64 {
	return rs
}

func U8ToF64(xs []uint8, rs []float64) []float64 {
	return rs
}

func U16ToF64(xs []uint16, rs []float64) []float64 {
	return rs
}

func U32ToF64(xs []uint32, rs []float64) []float64 {
	return rs
}

func U64ToF64(xs []uint64, rs []float64) []float64 {
	return rs
}

func F32ToF64(xs []float32, rs []float64) []float64 {
	return rs
}

func F64ToF64(xs []float64, rs []float64) []float64 {
	return rs
}

func DateToF64(xs []types.Date, rs []float64) []float64 {
	return rs
}

func DatetimeToF64(xs []types.Datetime, rs []float64) []float64 {
	return rs
}

func DecimalToF64(xs []types.Decimal, rs []float64) []float64 {
	return rs
}

func BytesToF64(xs *types.Bytes, rs []float64) []float64 {
	return rs
}

func I8ToDate(xs []int8, rs []types.Date) []types.Date {
	return rs
}

func I16ToDate(xs []int16, rs []types.Date) []types.Date {
	return rs
}

func I32ToDate(xs []int32, rs []types.Date) []types.Date {
	return rs
}

func I64ToDate(xs []int64, rs []types.Date) []types.Date {
	return rs
}

func U8ToDate(xs []uint8, rs []types.Date) []types.Date {
	return rs
}

func U16ToDate(xs []uint16, rs []types.Date) []types.Date {
	return rs
}

func U32ToDate(xs []uint32, rs []types.Date) []types.Date {
	return rs
}

func U64ToDate(xs []uint64, rs []types.Date) []types.Date {
	return rs
}

func F32ToDate(xs []float32, rs []types.Date) []types.Date {
	return rs
}

func F64ToDate(xs []float64, rs []types.Date) []types.Date {
	return rs
}

func DateToDate(xs []types.Date, rs []types.Date) []types.Date {
	return rs
}

func DatetimeToDate(xs []types.Datetime, rs []types.Date) []types.Date {
	return rs
}

func DecimalToDate(xs []types.Decimal, rs []types.Date) []types.Date {
	return rs
}

func BytesToDate(xs *types.Bytes, rs []types.Date) []types.Date {
	return rs
}

func I8ToDatetime(xs []int8, rs []types.Datetime) []types.Datetime {
	return rs
}

func I16ToDatetime(xs []int16, rs []types.Datetime) []types.Datetime {
	return rs
}

func I32ToDatetime(xs []int32, rs []types.Datetime) []types.Datetime {
	return rs
}

func I64ToDatetime(xs []int64, rs []types.Datetime) []types.Datetime {
	return rs
}

func U8ToDatetime(xs []uint8, rs []types.Datetime) []types.Datetime {
	return rs
}

func U16ToDatetime(xs []uint16, rs []types.Datetime) []types.Datetime {
	return rs
}

func U32ToDatetime(xs []uint32, rs []types.Datetime) []types.Datetime {
	return rs
}

func U64ToDatetime(xs []uint64, rs []types.Datetime) []types.Datetime {
	return rs
}

func F32ToDatetime(xs []float32, rs []types.Datetime) []types.Datetime {
	return rs
}

func F64ToDatetime(xs []float64, rs []types.Datetime) []types.Datetime {
	return rs
}

func DateToDatetime(xs []types.Date, rs []types.Datetime) []types.Datetime {
	return rs
}

func DatetimeToDatetime(xs []types.Datetime, rs []types.Datetime) []types.Datetime {
	return rs
}

func DecimalToDatetime(xs []types.Decimal, rs []types.Datetime) []types.Datetime {
	return rs
}

func BytesToDatetime(xs *types.Bytes, rs []types.Datetime) []types.Datetime {
	return rs
}

func I8ToBytes(xs []int8, rs *types.Bytes) *types.Bytes {
	return rs
}

func I16ToBytes(xs []int16, rs *types.Bytes) *types.Bytes {
	return rs
}

func I32ToBytes(xs []int32, rs *types.Bytes) *types.Bytes {
	return rs
}

func I64ToBytes(xs []int64, rs *types.Bytes) *types.Bytes {
	return rs
}

func U8ToBytes(xs []uint8, rs *types.Bytes) *types.Bytes {
	return rs
}

func U16ToBytes(xs []uint16, rs *types.Bytes) *types.Bytes {
	return rs
}

func U32ToBytes(xs []uint32, rs *types.Bytes) *types.Bytes {
	return rs
}

func U64ToBytes(xs []uint64, rs *types.Bytes) *types.Bytes {
	return rs
}

func F32ToBytes(xs []float32, rs *types.Bytes) *types.Bytes {
	return rs
}

func F64ToBytes(xs []float64, rs *types.Bytes) *types.Bytes {
	return rs
}

func DateToBytes(xs []types.Date, rs *types.Bytes) *types.Bytes {
	return rs
}

func DatetimeToBytes(xs []types.Datetime, rs *types.Bytes) *types.Bytes {
	return rs
}

func DecimalToBytes(xs []types.Decimal, rs *types.Bytes) *types.Bytes {
	return rs
}

func BytesToBytes(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	return rs
}
