package external

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"strconv"
	"strings"
	"time"
)

func getNullFlag(list []string, field string) bool {
	for i := 0; i < len(list); i++ {
		field = strings.ToLower(field)
		if list[i] == field {
			return true
		}
	}
	return false
}

func trimSpace(xs []string) ([]string, error) {
	var err error
	rs := make([]string, len(xs))
	for i := range xs {
		rs[i] = strings.TrimSpace(xs[i])
		if err != nil {
			return nil, err
		}
	}
	return rs, nil
}
func parseNullFlagNormal(xs, nullList []string) []bool {
	rs := make([]bool, len(xs))
	for i := range xs {
		rs[i] = xs[i] == NULL_FLAG || len(xs[i]) == 0 || getNullFlag(nullList, xs[i])
	}
	return rs
}
func parseNullFlagStrings(xs, nullList []string) []bool {
	rs := make([]bool, len(xs))
	for i := range xs {
		rs[i] = xs[i] == NULL_FLAG || getNullFlag(nullList, xs[i])
	}
	return rs
}

func insertNsp(xs []bool, nsp *nulls.Nulls) *nulls.Nulls {
	for i := range xs {
		if xs[i] {
			nsp.Set(uint64(i))
		}
	}
	return nsp
}

func parseBool(xs []string, nsp *nulls.Nulls, rs []bool) ([]bool, error) {

	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := strings.ToLower(xs[i])
			if xs[i] == "true" || field == "1" {
				rs[i] = true
			} else if field == "false" || field == "0" {
				rs[i] = false
			} else {
				return nil, moerr.NewInternalError("the input value '%s' is not bool type for row %d", field, i)
			}
		}
	}
	return rs, nil
}

func parseInt8(xs []string, nsp *nulls.Nulls, rs []int8) ([]int8, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseInt(field, 10, 8)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int8 type for row %d", field, i)
				}
				rs[i] = int8(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
					return nil, moerr.NewInternalError("the input value '%v' is not int8 type for row %d", field, i)
				}
				rs[i] = int8(d)
			}
		}
	}
	return rs, nil
}

func parseInt16(xs []string, nsp *nulls.Nulls, rs []int16) ([]int16, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseInt(field, 10, 16)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int16(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int16(d)
			}
		}
	}
	return rs, nil
}
func parseInt32(xs []string, nsp *nulls.Nulls, rs []int32) ([]int32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseInt(field, 10, 32)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int32(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int32(d)
			}
		}
	}
	return rs, nil
}

func parseInt64(xs []string, nsp *nulls.Nulls, rs []int64) ([]int64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseInt(field, 10, 64)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = d
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
					return nil, moerr.NewInternalError("the input value '%v' is not int16 type for row %d", field, i)
				}
				rs[i] = int64(d)
			}
		}
	}
	return rs, nil
}

func parseUint8(xs []string, nsp *nulls.Nulls, rs []uint8) ([]uint8, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseUint(field, 10, 8)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint8 type for row %d", field, i)
				}
				rs[i] = uint8(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint8 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint8 type for row %d", field, i)
				}
				rs[i] = uint8(d)
			}
		}
	}
	return rs, nil
}

func parseUint16(xs []string, nsp *nulls.Nulls, rs []uint16) ([]uint16, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseUint(field, 10, 16)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint16 type for row %d", field, i)
				}
				rs[i] = uint16(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint16 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint16 type for row %d", field, i)
				}
				rs[i] = uint16(d)
			}
		}
	}
	return rs, nil
}

func parseUint32(xs []string, nsp *nulls.Nulls, rs []uint32) ([]uint32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseUint(field, 10, 32)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint32 type for row %d", field, i)
				}
				rs[i] = uint32(d)
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint32 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint32 type for row %d", field, i)
				}
				rs[i] = uint32(d)
			}
		}
	}
	return rs, nil
}

func parseUint64(xs []string, nsp *nulls.Nulls, rs []uint64) ([]uint64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			if judgeInterge(field) {
				d, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return nil, moerr.NewInternalError("the input value '%v' is not uint64 type for row %d", field, i)
				}
				rs[i] = d
			} else {
				d, err := strconv.ParseFloat(field, 64)
				if err != nil || d < 0 || d > math.MaxUint64 {
					return nil, moerr.NewInternalError("the input value '%v' is not uint64 type for row %d", field, i)
				}
				rs[i] = uint64(d)
			}
		}
	}
	return rs, nil
}

func parseFloat32(xs []string, nsp *nulls.Nulls, rs []float32) ([]float32, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := strconv.ParseFloat(field, 32)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not float32 type for row %d", field, i)
			}
			rs[i] = float32(d)
		}
	}
	return rs, nil
}

func parseFloat64(xs []string, nsp *nulls.Nulls, rs []float64) ([]float64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := strconv.ParseFloat(field, 64)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not float64 type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func parseJson(xs []string, nsp *nulls.Nulls, rs [][]byte) ([][]byte, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			byteJson, err := types.ParseStringToByteJson(field)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not json type for row %d", field, i)
			}
			jsonBytes, err := types.EncodeJson(byteJson)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not json type for row %d", field, i)
			}
			rs[i] = jsonBytes
		}
	}
	return rs, nil
}

func parseDate(xs []string, nsp *nulls.Nulls, rs []types.Date) ([]types.Date, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseDate(field)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not date type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}
func parseDateTime(xs []string, nsp *nulls.Nulls, precision int32, rs []types.Datetime) ([]types.Datetime, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseDatetime(field, precision)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not datetime type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func parseTimeStamp(xs []string, nsp *nulls.Nulls, precision int32, rs []types.Timestamp) ([]types.Timestamp, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.ParseTimestamp(time.UTC, field, precision)
			if err != nil {
				return nil, moerr.NewInternalError("the input value '%v' is not timestamp type for row %d", field, i)
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func parseDecimal64(xs []string, nsp *nulls.Nulls, width, scale int32, rs []types.Decimal64) ([]types.Decimal64, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.Decimal64_FromStringWithScale(field, width, scale)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return nil, moerr.NewInternalError("the input value '%v' is invalid Decimal64 type for row %d", field, i)
				}
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func parseDecimal128(xs []string, nsp *nulls.Nulls, width, scale int32, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i := range xs {
		if !nsp.Contains(uint64(i)) {
			field := xs[i]
			d, err := types.Decimal128_FromStringWithScale(field, width, scale)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
					return nil, moerr.NewInternalError("the input value '%v' is invalid Decimal128 type for row %d", field, i)
				}
			}
			rs[i] = d
		}
	}
	return rs, nil
}

func ParseBool(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[bool](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseBool(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt8(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int8](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseInt8(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt16(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int16](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseInt16(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseInt32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[int64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseInt64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint8(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint8](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseUint8(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint16(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint16](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseUint16(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseUint32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseUint64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[uint64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseUint64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseFloat32(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[float32](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseFloat32(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseFloat64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[float64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	xs, err = trimSpace(xs)
	if err != nil {
		return nil, err
	}
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseFloat64(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseString(vectors []*vector.Vector, proc *process.Process) *vector.Vector {
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustStrCols(outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagStrings(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	copy(rs, xs)
	vector.SetCol(outputVec, rs)
	return outputVec
}

func ParseJson(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustBytesCols(outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseJson(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Date](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseDate(xs, outputVec.Nsp, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDateTime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Datetime](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseDateTime(xs, outputVec.Nsp, outputVec.Typ.Precision, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDecimal64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Decimal64](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseDecimal64(xs, outputVec.Nsp, outputVec.Typ.Width, outputVec.Typ.Scale, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseDecimal128(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Decimal128](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseDecimal128(xs, outputVec.Nsp, outputVec.Typ.Width, outputVec.Typ.Scale, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}

func ParseTimeStamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var err error
	inputVec := vectors[0]
	outputVec := vectors[1]
	nullVec := vectors[2]
	xs := vector.MustStrCols(inputVec)
	rs := vector.MustTCols[types.Timestamp](outputVec)
	nullList := vector.MustStrCols(nullVec)
	nullFlags := parseNullFlagNormal(xs, nullList)
	insertNsp(nullFlags, outputVec.Nsp)
	rs, err = parseTimeStamp(xs, outputVec.Nsp, outputVec.Typ.Precision, rs)
	if err != nil {
		return nil, err
	}
	vector.SetCol(outputVec, rs)
	return outputVec, nil
}
