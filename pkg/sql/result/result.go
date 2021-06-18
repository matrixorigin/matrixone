package result

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
)

func (rp *Result) FillResult(bat *batch.Batch) error {
	var choose bool = config.GlobalSystemVariables.GetSendRow()
	if choose {
		goID := client.GetRoutineId()
		fmt.Printf("goid %d \n",goID)

		ses := client.Rt.GetSession()
		proto := client.Rt.GetClientProtocol().(*client.MysqlClientProtocol)

		mrs := ses.Mrs
		row := make([]interface{}, len(rp.Attrs))
		//one row
		mrs.Data = make([][]interface{},1)
		mrs.Data[0] = row

		if n := len(bat.Sels); n == 0 {
			n = bat.Vecs[0].Length()
			for j := int64(0); j < int64(n); j++ {//row index
				for i, vec := range bat.Vecs {//col index
					switch vec.Typ.Oid {//get col
					case types.T_int8:
						vs := vec.Col.([]int8)
						row[i] = vs[j]
					case types.T_uint8:
						vs := vec.Col.([]uint8)
						row[i] = vs[j]
					case types.T_int16:
						vs := vec.Col.([]int16)
						row[i] = vs[j]
					case types.T_uint16:
						vs := vec.Col.([]uint16)
						row[i] = vs[j]
					case types.T_int32:
						vs := vec.Col.([]int32)
						row[i] = vs[j]
					case types.T_uint32:
						vs := vec.Col.([]uint32)
						row[i] = vs[j]
					case types.T_int64:
						vs := vec.Col.([]int64)
						row[i] = vs[j]
					case types.T_uint64:
						vs := vec.Col.([]uint64)
						row[i] = vs[j]
					case types.T_float32:
						vs := vec.Col.([]float32)
						row[i] = vs[j]
					case types.T_float64:
						vs := vec.Col.([]float64)
						row[i] = vs[j] //get the j row of the col
					case types.T_char:
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(j)
					case types.T_varchar:
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(j)
					default:
						return fmt.Errorf("FillResult : unsupported type %d \n",vec.Typ.Oid)
					}
				}

				//send row
				if err := proto.SendResultSetTextRow(mrs,0); err != nil {
					return err
				}
			}
			rp.Rows = nil
		} else {
			for j := 0; j < n; j++ {//row index
				for i, vec := range bat.Vecs {//col index
					switch vec.Typ.Oid {//get col
					case types.T_int8:
						vs := vec.Col.([]int8)
						row[i] = vs[bat.Sels[j]]
					case types.T_uint8:
						vs := vec.Col.([]uint8)
						row[i] = vs[bat.Sels[j]]
					case types.T_int16:
						vs := vec.Col.([]int16)
						row[i] = vs[bat.Sels[j]]
					case types.T_uint16:
						vs := vec.Col.([]uint16)
						row[i] = vs[bat.Sels[j]]
					case types.T_int32:
						vs := vec.Col.([]int32)
						row[i] = vs[bat.Sels[j]]
					case types.T_uint32:
						vs := vec.Col.([]uint32)
						row[i] = vs[bat.Sels[j]]
					case types.T_int64:
						vs := vec.Col.([]int64)
						row[i] = vs[bat.Sels[j]]
					case types.T_uint64:
						vs := vec.Col.([]uint64)
						row[i] = vs[bat.Sels[j]]
					case types.T_float32:
						vs := vec.Col.([]float32)
						row[i] = vs[bat.Sels[j]]
					case types.T_float64:
						vs := vec.Col.([]float64)
						row[i] = vs[bat.Sels[j]] //get the j row of the col
					case types.T_char:
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(bat.Sels[j])
					case types.T_varchar:
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(bat.Sels[j])
					default:
						return fmt.Errorf("RunWhileSend : unsupported type %d \n",vec.Typ.Oid)
					}
				}

				//send row
				if err := proto.SendResultSetTextRow(mrs,0); err != nil {
					return err
				}
			}
			rp.Rows = nil
		}
	}else {
		if n := len(bat.Sels); n == 0 {
			n = bat.Vecs[0].Length()
			rows := make([][]interface{}, n)
			for i := 0; i < n; i++ {
				rows[i] = make([]interface{}, len(rp.Attrs))
			}
			for i, vec := range bat.Vecs {//column index
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[j]
					}
				case types.T_char:
					vs := vec.Col.(*types.Bytes)
					for j := int64(0); j < int64(n); j++ {
						rows[j][i] = vs.Get(j)
					}
				case types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					for j := int64(0); j < int64(n); j++ {
						rows[j][i] = vs.Get(j)
					}
				default:
					return fmt.Errorf("FillResult else1: unsupported type %d \n",vec.Typ.Oid)
				}
			}
			rp.Rows = rows
		} else {
			rows := make([][]interface{}, n)
			for i := 0; i < n; i++ {
				rows[i] = make([]interface{}, len(rp.Attrs))
			}
			for i, vec := range bat.Vecs {
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					for j := 0; j < n; j++ {
						rows[j][i] = vs[bat.Sels[j]]
					}
				case types.T_char:
					vs := vec.Col.(*types.Bytes)
					for j := 0; j < n; j++ {
						rows[j][i] = vs.Get(bat.Sels[j])
					}
				case types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					for j := 0; j < n; j++ {
						rows[j][i] = vs.Get(bat.Sels[j])
					}
				default:
					return fmt.Errorf("FillResult else2: unsupported type %d \n",vec.Typ.Oid)
				}
			}
			rp.Rows = rows
		}
	}
	return nil
}
