package server

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/process"
)

type MysqlCmdExecutor struct {
	client.CmdExecutorImpl

	//the count of sql has been processed
	sqlCount uint64
}

//get new process id
func (mce *MysqlCmdExecutor) getNextProcessId()string{
	/*
	temporary method:
	routineId + sqlCount
	 */
	routineId := mce.Routine.ID()
	return fmt.Sprintf("%d%d",routineId,mce.sqlCount)
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64)  {
	mce.sqlCount += a
}

/*
extract the data from the pipeline.
obj: routine obj
TODO:Add error
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will
	access the shared data. To be careful, when it writes the shared data.
 */
func getDataFromPipeline(obj interface{},bat *batch.Batch){
	rt := obj.(client.Routine)
	ses := rt.GetSession()

	var choose bool = ! config.GlobalSystemVariables.GetSendRow()
	if choose {
		goID := client.GetRoutineId()

		fmt.Printf("goid %d \n",goID)

		proto := rt.GetClientProtocol().(*client.MysqlClientProtocol)

		//Create a new temporary resultset per pipeline thread.
		mrs := &client.MysqlResultSet{}
		//Warning: Don't change Columns in this.
		//Reference the shared Columns of the session among multi-thread.
		mrs.Columns = ses.Mrs.Columns
		mrs.Name2Index = ses.Mrs.Name2Index

		//one row
		row := make([]interface{}, len(bat.Vecs))
		mrs.Data = make([][]interface{},1)
		mrs.Data[0] = row

		if n := len(bat.Sels); n == 0 {
			n = bat.Vecs[0].Length()
			for j := int64(0); j < int64(n); j++ {//row index
				for i, vec := range bat.Vecs {//col index
					switch vec.Typ.Oid {//get col
					case types.T_int8:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int8)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int8)
								row[i] = vs[j]
							}
						}
					case types.T_uint8:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint8)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint8)
								row[i] = vs[j]
							}
						}
					case types.T_int16:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int16)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int16)
								row[i] = vs[j]
							}
						}
					case types.T_uint16:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint16)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint16)
								row[i] = vs[j]
							}
						}
					case types.T_int32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int32)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int32)
								row[i] = vs[j]
							}
						}
					case types.T_uint32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint32)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint32)
								row[i] = vs[j]
							}
						}
					case types.T_int64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int64)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int64)
								row[i] = vs[j]
							}
						}
					case types.T_uint64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint64)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint64)
								row[i] = vs[j]
							}
						}
					case types.T_float32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]float32)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]float32)
								row[i] = vs[j]
							}
						}
					case types.T_float64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]float64)
							row[i] = vs[j]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]float64)
								row[i] = vs[j]
							}
						}
					case types.T_char:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = vs.Get(j)
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.(*types.Bytes)
								row[i] = vs.Get(j)
							}
						}
					case types.T_varchar:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = vs.Get(j)
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.(*types.Bytes)
								row[i] = vs.Get(j)
							}
						}
					default:
						fmt.Printf("getDataFromPipeline : unsupported type %d \n",vec.Typ.Oid)
					}
				}

				//fmt.Printf("row -+> %v \n",row)

				//send row
				if err := proto.SendResultSetTextRow(mrs,0); err != nil {
					//return err
					fmt.Printf("getDataFromPipeline error %v \n",err)
				}
			}
		} else {
			for j := 0; j < n; j++ {//row index
				for i, vec := range bat.Vecs {//col index
					switch vec.Typ.Oid {//get col
					case types.T_int8:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int8)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int8)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_uint8:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint8)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint8)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_int16:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int16)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int16)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_uint16:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint16)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint16)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_int32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int32)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int32)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_uint32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint32)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint32)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_int64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]int64)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]int64)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_uint64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]uint64)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]uint64)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_float32:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]float32)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]float32)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_float64:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.([]float64)
							row[i] = vs[bat.Sels[j]]
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.([]float64)
								row[i] = vs[bat.Sels[j]]
							}
						}
					case types.T_char:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = vs.Get(bat.Sels[j])
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.(*types.Bytes)
								row[i] = vs.Get(bat.Sels[j])
							}
						}
					case types.T_varchar:
						if ! vec.Nsp.Any() {//all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = vs.Get(bat.Sels[j])
						}else{
							if vec.Nsp.Contains(uint64(j)) {//is null
								row[i] = nil
							}else{
								vs := vec.Col.(*types.Bytes)
								row[i] = vs.Get(bat.Sels[j])
							}
						}
					default:
						fmt.Printf("getDataFromPipeline : unsupported type %d \n",vec.Typ.Oid)
					}
				}

				//fmt.Printf("row -*> %v \n",row)

				//send row
				if err := proto.SendResultSetTextRow(mrs,0); err != nil {
					//return err
					fmt.Printf("getDataFromPipeline error %v \n",err)
				}
			}
		}
	}else {

		if n := len(bat.Sels); n == 0 {
			n = bat.Vecs[0].Length()
			rows := make([][]interface{}, n)
			for i := 0; i < n; i++ {
				rows[i] = make([]interface{}, len(bat.Vecs))
			}
			for i, vec := range bat.Vecs {//column index
				switch vec.Typ.Oid {
				case types.T_int8:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int8)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int8)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_uint8:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint8)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint8)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_int16:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int16)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int16)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_uint16:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint16)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint16)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_int32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int32)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_uint32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint32)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_int64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int64)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_uint64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint64)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_float32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]float32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]float32)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_float64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]float64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[j]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]float64)
								rows[j][i] = vs[j]
							}
						}
					}
				case types.T_char:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.(*types.Bytes)
						for j := 0; j < n; j++ {
							rows[j][i] = vs.Get(int64(j))
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								rows[j][i] = vs.Get(int64(j))
							}
						}
					}
				case types.T_varchar:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.(*types.Bytes)
						for j := 0; j < n; j++ {
							rows[j][i] = vs.Get(int64(j))
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								rows[j][i] = vs.Get(int64(j))
							}
						}
					}
				default:
					fmt.Printf("FillResult else1: unsupported type %d \n",vec.Typ.Oid)
				}
			}
			ses.Mrs.Data = rows
		} else {
			rows := make([][]interface{}, n)
			for i := 0; i < n; i++ {
				rows[i] = make([]interface{}, len(bat.Vecs))
			}
			for i, vec := range bat.Vecs {
				switch vec.Typ.Oid {
				case types.T_int8:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int8)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int8)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_uint8:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint8)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint8)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_int16:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int16)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int16)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_uint16:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint16)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint16)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_int32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int32)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_uint32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint32)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_int64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]int64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]int64)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_uint64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]uint64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]uint64)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_float32:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]float32)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]float32)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_float64:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.([]float64)
						for j := 0; j < n; j++ {
							rows[j][i] = vs[bat.Sels[j]]
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.([]float64)
								rows[j][i] = vs[bat.Sels[j]]
							}
						}
					}
				case types.T_char:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.(*types.Bytes)
						for j := 0; j < n; j++ {
							rows[j][i] = vs.Get(bat.Sels[j])
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								rows[j][i] = vs.Get(bat.Sels[j])
							}
						}
					}
				case types.T_varchar:
					if ! vec.Nsp.Any() {//all data in this column are not null
						vs := vec.Col.(*types.Bytes)
						for j := 0; j < n; j++ {
							rows[j][i] = vs.Get(bat.Sels[j])
						}
					}else{
						for j := 0; j < n; j++ {
							if vec.Nsp.Contains(uint64(j)) { //is null
								rows[j][i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								rows[j][i] = vs.Get(bat.Sels[j])
							}
						}
					}
				default:
					fmt.Printf("FillResult else2: unsupported type %d \n", vec.Typ.Oid)
				}
			}
		}
	}
}

//execute query
func (mce *MysqlCmdExecutor) doComQuery(sql string) error {
	ses := mce.Routine.GetSession()
	proto := mce.Routine.GetClientProtocol().(*client.MysqlClientProtocol)

	proc := process.New(ses.GuestMmu, ses.Mempool)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = config.GlobalSystemVariables.GetProcessLimitationSize()
	proc.Lim.BatchRows = config.GlobalSystemVariables.GetProcessLimitationBatchRows()
	proc.Lim.PartitionRows = config.GlobalSystemVariables.GetProcessLimitationPartitionRows()
	proc.Refer = make(map[string]uint64)

	comp := compile.New(ses.Dbname, sql, config.StorageEngine, config.ClusterNodes, proc)
	execs, err := comp.Compile(mce.Routine,getDataFromPipeline)
	if err != nil {
		return err
	}

	var choose bool = ! config.GlobalSystemVariables.GetSendRow()

	ses.Mrs = &client.MysqlResultSet{}

	defer func(){
		ses.Mrs = nil
	}()

	for _, exec := range execs {
		columns := exec.Columns()
		if choose {

			/*
			Step 1 : send column count and column definition.
			 */

			//send column count
			colCnt := uint64(len(columns))
			if err = proto.SendColumnCount(colCnt);err!=nil{
				return err
			}

			//send columns
			//column_count * Protocol::ColumnDefinition packets
			cmd := ses.Cmd
			for _, c := range columns {
				col := new(client.MysqlColumn)
				col.SetName(c.Name)
				switch c.Typ {
				case types.T_int8:
					col.SetColumnType(client.MYSQL_TYPE_TINY)
				case types.T_uint8:
					col.SetColumnType(client.MYSQL_TYPE_TINY)
					col.SetSigned(true)
				case types.T_int16:
					col.SetColumnType(client.MYSQL_TYPE_SHORT)
				case types.T_uint16:
					col.SetColumnType(client.MYSQL_TYPE_SHORT)
					col.SetSigned(true)
				case types.T_int32:
					col.SetColumnType(client.MYSQL_TYPE_LONG)
				case types.T_uint32:
					col.SetColumnType(client.MYSQL_TYPE_LONG)
					col.SetSigned(true)
				case types.T_int64:
					col.SetColumnType(client.MYSQL_TYPE_LONGLONG)
				case types.T_uint64:
					col.SetColumnType(client.MYSQL_TYPE_LONGLONG)
					col.SetSigned(true)
				case types.T_float32:
					col.SetColumnType(client.MYSQL_TYPE_FLOAT)
				case types.T_float64:
					col.SetColumnType(client.MYSQL_TYPE_DOUBLE)
				case types.T_char:
					col.SetColumnType(client.MYSQL_TYPE_STRING)
				case types.T_varchar:
					col.SetColumnType(client.MYSQL_TYPE_VAR_STRING)
				default:
					return fmt.Errorf("RunWhileSend : unsupported type %d \n",c.Typ)
				}

				ses.Mrs.AddColumn(col)

				//fmt.Printf("doComQuery col name %v type %v \n",col.Name(),col.ColumnType())

				/*
					mysql COM_QUERY response: send the column definition per column
				*/
				if err = proto.SendColumnDefinition(col,cmd); err != nil {
					return err
				}
			}

			/*
				mysql COM_QUERY response: End after the column has been sent.
				send EOF packet
			*/
			if err = proto.SendEOFPacketIf(0,0); err != nil {
				return err
			}

			/*
			Step 2: Start pipeline
			Producing the data row and sending the data row
			 */

			if er := exec.Run(); er != nil {
				return er
			}

			/*
			Step 3: Say goodbye
			 */

			/*
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			if err = proto.SendEOFOrOkPacket(0,0); err != nil {
				return err
			}
		}else{
			for _, c := range columns {
				col := new(client.MysqlColumn)
				col.SetName(c.Name)
				switch c.Typ {
				case types.T_int8:
					col.SetColumnType(client.MYSQL_TYPE_TINY)
				case types.T_uint8:
					col.SetColumnType(client.MYSQL_TYPE_TINY)
					col.SetSigned(true)
				case types.T_int16:
					col.SetColumnType(client.MYSQL_TYPE_SHORT)
				case types.T_uint16:
					col.SetColumnType(client.MYSQL_TYPE_SHORT)
					col.SetSigned(true)
				case types.T_int32:
					col.SetColumnType(client.MYSQL_TYPE_LONG)
				case types.T_uint32:
					col.SetColumnType(client.MYSQL_TYPE_LONG)
					col.SetSigned(true)
				case types.T_int64:
					col.SetColumnType(client.MYSQL_TYPE_LONGLONG)
				case types.T_uint64:
					col.SetColumnType(client.MYSQL_TYPE_LONGLONG)
					col.SetSigned(true)
				case types.T_float32:
					col.SetColumnType(client.MYSQL_TYPE_FLOAT)
				case types.T_float64:
					col.SetColumnType(client.MYSQL_TYPE_DOUBLE)
				case types.T_char:
					col.SetColumnType(client.MYSQL_TYPE_STRING)
				case types.T_varchar:
					col.SetColumnType(client.MYSQL_TYPE_VAR_STRING)
				default:
					return fmt.Errorf("RunWhileSend : unsupported type %d \n",c.Typ)
				}

				ses.Mrs.AddColumn(col)
			}

			if er := exec.Run(); er != nil {
				return er
			}

			mer := client.NewMysqlExecutionResult(0,0,0,0,ses.Mrs)
			resp := client.NewResponse(client.ResultResponse,0,int(client.COM_QUERY),mer)

			if err = proto.SendResponse(resp);err != nil{
				return fmt.Errorf("routine send response failed. error:%v ",err)
			}
		}
	}

	return nil
}

//the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(req *client.Request) (*client.Response, error) {
	var resp *client.Response = nil
	switch uint8(req.GetCmd()){
	case client.COM_QUIT:
		resp = client.NewResponse(
			client.OkResponse,
			0,
			int(client.COM_QUIT),
			nil,
		)
		return resp,nil
	case client.COM_QUERY:
		var query =string(req.GetData().([]byte))

		mce.addSqlCount(1)

		fmt.Printf("query:%s \n",query)

		err := mce.doComQuery(query)

		if err != nil{
			resp = client.NewResponse(
				client.ErrorResponse,
				0,
				int(client.COM_QUERY),
				err,
			)
		}
		return resp,nil
	case client.COM_INIT_DB:

		var dbname =string(req.GetData().([]byte))
		ses := mce.Routine.GetSession()
		oldname := ses.Dbname
		ses.Dbname = dbname

		fmt.Printf("user %s change databse from [%s] to [%s]\n",ses.User,oldname,ses.Dbname)

		resp = client.NewResponse(
			client.OkResponse,
			0,
			int(client.COM_INIT_DB),
			nil,
		)
		return resp,nil
	default:
		err := fmt.Errorf("unsupported command. 0x%x \n",req.Cmd)
		resp = client.NewResponse(
			client.ErrorResponse,
			0,
			req.Cmd,
			err,
		)
	}
	return resp,nil
}

func (mce *MysqlCmdExecutor) Close() {
	//TODO:
}

func NewMysqlCmdExecutor()*MysqlCmdExecutor{
	return &MysqlCmdExecutor{}
}