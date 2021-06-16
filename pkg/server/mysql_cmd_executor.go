package server

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/process"
)

type MysqlCmdExecutor struct {
	CmdExecutorImpl

	//the count of sql has been processed
	sqlCount uint64
}

//get new process id
func (mce *MysqlCmdExecutor) getNextProcessId()string{
	/*
	temporary method:
	routineId + sqlCount
	 */
	routineId := mce.routine.ID()
	return fmt.Sprintf("%d%d",routineId,mce.sqlCount)
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64)  {
	mce.sqlCount += a
}

//execute query
func (mce *MysqlCmdExecutor) doComQuery(sql string) error {
	ses := mce.routine.GetSession()
	proto := mce.routine.GetClientProtocol()

	proc := process.New(ses.guestMmu, ses.mempool)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = 10 << 32
	proc.Lim.BatchRows = 10 << 32
	proc.Lim.PartitionRows = 10 << 32
	proc.Refer = make(map[string]uint64)

	comp := compile.New(ses.dbname, sql, StorageEngine, ClusterNodes, proc)
	execs, err := comp.Compile()
	if err != nil {
		return err
	}

	for _, exec := range execs {
		mrs := new(client.MysqlResultSet)
		if er := exec.Run(mrs); er != nil {
			return er
		}

		mer := client.NewMysqlExecutionResult(0,0,0,0,mrs)
		fmt.Printf("row count %d col count %d\n",mrs.GetRowCount(),mrs.GetColumnCount())
		resp := &Response{
			category: resultResponse,
			status: 0,
			cmd:int(COM_QUERY),
			data:mer,
		}

		if err = proto.SendResponse(resp);err != nil{
			return fmt.Errorf("routine send response failed. error:%v ",err)
		}
	}

	return nil
}

//the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(req *Request) (*Response, error) {
	var resp *Response = nil
	switch uint8(req.GetCmd()){
	case COM_QUIT:
		resp = &Response{
			category: okResponse,
			status: 0,
			cmd:int(COM_QUIT),
			data:nil,
		}
		return resp,nil
	case COM_QUERY:
		var query =string(req.GetData().([]byte))

		mce.addSqlCount(1)

		fmt.Printf("query:%s \n",query)

		err := mce.doComQuery(query)
		if err != nil{
			resp = &Response{
				category: errorResponse,
				status: 0,
				cmd:int(COM_QUERY),
				data:err,
			}
		}
		return resp,nil
	case COM_INIT_DB:

		var dbname =string(req.GetData().([]byte))
		ses := mce.routine.GetSession()
		oldname := ses.dbname
		ses.dbname = dbname

		fmt.Printf("user %s change databse from [%s] to [%s]\n",ses.user,oldname,ses.dbname)

		resp = &Response{
			category: okResponse,
			status: 0,
			cmd:int(COM_INIT_DB),
			data:nil,
		}
		return resp,nil
	default:
		err := fmt.Errorf("unsupported command. 0x%x \n",req.cmd)
		resp = &Response{
			category: errorResponse,
			status: 0,
			cmd: req.cmd,
			data: err,
		}
	}
	return resp,nil
}

func (mce *MysqlCmdExecutor) Close() {
	//TODO:
}

func NewMysqlCmdExecutor()*MysqlCmdExecutor{
	return &MysqlCmdExecutor{}
}