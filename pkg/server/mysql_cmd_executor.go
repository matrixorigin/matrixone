package server

import (
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
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
	execs, err := comp.Compile()
	if err != nil {
		return err
	}

	var choose bool = config.GlobalSystemVariables.GetSendRow()

	for _, exec := range execs {
		if choose {
			client.Rt = mce.Routine
			if er := exec.RunWhileSend(mce.Routine); er != nil {
				return er
			}
		}else{
			mrs := new(client.MysqlResultSet)
			if er := exec.Run(mrs); er != nil {
				return er
			}

			mer := client.NewMysqlExecutionResult(0,0,0,0,mrs)
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