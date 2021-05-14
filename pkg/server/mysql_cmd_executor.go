package server

import "fmt"

type MysqlCmdExecutor struct {
	CmdExecutorImpl
}

//the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(req *Request) (*Response, error) {
	var resp *Response
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
		fmt.Printf("query: %s \n",query)
		resp = &Response{
			category: okResponse,
			status: 0,
			cmd:int(COM_QUERY),
			data:nil,
		}
		return resp,nil
	default:
		fmt.Printf("unsupported command. 0x%x \n",req.cmd)
	}
	return nil,	nil
}

func (mce *MysqlCmdExecutor) Close() {
	//TODO:
}

func NewMysqlCmdExecutor()*MysqlCmdExecutor{
	return &MysqlCmdExecutor{}
}