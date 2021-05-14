package server

//handle the command from the client
type CmdExecutor interface {
	//execute the request and get the response
	ExecRequest(*Request) (*Response,error)

	Close()
}

type CmdExecutorImpl struct {
	//sql parser
	//database engine
}