package client

//handle the command from the client
type CmdExecutor interface {
	//execute the request and get the response
	ExecRequest(*Request) (*Response,error)

	Close()

	//the routine
	SetRoutine(Routine)
}

type CmdExecutorImpl struct {
	CmdExecutor
	//sql parser
	//database engine

	Routine Routine
}

func (cei *CmdExecutorImpl) SetRoutine(r Routine)  {
	cei.Routine = r
}