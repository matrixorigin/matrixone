package server

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

	routine Routine
}

func (cei *CmdExecutorImpl) SetRoutine(r Routine)  {
	cei.routine = r
}