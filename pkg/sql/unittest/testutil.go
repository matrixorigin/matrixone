package unittest

import (
	"fmt"
	"matrixone/pkg/logutil"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
)

func NewTestServer(port int, e engine.Engine, proc *process.Process) (rpcserver.Server, error) {
	srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", port), 1<<30, logutil.GetGlobalLogger())
	if err != nil {
		return nil, err
	}
	hp := handler.New(e, proc)
	srv.Register(hp.Process)
	return srv, nil
}
