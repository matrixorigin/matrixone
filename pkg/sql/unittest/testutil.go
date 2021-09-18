package unittest

import (
	"fmt"
	"matrixone/pkg/logutil"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
)

func StartTestServer(port int, e engine.Engine, proc *process.Process) {
	srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", port), 1<<30, logutil.GetGlobalLogger())
	if err != nil {
		logutil.Fatal(err.Error())
	}
	hp := handler.New(e, proc)
	srv.Register(hp.Process)
	go srv.Run()
}
