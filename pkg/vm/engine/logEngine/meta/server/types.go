package server

import (
	"matrixbase/pkg/vm/engine/logEngine/meta"
	"time"
)

type Arg struct {
	Id string
	Md meta.Metadata
}

type LockArg struct {
	Id   string
	Time time.Time
}
