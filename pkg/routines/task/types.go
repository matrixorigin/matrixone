package task

import "matrixbase/pkg/mempool"

type Task interface {
	Stop(TaskResult)
	Execute(*mempool.Mempool) TaskResult
}

type TaskResult interface {
	Error() error
	Result() interface{}
}
