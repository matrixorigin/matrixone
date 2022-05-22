package common

import (
	"context"
	"runtime"
)

type RetryOp = func() error

func DoRetry(op RetryOp, ctx context.Context) (err error) {
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		err = op()
		if err == nil {
			break
		}
		runtime.Gosched()
	}
	return
}
