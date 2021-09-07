package wrapper

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

var (
	_ batch.IBatch = (*batchWrapper)(nil)
)

type batchWrapper struct {
	batch.IBatch
	host   common.IRef
	closed int32
}

func NewBatch(host common.IRef, attrs []int, vecs []vector.IVector) batch.IBatch {
	bat := &batchWrapper{
		host:   host,
		IBatch: batch.NewBatch(attrs, vecs),
	}
	return bat
}

func (bat *batchWrapper) close() error {
	err := bat.IBatch.Close()
	if err != nil {
		panic(err)
	}
	bat.host.Unref()
	return nil
}

func (bat *batchWrapper) Close() error {
	if atomic.CompareAndSwapInt32(&bat.closed, int32(0), int32(1)) {
		return bat.close()
	} else {
		panic("logic error")
	}
}
