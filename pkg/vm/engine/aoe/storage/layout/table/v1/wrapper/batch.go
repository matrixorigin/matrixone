// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wrapper

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
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

func NewBatch(host common.IRef, attrs []int, vecs []vector.IVector) (batch.IBatch, error) {
	ibat, err := batch.NewBatch(attrs, vecs)
	if err != nil {
		return nil, err
	}
	bat := &batchWrapper{
		host:   host,
		IBatch: ibat,
	}
	return bat, nil
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
