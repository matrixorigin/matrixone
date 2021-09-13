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

package gci

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
)

type RequestType int32

const (
	GCDropTable RequestType = 0
)

const (
	DefaultInterval = 10 * time.Millisecond
)

type WorkerCfg struct {
	Interval time.Duration
	Executor iw.IOpWorker
}

type IRequest interface {
	iops.IOp
	GetNext() IRequest
	IncIteration()
	GetIteration() uint32
}

type IAcceptor interface {
	Accept(IRequest)
	Start()
	Stop()
}

type IWorker interface {
	IAcceptor
	iw.IOpWorker
}
