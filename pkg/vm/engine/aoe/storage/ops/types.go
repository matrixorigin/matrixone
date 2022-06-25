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

package ops

import (
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
	// log "github.com/sirupsen/logrus"
)

type OpDoneCB = func(iops.IOp)

type Op struct {
	Impl       iops.IOpInternal
	ErrorC     chan error
	Worker     iworker.IOpWorker
	Err        error
	Result     interface{}
	CreateTime time.Time
	StartTime  time.Time
	EndTime    time.Time
	DoneCB     OpDoneCB
	Observers  []iops.Observer
}
