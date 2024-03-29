// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var StopMerge atomic.Bool

func init() {
	StopMerge.Store(true)
}

const (
	constMergeMinBlks       = 5
	constMergeExpansionRate = 6
	constMaxMemCap          = 4 * constMergeExpansionRate * common.Const1GBytes // max orginal memory for a object
	constSmallMergeGap      = 3 * time.Minute
)

type Policy interface {
	OnObject(obj *catalog.ObjectEntry)
	Revise(cpu, mem int64) []*catalog.ObjectEntry
	ResetForTable(*catalog.TableEntry)
	SetConfig(*catalog.TableEntry, func() txnif.AsyncTxn, any)
	GetConfig(*catalog.TableEntry) any
}
