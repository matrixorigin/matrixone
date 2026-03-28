// Copyright 2024 Matrix Origin
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

package service

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

// catalogActivation represents an in-flight activation request.
type catalogActivation struct {
	timeout   time.Duration
	accountID uint32
	seq       uint64
	session   *Session
}

// catalogActivationPhase1 carries phase-1 pull results for all three catalog
// tables. It is sent from the pull goroutine to the logtailSender for
// serialized phase-2 completion.
type catalogActivationPhase1 struct {
	activation catalogActivation
	tails      [3]logtail.TableLogtail
	closeCBs   [3]func()
}

// lazyCatalogTableIDs lists the three catalog tables in a fixed order that
// aligns with the tails/closeCBs arrays in catalogActivationPhase1.
var lazyCatalogTableIDs = [3]api.TableID{
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_DATABASE_ID},
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
}

func (p *catalogActivationPhase1) closeAll() {
	for i := range p.closeCBs {
		if p.closeCBs[i] != nil {
			p.closeCBs[i]()
			p.closeCBs[i] = nil
		}
	}
}

func (p *catalogActivationPhase1) takeCloseCB(idx int) func() {
	cb := p.closeCBs[idx]
	p.closeCBs[idx] = nil
	return cb
}

func closeCallbacks(callbacks ...func()) {
	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}
}

func composeCloseCallback(callbacks ...func()) func() {
	var nonNil []func()
	for _, cb := range callbacks {
		if cb != nil {
			nonNil = append(nonNil, cb)
		}
	}
	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return func() {
			closeCallbacks(nonNil...)
		}
	}
}
