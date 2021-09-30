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

package metadata

import (
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"matrixone/pkg/vm/engine/aoe/storage/worker/base"
)

type hbHandle struct {
	catalog *Catalog
}

func (h *hbHandle) OnExec() {
	ckId := h.catalog.GetCheckpointId()
	cId := h.catalog.GetSafeCommitId()
	if cId > ckId && cId-ckId >= 1000 {
		if err := h.catalog.Checkpoint(); err != nil {
			logutil.Warn(err.Error())
		} else {
			logutil.Infof("Checkpoint %d", h.catalog.GetCheckpointId())
		}
	} else {
		if err := h.catalog.Store.Sync(); err != nil {
			logutil.Warn(err.Error())
		}
	}
}
func (h *hbHandle) OnStopped() {
	if err := h.catalog.Checkpoint(); err != nil {
		logutil.Warn(err.Error())
	}
	logutil.Infof("hbHandle Stoped at: %d", h.catalog.Store.GetSyncedId())
}

type hbHandleFactory struct {
	catalog *Catalog
}

func (factory *hbHandleFactory) builder(_ logstore.BufferedStore) base.IHBHandle {
	return &hbHandle{
		catalog: factory.catalog,
	}
}
