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

package tuplecodec

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"sync"
)

var (
	errorItIsNotDescriptorHandlerImpl = errors.New("it is not descriptor handler impl")
)

type EpochHandler struct {
	epoch  uint64
	tch    *TupleCodecHandler
	dh     descriptor.DescriptorHandler
	kv     KVHandler
	rwlock sync.RWMutex
}

func NewEpochHandler(tch *TupleCodecHandler,
		dh descriptor.DescriptorHandler,
		kv KVHandler) *EpochHandler {
	return &EpochHandler{
		tch: tch,
		dh: dh,
		kv: kv,
	}
}

func (eh *EpochHandler) SetEpoch(e uint64) {
	eh.rwlock.Lock()
	defer eh.rwlock.Unlock()
	eh.epoch = e
}

func (eh *EpochHandler) GetEpoch() uint64{
	eh.rwlock.RLock()
	defer eh.rwlock.RUnlock()
	return eh.epoch
}

func (eh *EpochHandler) RemoveDeletedTable(epoch uint64) (int, error) {
	eh.rwlock.Lock()
	defer eh.rwlock.Unlock()

	//1.load epoch items from asyncGC
	gcItems, err := eh.dh.ListRelationDescFromAsyncGC(epoch)
	if err != nil {
		return 0, err
	}

	tke := eh.tch.GetEncoder()

	dhi,ok := eh.dh.(*DescriptorHandlerImpl)
	if !ok {
		return 0, errorItIsNotDescriptorHandlerImpl
	}

	//2. drop table data on gcItems
	for _, item := range gcItems {
		//delete the data in the table
		prefixDeleted, _ := tke.EncodeIndexPrefix(nil, item.DbID, item.TableID,uint64(PrimaryIndexID))
		err = eh.kv.DeleteWithPrefix(prefixDeleted)
		if err != nil {
			return 0, err
		}

		//3.delete gc item in asyncGC table
		epochItemKeyDeleted,_ := dhi.MakePrefixWithEpochAndDBIDAndTableID(
			InternalDatabaseID,InternalAsyncGCTableID,uint64(PrimaryIndexID),
			item.Epoch,item.DbID,item.TableID)

		err = dhi.kvHandler.Delete(epochItemKeyDeleted)
		if err != nil {
			return 0, err
		}
	}

	return len(gcItems), nil
}