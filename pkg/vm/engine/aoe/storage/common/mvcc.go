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

package common

import (
	"sync/atomic"
	"unsafe"
	// log "github.com/sirupsen/logrus"
)

type MVCC interface {
	GetPrevVersion() interface{}
	SetPrevVersion(interface{})
	GetNextVersion() interface{}
	SetNextVersion(interface{})
	Object() interface{}
}

type PinFunc func(interface{})
type UnpinFunc func(interface{})
type GetObjectFunc func() interface{}

type BaseMvcc struct {
	PrevVer   *interface{}
	NextVer   *interface{}
	Pin       PinFunc
	Unpin     UnpinFunc
	GetObject GetObjectFunc
}

func (mvcc *BaseMvcc) Object() interface{} {
	return mvcc.GetObject()
}

func (mvcc *BaseMvcc) GetPrevVersion() interface{} {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mvcc.PrevVer)))
	if ptr == nil {
		return nil
	}
	return *(*interface{})(ptr)
}

func (mvcc *BaseMvcc) SetPrevVersion(prev interface{}) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&mvcc.PrevVer)), unsafe.Pointer(&prev))
	if prev != nil {
		mvcc.Pin(mvcc.GetObject())
		prev.(MVCC).SetNextVersion(mvcc)
	}
}

func (mvcc *BaseMvcc) GetNextVersion() interface{} {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mvcc.NextVer)))
	if ptr == nil {
		return nil
	}
	return *(*interface{})(ptr)
}

func (mvcc *BaseMvcc) SetNextVersion(next interface{}) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&mvcc.NextVer)), unsafe.Pointer(&next))
}

func (mvcc *BaseMvcc) OnVersionStale() {
	nextVer := mvcc.GetNextVersion()
	if nextVer != nil {
		nv := nextVer.(MVCC)
		nv.SetPrevVersion(nil)
		if mvcc.Unpin != nil {
			mvcc.Unpin(nv.Object())
		}
	}
}
