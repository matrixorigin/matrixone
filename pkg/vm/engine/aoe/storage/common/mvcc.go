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
