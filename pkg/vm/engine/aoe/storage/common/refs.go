package common

import "sync/atomic"

type OnZeroCB func()

type RefHelper struct {
	Refs     int64
	OnZeroCB OnZeroCB
}

func (helper *RefHelper) RefCount() int64 {
	return atomic.LoadInt64(&helper.Refs)
}

func (helper *RefHelper) Ref() {
	atomic.AddInt64(&helper.Refs, int64(1))
}

func (helper *RefHelper) Unref() {
	v := atomic.AddInt64(&helper.Refs, int64(-1))
	if v == 0 {
		if helper.OnZeroCB != nil {
			helper.OnZeroCB()
		}
	} else if v < 0 {
		panic("logic error")
	}
}
