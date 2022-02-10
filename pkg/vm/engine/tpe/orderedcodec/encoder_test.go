package orderedcodec

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestOrderedEncoder_EncodeKey(t *testing.T) {
	convey.Convey("encodeKey null",t, func() {
		oe := &OrderedEncoder{}
		d,_ := oe.EncodeKey([]byte{},nil)
		convey.So(d[len(d) - 1],convey.ShouldEqual,nullEncoding)
	})
}

func TestOrderedEncoder_EncodeNull(t *testing.T) {
	convey.Convey("encodeNUll",t, func() {
		oe := &OrderedEncoder{}
		kases := [][]byte{
			nil,
			[]byte{},
			[]byte{0x0,0x1},
		}
		for _, k := range kases {
			d,_ := oe.EncodeNull(k)
			convey.So(d[len(d) - 1],convey.ShouldEqual,nullEncoding)
		}
	})
}