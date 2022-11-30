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

package frontend

import (
	"testing"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec/simple"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/smartystreets/goconvey/convey"
)

func Test_protocol(t *testing.T) {
	convey.Convey("test protocol.go succ", t, func() {
		req := &Request{}
		req.SetCmd(1)
		convey.So(req.cmd, convey.ShouldEqual, 1)

		res := &Response{}
		res.SetStatus(1)
		convey.So(res.GetStatus(), convey.ShouldEqual, 1)

		res.SetCategory(2)
		convey.So(res.GetCategory(), convey.ShouldEqual, 2)

		cpi := &ProtocolImpl{}
		io := goetty.NewIOSession(goetty.WithSessionCodec(simple.NewStringCodec()))
		cpi.tcpConn = io

		str1, str2, _, _ := cpi.Peer()
		convey.So(str1, convey.ShouldEqual, "failed")
		convey.So(str2, convey.ShouldEqual, "0")
	})
}

func Test_SendResponse(t *testing.T) {
	convey.Convey("SendResponse succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iopackage := mock_frontend.NewMockIOPackage(ctrl)
		iopackage.EXPECT().WriteUint8(gomock.Any(), gomock.Any(), gomock.Any()).Return(0).AnyTimes()
		iopackage.EXPECT().WriteUint16(gomock.Any(), gomock.Any(), gomock.Any()).Return(0).AnyTimes()
		iopackage.EXPECT().WriteUint32(gomock.Any(), gomock.Any(), gomock.Any()).Return(0).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mp := &MysqlProtocolImpl{}
		mp.io = iopackage
		mp.tcpConn = ioses
		resp := &Response{}
		resp.category = EoFResponse
		err := mp.SendResponse(resp)
		convey.So(err, convey.ShouldBeNil)

		resp.SetData(moerr.NewInternalError(""))
		resp.category = ErrorResponse
		err = mp.SendResponse(resp)
		convey.So(err, convey.ShouldBeNil)

		resp.category = -1
		err = mp.SendResponse(resp)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
