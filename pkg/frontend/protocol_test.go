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
	"context"
	"github.com/matrixorigin/matrixone/pkg/config"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Test_protocol(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	convey.Convey("test protocol.go succ", t, func() {
		req := &Request{}
		req.SetCmd(1)
		convey.So(req.cmd, convey.ShouldEqual, 1)

		res := &Response{}
		res.SetStatus(1)
		convey.So(res.GetStatus(), convey.ShouldEqual, 1)

		res.SetCategory(2)
		convey.So(res.GetCategory(), convey.ShouldEqual, 2)

		cpi := &MysqlProtocolImpl{}

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setGlobalPu(pu)
		io, err := NewIOSession(serverConn, pu)
		convey.ShouldBeNil(err)
		cpi.tcpConn = io

		str1 := cpi.Peer()
		convey.So(str1, convey.ShouldEqual, "pipe")
	})
}

func Test_SendResponse(t *testing.T) {
	ctx := context.TODO()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go startConsumeRead(clientConn)
	convey.Convey("SendResponse succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iopackage := NewIOPackage(true)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setGlobalPu(pu)
		ioses, err := NewIOSession(serverConn, pu)
		convey.ShouldBeNil(err)
		mp := &MysqlProtocolImpl{}
		mp.io = iopackage
		mp.tcpConn = ioses
		resp := &Response{}
		resp.category = EoFResponse
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)

		resp.SetData(moerr.NewInternalError(context.TODO(), ""))
		resp.category = ErrorResponse
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)

		resp.category = -1
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
