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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

		cpi := &MysqlProtocolImpl{}

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setSessionAlloc("", NewLeakCheckAllocator())
		setPu("", pu)
		io, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		cpi.tcpConn = io

		str1 := cpi.Peer()
		convey.So(str1, convey.ShouldEqual, "test addr")
	})
}

func Test_SendResponse(t *testing.T) {
	ctx := context.TODO()
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
		setSessionAlloc("", NewLeakCheckAllocator())
		setPu("", pu)
		rawConn := &testConn{}
		ioses, err := NewIOSession(rawConn, pu, "")
		convey.ShouldBeNil(err)
		mp := &MysqlProtocolImpl{}
		mp.io = iopackage
		mp.tcpConn = ioses
		mp.capability = CLIENT_PROTOCOL_41
		resp := &Response{}
		resp.category = EoFResponse
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)

		resp.SetData(moerr.NewInternalError(context.TODO(), ""))
		resp.category = ErrorResponse
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)

		rawConn.data = nil
		resp.SetData(errors.Join(moerr.NewInvalidInput(ctx, "bad numeric parameter"), errors.New("cleanup")))
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)
		packets := splitProtocolPackets(t, rawConn.data)
		convey.So(len(packets), convey.ShouldEqual, 1)
		convey.So(binary.LittleEndian.Uint16(packets[0][1:]), convey.ShouldEqual, moerr.ErrInvalidInput)
		convey.So(string(packets[0][4:9]), convey.ShouldEqual, moerr.MySQLDefaultSqlState)
		convey.So(string(packets[0][9:]), convey.ShouldEqual,
			"invalid input: bad numeric parameter\ncleanup")

		rawConn.data = nil
		resp.SetData(errors.Join(errors.New("rollback failed"),
			moerr.NewInvalidInput(ctx, "bad numeric parameter")))
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)
		packets = splitProtocolPackets(t, rawConn.data)
		convey.So(len(packets), convey.ShouldEqual, 1)
		convey.So(binary.LittleEndian.Uint16(packets[0][1:]), convey.ShouldEqual, moerr.ErrInvalidInput)
		convey.So(string(packets[0][4:9]), convey.ShouldEqual, moerr.MySQLDefaultSqlState)
		convey.So(string(packets[0][9:]), convey.ShouldEqual,
			"rollback failed\ninvalid input: bad numeric parameter")

		rawConn.data = nil
		resp.SetData(fmt.Errorf("execute context: %w", moerr.NewInvalidInput(ctx, "bad numeric parameter")))
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)
		packets = splitProtocolPackets(t, rawConn.data)
		convey.So(len(packets), convey.ShouldEqual, 1)
		convey.So(binary.LittleEndian.Uint16(packets[0][1:]), convey.ShouldEqual, moerr.ErrInvalidInput)
		convey.So(string(packets[0][4:9]), convey.ShouldEqual, moerr.MySQLDefaultSqlState)
		convey.So(string(packets[0][9:]), convey.ShouldEqual,
			"execute context: invalid input: bad numeric parameter")

		driverErr := &mysql.MySQLError{Number: moerr.ErrTxnWWConflict, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "w-w conflict"}
		for _, tc := range []struct {
			name    string
			err     error
			code    uint16
			state   string
			message string
		}{
			{"driver conflict", driverErr, moerr.ErrTxnWWConflict, "HY000", "w-w conflict"},
			{"driver duplicate", &mysql.MySQLError{Number: moerr.ER_DUP_ENTRY, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "duplicate"}, moerr.ER_DUP_ENTRY, "23000", "duplicate"},
			{"missing state", &mysql.MySQLError{Number: moerr.ErrTxnWWConflict, Message: "conflict"}, moerr.ErrTxnWWConflict, "HY000", "conflict"},
			{"wrapped driver error", fmt.Errorf("execute: %w", driverErr), moerr.ER_UNKNOWN_ERROR, "HY000", "execute: Error 20619 (HY000): w-w conflict"},
			{"joined rollback error", errors.Join(driverErr, errors.New("rollback failed")), moerr.ER_UNKNOWN_ERROR, "HY000", "Error 20619 (HY000): w-w conflict\nrollback failed"},
			{"joined commit error", errors.Join(driverErr), moerr.ER_UNKNOWN_ERROR, "HY000", "Error 20619 (HY000): w-w conflict"},
			{"nil driver error", (*mysql.MySQLError)(nil), moerr.ER_UNKNOWN_ERROR, "HY000", "<nil>"},
		} {
			convey.Convey(tc.name, func() {
				rawConn.data = nil
				resp.SetData(tc.err)
				err = mp.SendResponse(ctx, resp)
				convey.So(err, convey.ShouldBeNil)
				packets = splitProtocolPackets(t, rawConn.data)
				convey.So(len(packets), convey.ShouldEqual, 1)
				convey.So(binary.LittleEndian.Uint16(packets[0][1:]), convey.ShouldEqual, tc.code)
				convey.So(string(packets[0][4:9]), convey.ShouldEqual, tc.state)
				convey.So(string(packets[0][9:]), convey.ShouldEqual, tc.message)
			})
		}

		resp.category = -1
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
