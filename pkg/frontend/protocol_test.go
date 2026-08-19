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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"

	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type observedCloseProtocol struct {
	*MysqlProtocolImpl
	closeStarted chan struct{}
	closeOnce    sync.Once
}

func (p *observedCloseProtocol) Close() {
	p.closeOnce.Do(func() {
		close(p.closeStarted)
	})
	p.MysqlProtocolImpl.Close()
}

type blockedCanceledError struct {
	checkStarted chan struct{}
	checkRelease chan struct{}
	checkOnce    sync.Once
}

func (e *blockedCanceledError) Error() string {
	return context.Canceled.Error()
}

func (e *blockedCanceledError) Is(target error) bool {
	if target != context.Canceled {
		return false
	}
	e.checkOnce.Do(func() {
		close(e.checkStarted)
	})
	<-e.checkRelease
	return true
}

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

		rawConn.data = nil
		resp.SetData(moerr.NewBadFieldErrorf(ctx, "invalid input: column %s does not exist", "metric"))
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldBeNil)
		packets = splitProtocolPackets(t, rawConn.data)
		convey.So(len(packets), convey.ShouldEqual, 1)
		convey.So(binary.LittleEndian.Uint16(packets[0][1:]), convey.ShouldEqual, moerr.ER_BAD_FIELD_ERROR)
		convey.So(string(packets[0][4:9]), convey.ShouldEqual, "42S22")
		convey.So(string(packets[0][9:]), convey.ShouldEqual, "invalid input: column metric does not exist")

		resp.category = -1
		err = mp.SendResponse(ctx, resp)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSendResponseServerShutdown(t *testing.T) {
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Fatal(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	rawConn := &testConn{}
	ioses, err := NewIOSessionWithOptions(
		rawConn,
		pu,
		"",
		WithIOSessionAllocator(NewLeakCheckAllocator()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ioses.Close(); err != nil {
			t.Errorf("close IO session: %v", err)
		}
	})

	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	ses := &Session{}
	ses.setRoutineManager(&RoutineManager{ctx: serverCtx})
	mp := &MysqlProtocolImpl{
		io:         NewIOPackage(true),
		tcpConn:    ioses,
		capability: CLIENT_PROTOCOL_41,
		ses:        ses,
	}
	resp := NewGeneralErrorResponse(COM_QUERY, 0, context.Canceled)

	// A query canceled while the service is still running keeps the existing
	// generic error classification.
	if err = mp.SendResponse(serverCtx, resp); err != nil {
		t.Fatal(err)
	}
	packets := splitProtocolPackets(t, rawConn.data)
	if len(packets) != 1 {
		t.Fatalf("expected one error packet, got %d", len(packets))
	}
	if code := binary.LittleEndian.Uint16(packets[0][1:]); code != moerr.ER_UNKNOWN_ERROR {
		t.Fatalf("expected error code %d, got %d", moerr.ER_UNKNOWN_ERROR, code)
	}
	if state := string(packets[0][4:9]); state != DefaultMySQLState {
		t.Fatalf("expected SQLSTATE %s, got %s", DefaultMySQLState, state)
	}

	// Once the service context is canceled, the same execution error denotes a
	// connection interruption and must use MySQL's shutdown SQLSTATE.
	rawConn.data = nil
	stopServer()
	if err = mp.SendResponse(serverCtx, resp); err != nil {
		t.Fatal(err)
	}
	packets = splitProtocolPackets(t, rawConn.data)
	if len(packets) != 1 {
		t.Fatalf("expected one error packet, got %d", len(packets))
	}
	shutdown := moerr.MysqlErrorMsgRefer[moerr.ER_SERVER_SHUTDOWN]
	if code := binary.LittleEndian.Uint16(packets[0][1:]); code != shutdown.ErrorCode {
		t.Fatalf("expected error code %d, got %d", shutdown.ErrorCode, code)
	}
	if state := string(packets[0][4:9]); state != shutdown.SqlStates[0] {
		t.Fatalf("expected SQLSTATE %s, got %s", shutdown.SqlStates[0], state)
	}
	if msg := string(packets[0][9:]); msg != shutdown.ErrorMsgOrFormat {
		t.Fatalf("expected error message %q, got %q", shutdown.ErrorMsgOrFormat, msg)
	}
}

func TestSendResponseConcurrentSessionClose(t *testing.T) {
	const service = "send-response-concurrent-close"
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Fatal(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	InitServerLevelVars(service)
	setPu(service, pu)

	rawConn := &testConn{}
	allocator := NewLeakCheckAllocator()
	ioses, err := NewIOSessionWithOptions(
		rawConn,
		pu,
		service,
		WithIOSessionAllocator(allocator),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ioses.Close(); err != nil {
			t.Errorf("close IO session: %v", err)
		}
		if !allocator.CheckBalance() {
			t.Error("IO session allocator is unbalanced")
		}
	})
	mp := NewMysqlClientProtocol(service, 1, ioses, 1024, sv)
	closeStarted := make(chan struct{})
	protocol := &observedCloseProtocol{
		MysqlProtocolImpl: mp,
		closeStarted:      closeStarted,
	}

	serverCtx, stopServer := context.WithCancel(context.Background())
	stopServer()
	ses := NewSession(serverCtx, service, protocol, nil)
	ses.setRoutineManager(&RoutineManager{ctx: serverCtx})
	mp.SetSession(ses)

	checkRelease := make(chan struct{})
	var releaseCheck sync.Once
	release := func() {
		releaseCheck.Do(func() {
			close(checkRelease)
		})
	}
	t.Cleanup(release)
	blockedErr := &blockedCanceledError{
		checkStarted: make(chan struct{}),
		checkRelease: checkRelease,
	}

	responseDone := make(chan error, 1)
	go func() {
		responseDone <- mp.SendResponse(
			serverCtx,
			NewGeneralErrorResponse(COM_QUERY, 0, blockedErr),
		)
	}()

	select {
	case <-blockedErr.checkStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("response did not reach shutdown classification")
	}

	closeDone := make(chan struct{})
	go func() {
		ses.Close()
		close(closeDone)
	}()

	select {
	case <-closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("session close did not reach protocol close")
	}
	// Session.Close now owns Session.mu. Let the response inspect shutdown state;
	// it must not retain the protocol lock while waiting for the session lock.
	release()

	select {
	case err = <-responseDone:
		if err != nil {
			t.Fatalf("emit response: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("response emission deadlocked with session close")
	}
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("session close deadlocked with response emission")
	}

	packets := splitProtocolPackets(t, rawConn.data)
	if len(packets) != 1 {
		t.Fatalf("expected one error packet, got %d", len(packets))
	}
	shutdown := moerr.MysqlErrorMsgRefer[moerr.ER_SERVER_SHUTDOWN]
	if code := binary.LittleEndian.Uint16(packets[0][1:]); code != shutdown.ErrorCode {
		t.Fatalf("expected error code %d after close, got %d", shutdown.ErrorCode, code)
	}
	if state := string(packets[0][4:9]); state != shutdown.SqlStates[0] {
		t.Fatalf("expected SQLSTATE %s after close, got %s", shutdown.SqlStates[0], state)
	}
}
