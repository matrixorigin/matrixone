// Copyright 2024 Matrix Origin
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
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
)

func Test_handshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//before anything using the configuration
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 0
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	rm, _ := NewRoutineManager(ctx, "")
	setRtMgr("", rm)
	sv := MOServer{
		rm: rm,
	}

	tConn := &testConn{
		mod:  testConnModReadBuffer,
		rbuf: makePacket([]byte{0, 0}, 1),
	}

	ioses, err := NewIOSession(tConn, pu, "")
	if err != nil {
		panic(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, "", proto, nil)
	proto.ses = ses

	rt := &Routine{}
	rt.protocol.Store(&holder[MysqlRrWr]{value: proto})
	rt.ses = ses

	rm.setRoutine(ioses, 0, rt)

	err = sv.handshake(ioses)
	assert.Error(t, err)

	////SSL handshake
	data := gIO.AppendUint32(nil, DefaultCapability|CLIENT_SSL) //capability
	data = gIO.AppendUint32(data, MaxPayloadSize)               //payload size
	data = gIO.AppendUint8(data, 1)                             //collationid
	data = append(data, make([]byte, 23)...)
	tConn.rbuf = makePacket(data, 1)
	err = sv.handshake(ioses)
	assert.Error(t, err)

	////no SSL handshake
	data = gIO.AppendUint32(nil, DefaultCapability) //capability
	data = gIO.AppendUint32(data, MaxPayloadSize)   //payload size
	data = gIO.AppendUint8(data, 1)                 //collationid
	data = append(data, make([]byte, 23)...)
	data = append(data, []byte("abc")...) //user name
	data = append(data, 0)

	lenencBuffer := make([]byte, 9)
	l := proto.writeIntLenEnc(lenencBuffer, 0, 3)
	data = append(data, lenencBuffer[:l]...) //password length
	data = append(data, []byte("111")...)    //password

	data = append(data, []byte("db")...) //db name
	data = append(data, 0)

	data = append(data, []byte(AuthNativePassword)...) //plugin
	data = append(data, 0)

	l = proto.writeIntLenEnc(lenencBuffer, 0, 0)
	data = append(data, lenencBuffer[:l]...) //connect attrs

	tConn.rbuf = makePacket(data, 1)
	err = sv.handshake(ioses)
	assert.NoError(t, err)
}
