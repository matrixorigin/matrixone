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
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	rm, _ := NewRoutineManager(ctx, "")
	setRtMgr("", rm)
	sv := MOServer{
		rm: rm,
	}

	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		panic(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, "", proto, nil)
	proto.ses = ses

	rt := &Routine{}
	rt.protocol.Store(&holder{proto: proto})
	rt.ses = ses

	rm.setRoutine(ioses, 0, rt)

	err = sv.handshake(ioses)
	assert.NoError(t, err)
}
