package ctl

import (
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHandleGetProtocolVersion(t *testing.T) {
	var arguments struct {
		proc    *process.Process
		service serviceType
	}

	trace.InitMOCtledSpan()

	id := uuid.New().String()
	addr := "127.0.0.1:7777"
	initRuntime([]string{id}, []string{addr})
	qs, err := queryservice.NewQueryService(id, addr, morpc.Config{})
	require.NoError(t, err)

	arguments.proc = new(process.Process)
	arguments.proc.QueryService = qs
	arguments.service = cn

	err = qs.Start()
	require.NoError(t, err)

	defer func() {
		qs.Close()
	}()

	version := map[string]int64{
		id: defines.MORPCLatestVersion,
	}
	bytes, err := json.Marshal(version)
	require.NoError(t, err)

	ret, err := handleGetProtocolVersion(arguments.proc, arguments.service, "", nil)
	require.NoError(t, err)
	require.Equal(t, ret, Result{
		Method: GetProtocolVersionMethod,
		Data:   string(bytes),
	})
}

func TestHandleSetProtocolVersion(t *testing.T) {
	trace.InitMOCtledSpan()
	proc := new(process.Process)
	id := uuid.New().String()
	addr := "127.0.0.1:7777"
	initRuntime([]string{id}, []string{addr})
	requireVersionValue(t, 1)
	qs, err := queryservice.NewQueryService(id, addr, morpc.Config{})
	require.NoError(t, err)
	proc.QueryService = qs

	cases := []struct {
		service serviceType
		version int64

		expectedErr error
	}{
		{service: cn, version: 1},
		{service: cn, version: 2},
		{service: tn, version: 1, expectedErr: moerr.NewInternalError(proc.Ctx, "no such tn service")},
	}

	err = qs.Start()
	require.NoError(t, err)
	defer func() {
		qs.Close()
	}()

	for _, c := range cases {
		bytes, err := json.Marshal(map[string]int64{id: c.version})
		require.NoError(t, err)
		var parameter string
		if c.service == tn {
			parameter = fmt.Sprintf("%d", c.version)
		} else {
			parameter = fmt.Sprintf("%s:%d", id, c.version)
		}

		ret, err := handleSetProtocolVersion(proc, c.service, parameter, nil)
		if c.expectedErr != nil {
			require.Equal(t, c.expectedErr, err)
			continue
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ret, Result{
			Method: SetProtocolVersionMethod,
			Data:   string(bytes),
		})
		requireVersionValue(t, c.version)
	}
}

func requireVersionValue(t *testing.T, version int64) {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.MOProtocolVersion)
	require.True(t, ok)
	require.EqualValues(t, version, v)
}
