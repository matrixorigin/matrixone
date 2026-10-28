// Copyright 2026 Matrix Origin
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

package ctl

import (
	"context"
	"sync"
	"testing"

	uuid2 "github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// readerCtlQueryClient is a test double of the query client boundary: it captures
// the requests handleCtlReader forwards to other CNs without opening sockets. The
// full production forward path is exercised up to the client interface, so the
// remote request shape can be asserted directly from the captured requests.
type readerCtlQueryClient struct {
	mu   sync.Mutex
	sid  string
	sent []readerCtlSend
}

type readerCtlSend struct {
	addr string
	req  *query.Request
}

var _ client.QueryClient = (*readerCtlQueryClient)(nil)

func (c *readerCtlQueryClient) ServiceID() string { return c.sid }

func (c *readerCtlQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	req := &query.Request{}
	req.CmdMethod = method
	return req
}

func (c *readerCtlQueryClient) SendMessage(
	_ context.Context, addr string, req *query.Request,
) (*query.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, readerCtlSend{addr: addr, req: req})
	return &query.Response{
		CtlReaderResponse: &query.CtlReaderResponse{Resp: "remote-echo"},
	}, nil
}

func (c *readerCtlQueryClient) calls() []readerCtlSend {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]readerCtlSend, len(c.sent))
	copy(out, c.sent)
	return out
}

func (c *readerCtlQueryClient) Release(*query.Response) {}

func (c *readerCtlQueryClient) Close() error { return nil }

// newReaderCtlProc builds a process bound to the test cluster registered by
// initRuntime for the "" service, running on the CN identified by sid.
func newReaderCtlProc(t *testing.T, sid string) (*process.Process, *readerCtlQueryClient) {
	t.Helper()
	proc := new(process.Process)
	proc.Base = &process.BaseProcess{}
	cli := &readerCtlQueryClient{sid: sid}
	proc.Base.QueryClient = cli
	return proc, cli
}

// resetReaderEngineFlags starts and ends each reader test from a clean engine
// configuration, so assertions on the global reader sets are never polluted by
// other tests or previous cases.
func resetReaderEngineFlags(t *testing.T) {
	t.Helper()
	engine.SetForceBuildRemoteDS(false, []string{})
	engine.SetForceShuffleReader(false, []string{}, 0)
	t.Cleanup(func() {
		engine.SetForceBuildRemoteDS(false, []string{})
		engine.SetForceShuffleReader(false, []string{}, 0)
	})
}

// TestMoCtlReaderRejectsShortParams pins the contract for malformed reader ctl
// parameters: an empty, single-segment, or two-segment parameter must come back
// as an explicit invalid-input error from the ctl entry instead of panicking on
// the forwarding branch, and it must do so before any cluster action: no
// request is transferred to other CNs and no reader setting is applied locally.
func TestMoCtlReaderRejectsShortParams(t *testing.T) {
	localCN := uuid2.New().String()
	remoteCN := uuid2.New().String()
	initRuntime([]string{localCN, remoteCN}, []string{"local-addr", "remote-addr"})
	proc, cli := newReaderCtlProc(t, localCN)
	resetReaderEngineFlags(t)

	// a non-cn service is refused before any parameter parsing.
	res, err := handleCtlReader(proc, tn, "enable:force_build_remote_ds:1,2", MoCtlTNCmdSender)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only cn supported")
	require.Equal(t, Result{}, res)
	require.Zero(t, len(cli.calls()))

	for _, tc := range []struct{ name, param string }{
		{"empty", ""},
		{"single segment", "lkt.t"},
		{"two segments", "a:b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := handleCtlReader(proc, cn, tc.param, MoCtlTNCmdSender)

			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
				"want an explicit invalid-input error, got %v", err)
			require.Equal(t, Result{}, res)

			// malformed input must not trigger any cluster action.
			require.Zero(t, len(cli.calls()),
				"no transfer request may leave this CN for a malformed parameter")

			force, tbls := engine.GetForceBuildRemoteDS()
			require.False(t, force)
			require.Empty(t, tbls)
			force, tbls, blkCnt := engine.GetForceShuffleReader()
			require.False(t, force)
			require.Empty(t, tbls)
			require.Zero(t, blkCnt)
		})
	}
}

// TestMoCtlReaderAppliesValidParams covers the documented shapes of the reader
// ctl command on the local CN: the three-part force_build_remote_ds form, the
// four-part force_shuffle form with block count, disabling, case-insensitive
// commands, and the graceful string reports for shape-valid but unsupported
// commands, configurations, and block counts.
func TestMoCtlReaderAppliesValidParams(t *testing.T) {
	localCN := uuid2.New().String()
	initRuntime([]string{localCN}, []string{"local-addr"})
	proc, cli := newReaderCtlProc(t, localCN)
	resetReaderEngineFlags(t)

	// three-part form applies on the current CN and reports success.
	res, err := handleCtlReader(proc, cn, "enable:force_build_remote_ds:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	require.Equal(t, CtlReaderMethod, res.Method)
	force, tbls := engine.GetForceBuildRemoteDS()
	require.True(t, force)
	require.Equal(t, []uint64{1, 2}, tbls)
	require.Contains(t, res.Data.(string), "successed cmd: enable")
	require.Zero(t, len(cli.calls()))

	// four-part form carries the block count.
	res, err = handleCtlReader(proc, cn, "enable:force_shuffle:3,4:8", MoCtlTNCmdSender)
	require.NoError(t, err)
	force, tbls, blkCnt := engine.GetForceShuffleReader()
	require.True(t, force)
	require.Equal(t, []uint64{3, 4}, tbls)
	require.Equal(t, 8, blkCnt)
	require.Contains(t, res.Data.(string), "successed cmd: enable")

	// disable reports the mutation too.
	res, err = handleCtlReader(proc, cn, "disable:force_build_remote_ds:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	force, _ = engine.GetForceBuildRemoteDS()
	require.False(t, force)
	require.Contains(t, res.Data.(string), "successed cmd: disable")

	// commands and configurations are case-insensitive.
	res, err = handleCtlReader(proc, cn, "ENABLE:FORCE_BUILD_REMOTE_DS:5", MoCtlTNCmdSender)
	require.NoError(t, err)
	force, tbls = engine.GetForceBuildRemoteDS()
	require.True(t, force)
	require.Equal(t, []uint64{5}, tbls)

	// shape-valid but unsupported input keeps returning a report, not an error.
	res, err = handleCtlReader(proc, cn, "foo:force_build_remote_ds:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	require.Contains(t, res.Data.(string), "not support cmd: foo")

	res, err = handleCtlReader(proc, cn, "enable:unknown_cfg:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	require.Contains(t, res.Data.(string), "not support cfg")

	res, err = handleCtlReader(proc, cn, "enable:force_shuffle:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	require.Contains(t, res.Data.(string), "not support cfg")

	res, err = handleCtlReader(proc, cn, "enable:force_shuffle:1,2:abc", MoCtlTNCmdSender)
	require.NoError(t, err)
	require.Contains(t, res.Data.(string), "parametrer err")
}

// TestMoCtlReaderForwardsWellFormedParams covers the distributed path: on a
// two-CN cluster the current CN applies the command locally and forwards the
// very same shape to the other CN as a CtlReader request, with the command,
// configuration, and table-list-plus-block-count fields preserved.
func TestMoCtlReaderForwardsWellFormedParams(t *testing.T) {
	localCN := uuid2.New().String()
	remoteCN := uuid2.New().String()
	remoteAddr := "remote-addr"
	initRuntime([]string{localCN, remoteCN}, []string{"local-addr", remoteAddr})
	proc, cli := newReaderCtlProc(t, localCN)
	resetReaderEngineFlags(t)

	res, err := handleCtlReader(proc, cn, "enable:force_build_remote_ds:1,2", MoCtlTNCmdSender)
	require.NoError(t, err)
	data := res.Data.(string)

	// the local CN applied the command
	force, tbls := engine.GetForceBuildRemoteDS()
	require.True(t, force)
	require.Equal(t, []uint64{1, 2}, tbls)
	require.Contains(t, data, localCN+":successed cmd: enable")
	// and the remote CN's reply from the forwarded request is part of the report
	require.Contains(t, data, remoteCN+":remote-echo")

	sends := cli.calls()
	require.Equal(t, 1, len(sends))
	require.Equal(t, remoteAddr, sends[0].addr)
	require.Equal(t, query.CmdMethod_CtlReader, sends[0].req.CmdMethod)
	require.NotNil(t, sends[0].req.CtlReaderRequest)
	require.Equal(t, "enable", sends[0].req.CtlReaderRequest.Cmd)
	require.Equal(t, "force_build_remote_ds", sends[0].req.CtlReaderRequest.Cfg)
	decoded := types.DecodeStringSlice(sends[0].req.CtlReaderRequest.Extra)
	require.Equal(t, 1, len(decoded))
	require.Equal(t, "1,2", decoded[0])

	// four-part form: tables and block count travel as one extra field, so the
	// remote CN's UpdateCurrentCNReader receives cmd + cfg + tables + block count.
	res, err = handleCtlReader(proc, cn, "disable:force_shuffle:3,4:8", MoCtlTNCmdSender)
	require.NoError(t, err)
	sends = cli.calls()
	require.Equal(t, 2, len(sends))
	require.Equal(t, "disable", sends[1].req.CtlReaderRequest.Cmd)
	require.Equal(t, "force_shuffle", sends[1].req.CtlReaderRequest.Cfg)
	decoded = types.DecodeStringSlice(sends[1].req.CtlReaderRequest.Extra)
	require.Equal(t, 1, len(decoded))
	require.Equal(t, "3,4:8", decoded[0])
}
