// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Exercise the actual morpc connection/session lifetime, including the server's
// asynchronous request dispatch. Unit fake channels cannot establish that a
// forced client backend close really cancels the independent server owner.
func TestStatementMemoryRealControlTransport(t *testing.T) {
	for _, terminal := range []string{"end", "disconnect"} {
		t.Run(terminal, func(t *testing.T) {
			service := t.Name()
			rt := runtime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
			runtime.SetupServiceBasedRuntime(service, rt)
			throttler := rscthrottler.NewMemThrottler(service, .9, rscthrottler.WithConstLimit(1<<30))
			rt.SetGlobalVariables(runtime.CNMemoryThrottler, throttler)
			addr := "unix://" + filepath.Join("/tmp", "mo-memory-"+uuid.NewString()+".sock")
			codec := morpc.NewMessageCodec(service, func() morpc.Message { return &pipeline.Message{} })
			server, err := morpc.NewRPCServer(service, addr, codec, morpc.WithServerDisableAutoCancelContext())
			require.NoError(t, err)
			done := make(chan error, 4)
			server.RegisterRequestHandler(func(ctx context.Context, request morpc.RPCMessage, _ uint64, cs morpc.ClientSession) error {
				go func() {
					defer request.Cancel()
					_, err := handleMemoryControl(ctx, service, request.Message.(*pipeline.Message), cs, func() morpc.Message { return &pipeline.Message{} })
					done <- err
				}()
				return nil
			})
			require.NoError(t, server.Start())
			t.Cleanup(func() { require.NoError(t, server.Close()) })
			client, err := cnclient.NewPipelineClient(service, "", &cnclient.PipelineConfig{MaxSenderNumber: 1})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			rt.SetGlobalVariables(runtime.PipelineClient, client)
			// morpc retires disconnected server sessions on its shared 10s
			// sweep; the independent owner must survive until that real event.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			manifest := MemoryManifest{Version: query.MemoryAdmissionProtocolVersion, TargetUnit: 1 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{{Role: MemoryStageOutput, Count: 1, Schema: []types.Type{types.T_int64.ToType()}, TableCount: 1}}}
			handle, err := openRemoteMemory(ctx, service, addr, manifest, runtime.ProcessInstanceID())
			require.NoError(t, err)
			t.Cleanup(func() { handle.close(context.Canceled) })
			require.NoError(t, handle.commit(ctx, handle.unit))
			require.NoError(t, <-done, "commit handler completes while open handler remains alive")
			_, scopeCancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { scopeCancel(nil) })
			_, detach, err := AttachStatementMemory(service, handle.token, message.NewMessageBoard(), scopeCancel)
			require.NoError(t, err)
			t.Cleanup(detach)
			if terminal == "end" {
				detach()
				handle.close(nil)
			} else {
				require.NoError(t, handle.stream.Close(true))
				handle.stream = nil
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("real connection terminal did not retire its server owner")
			}
			_, err = findStatementMemory(service, handle.token)
			require.Error(t, err)
		})
	}
}
