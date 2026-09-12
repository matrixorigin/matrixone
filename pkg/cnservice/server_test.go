// Copyright 2023 Matrix Origin
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

package cnservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type closeErrorMOServer struct {
	err error
}

func (s closeErrorMOServer) GetRoutineManager() *frontend.RoutineManager {
	return nil
}

func (s closeErrorMOServer) Start() error {
	return nil
}

func (s closeErrorMOServer) Stop() error {
	return s.err
}

type listeningMOServer struct {
	listener net.Listener
}

func (s *listeningMOServer) GetRoutineManager() *frontend.RoutineManager {
	return nil
}

func (s *listeningMOServer) Start() error {
	return nil
}

func (s *listeningMOServer) Stop() error {
	return s.listener.Close()
}

type closeOnlyRPCServer struct {
	closeErr error
	onClose  func()
}

func (s closeOnlyRPCServer) Start() error {
	return nil
}

func (s closeOnlyRPCServer) Close() error {
	if s.onClose != nil {
		s.onClose()
	}
	return s.closeErr
}

type closeRecordingTraceService struct {
	trace.Service
	closed chan struct{}
}

func (s *closeRecordingTraceService) Close() {
	close(s.closed)
	s.Service.Close()
}

type closeOnlyIncrService struct {
	incrservice.AutoIncrementService
	onClose  func()
	onReload func(context.Context, uint64) error
}

func (s closeOnlyIncrService) Close() {
	s.onClose()
}

func (s closeOnlyIncrService) Reload(ctx context.Context, tableID uint64) error {
	return s.onReload(ctx, tableID)
}

type closeOnlyTxnClient struct {
	client.TxnClient
	onClose func() error
}

func (c closeOnlyTxnClient) Close() error {
	return c.onClose()
}

type closeRecordingQueryService struct {
	queryservice.QueryService
	handlers map[querypb.CmdMethod]func(context.Context, *querypb.Request, *querypb.Response, *morpc.Buffer) error
	closed   chan struct{}
}

func (s *closeRecordingQueryService) AddHandleFunc(
	method querypb.CmdMethod,
	handler func(context.Context, *querypb.Request, *querypb.Response, *morpc.Buffer) error,
	_ bool,
) {
	s.handlers[method] = handler
}

func (s *closeRecordingQueryService) Close() error {
	close(s.closed)
	return nil
}

func (s closeOnlyRPCServer) RegisterRequestHandler(
	func(context.Context, morpc.RPCMessage, uint64, morpc.ClientSession) error,
) {
}

func TestCloseCNServiceStepsAttemptsAllAndAggregatesErrors(t *testing.T) {
	bootstrapErr := errors.New("bootstrap close failed")
	gossipErr := errors.New("gossip leave failed")

	var calls []string
	steps := []struct {
		name string
		err  error
	}{
		{name: "bootstrap", err: bootstrapErr},
		{name: "frontend"},
		{name: "task"},
		{name: "rpcs"},
		{name: "io-pipeline"},
		{name: "gossip", err: gossipErr},
		{name: "pipeline-server"},
		{name: "lock-service"},
		{name: "shard-service"},
		{name: "pipeline-client"},
	}

	closeSteps := make([]func() error, 0, len(steps))
	for _, step := range steps {
		closeSteps = append(closeSteps, func() error {
			calls = append(calls, step.name)
			return step.err
		})
	}

	err := closeCNServiceSteps(closeSteps...)
	require.ErrorIs(t, err, bootstrapErr)
	require.ErrorIs(t, err, gossipErr)
	require.Equal(t, []string{
		"bootstrap",
		"frontend",
		"task",
		"rpcs",
		"io-pipeline",
		"gossip",
		"pipeline-server",
		"lock-service",
		"shard-service",
		"pipeline-client",
	}, calls)
}

func TestServiceCloseDoesNotHangOnNeverReadyClusterAfterEarlyError(t *testing.T) {
	moruntime.RunTest(
		t.Name(),
		func(rt moruntime.Runtime) {
			frontendErr := errors.New("frontend close failed")
			refreshErr := errors.New("hakeeper refresh failed")
			hc := &testHAKClient{clusterErr: refreshErr}
			moCluster := clusterservice.NewMOCluster(t.Name(), hc, time.Hour)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ls := mock_lock.NewMockLockService(ctrl)
			ls.EXPECT().Close().Return(nil).Times(2)
			sv := &service{
				cfg:                &Config{UUID: t.Name()},
				logger:             zap.NewNop(),
				stopper:            stopper.NewStopper("test-cn-close"),
				bootstrapService:   &testBootService{},
				mo:                 closeErrorMOServer{err: frontendErr},
				cancelMoServerFunc: func() {},
				_hakeeperClient:    hc,
				moCluster:          moCluster,
				server:             closeOnlyRPCServer{},
				lockService:        ls,
			}

			done := make(chan error, 1)
			go func() {
				done <- sv.Close()
			}()

			select {
			case err := <-done:
				require.ErrorIs(t, err, frontendErr)
				require.Equal(t, 1, hc.closed)
			case <-time.After(time.Second):
				t.Fatal("service.Close blocked on never-ready cluster")
			}
		},
	)
}

func TestMakeRSSCacheEvictorEvictsMemoryCacheOnly(t *testing.T) {
	oldMemoryEvictor := evictMemoryCachesToCapacityPercent
	defer func() {
		evictMemoryCachesToCapacityPercent = oldMemoryEvictor
	}()

	memoryStarted := make(chan struct{})
	releaseMemory := make(chan struct{})

	evictMemoryCachesToCapacityPercent = func(ctx context.Context, targetPercent int64) map[string]int64 {
		close(memoryStarted)
		select {
		case <-releaseMemory:
		case <-ctx.Done():
		}
		return nil
	}

	done := make(chan struct{})
	go func() {
		makeRSSCacheEvictor(time.Second)(context.Background(), 50)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-memoryStarted:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	close(releaseMemory)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func Test_InitServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := &Config{
		UUID:     "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf",
		PortBase: 18000,
	}

	srv := &service{
		metadata: metadata.CNStore{
			UUID: cfg.UUID,
		},
		cfg: cfg,
		responsePool: &sync.Pool{
			New: func() any {
				return &pipeline.Message{}
			},
		},
		addressMgr: address.NewAddressManager(cfg.ServiceHost, cfg.PortBase),
	}
	srv.addressMgr.Register(0)

	WithTaskStorageFactory(nil)(srv)
	handler := func(
		ctx context.Context,
		cnAddr string,
		message morpc.Message,
		cs morpc.ClientSession,
		engine engine.Engine,
		fs fileservice.FileService,
		lockService lockservice.LockService,
		queryClient qclient.QueryClient,
		hakeeper logservice.CNHAKeeperClient,
		udfService udf.Service,
		cli client.TxnClient,
		aicm *defines.AutoIncrCacheManager,
		mAcquirer func() morpc.Message) error {
		return nil
	}
	WithMessageHandle(handler)(srv)

	require.Equal(t, srv.ID(), cfg.UUID)
	require.Equal(t, srv.SQLAddress(), cfg.SQLAddress)

	msg := &pipeline.Message{}

	srv.releaseMessage(msg)
	message := srv.acquireMessage()
	require.Equal(t, message.(*pipeline.Message).Sid, msg.Sid)

	var err error
	ctx := context.TODO()
	session := mock_morpc.NewMockClientSession(ctrl)
	msg.Cmd = pipeline.Method_PipelineMessage
	session.EXPECT().CreateCacheWithCancel(gomock.Any(), uint64(0), gomock.Any()).Return(&testMessageCache{}, nil)
	session.EXPECT().CreateCache(gomock.Any(), uint64(0)).Return(&testMessageCache{}, nil)
	session.EXPECT().DeleteCache(uint64(0)).Times(1)

	msg.Sid = pipeline.Status_WaitingNext
	err = srv.handleRequest(
		ctx,
		morpc.RPCMessage{
			Ctx:     ctx,
			Cancel:  func() {},
			Message: msg,
		},
		0,
		session,
	)
	require.Nil(t, err)

	msg.Sid = pipeline.Status_Last
	err = srv.handleRequest(
		ctx,
		morpc.RPCMessage{
			Ctx:     ctx,
			Cancel:  func() {},
			Message: msg,
		},
		0,
		session,
	)
	require.Nil(t, err)
}

type testMessageCache struct {
	cache []morpc.Message
}

func TestHandleAssemblePipelineDeletesCacheAndRejectsMixedNegotiation(t *testing.T) {
	t.Run("assembles and deletes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		session := mock_morpc.NewMockClientSession(ctrl)
		ctx := context.Background()
		cache := &testMessageCache{cache: []morpc.Message{
			&pipeline.Message{Id: 42, Cmd: pipeline.Method_PipelineMessage, Data: []byte("a"), RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck},
			&pipeline.Message{Id: 42, Cmd: pipeline.Method_PipelineMessage, Data: []byte("b"), RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck},
		}}
		session.EXPECT().CreateCache(ctx, uint64(42)).Return(cache, nil)
		session.EXPECT().DeleteCache(uint64(42))
		final := &pipeline.Message{Id: 42, Cmd: pipeline.Method_PipelineMessage, Data: []byte("c"), RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
		require.NoError(t, handleAssemblePipeline(ctx, final, session))
		require.Equal(t, []byte("abc"), final.GetData())
	})

	t.Run("mixed teardown mode is protocol error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		session := mock_morpc.NewMockClientSession(ctrl)
		ctx := context.Background()
		cache := &testMessageCache{cache: []morpc.Message{
			&pipeline.Message{Id: 43, Cmd: pipeline.Method_PipelineMessage, RequestedTeardownMode: pipeline.StreamTeardownMode_LegacyClose},
		}}
		session.EXPECT().CreateCache(ctx, uint64(43)).Return(cache, nil)
		session.EXPECT().DeleteCache(uint64(43))
		final := &pipeline.Message{Id: 43, Cmd: pipeline.Method_PipelineMessage, RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck}
		require.Error(t, handleAssemblePipeline(ctx, final, session))
	})

	for _, tt := range []struct {
		name     string
		fragment *pipeline.Message
		final    *pipeline.Message
	}{
		{
			name: "mixed batch count credit is protocol error",
			fragment: &pipeline.Message{Id: 44, Cmd: pipeline.Method_PipelineMessage,
				RequestedTeardownMode:     pipeline.StreamTeardownMode_FinishAck,
				RequestedBatchCreditCount: 7, RequestedBatchCreditBytes: 1024},
			final: &pipeline.Message{Id: 44, Cmd: pipeline.Method_PipelineMessage,
				RequestedTeardownMode:     pipeline.StreamTeardownMode_FinishAck,
				RequestedBatchCreditCount: 8, RequestedBatchCreditBytes: 1024},
		},
		{
			name: "mixed batch byte credit is protocol error",
			fragment: &pipeline.Message{Id: 45, Cmd: pipeline.Method_PipelineMessage,
				RequestedTeardownMode:     pipeline.StreamTeardownMode_FinishAck,
				RequestedBatchCreditCount: 8, RequestedBatchCreditBytes: 1024},
			final: &pipeline.Message{Id: 45, Cmd: pipeline.Method_PipelineMessage,
				RequestedTeardownMode:     pipeline.StreamTeardownMode_FinishAck,
				RequestedBatchCreditCount: 8, RequestedBatchCreditBytes: 2048},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			session := mock_morpc.NewMockClientSession(ctrl)
			ctx := context.Background()
			cache := &testMessageCache{cache: []morpc.Message{tt.fragment}}
			session.EXPECT().CreateCache(ctx, tt.final.GetID()).Return(cache, nil)
			session.EXPECT().DeleteCache(tt.final.GetID())
			require.Error(t, handleAssemblePipeline(ctx, tt.final, session))
		})
	}
}

func (c *testMessageCache) Add(val morpc.Message) error {
	c.cache = append(c.cache, val)
	return nil
}

func (c *testMessageCache) Len() (int, error) {
	return len(c.cache), nil
}

func (c *testMessageCache) Pop() (morpc.Message, bool, error) {
	if len(c.cache) == 0 {
		return nil, false, nil
	}
	ret := c.cache[0]
	c.cache = c.cache[1:]
	return ret, true, nil
}

func (c *testMessageCache) Close() {
}

var _ bootstrap.Service = new(testBootService)

type testBootService struct {
	choice               int
	closeCount           int
	closeErr             error
	bootstrapErr         error
	bootstrapCount       atomic.Int32
	bootstrapHook        func()
	bootstrapUpgradeHook func(context.Context) error
	maybeUpgrade         func()
}

func (boot *testBootService) Bootstrap(ctx context.Context) error {
	boot.bootstrapCount.Add(1)
	if boot.bootstrapHook != nil {
		boot.bootstrapHook()
	}
	return boot.bootstrapErr
}

func (boot *testBootService) BootstrapUpgrade(ctx context.Context) error {
	if boot.bootstrapUpgradeHook != nil {
		return boot.bootstrapUpgradeHook(ctx)
	}
	return nil
}

func (boot *testBootService) MaybeUpgradeTenant(ctx context.Context, tenantFetchFunc func() (int32, string, error), txnOp client.TxnOperator) (bool, error) {
	if boot.maybeUpgrade != nil {
		boot.maybeUpgrade()
	}
	if boot.choice == 1 {
		return false, moerr.NewInternalErrorNoCtx("return_err")
	}
	return true, nil
}

func (boot *testBootService) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) (bool, error) {
	if boot.choice == 1 {
		return false, moerr.NewInternalErrorNoCtx("return_err")
	}
	return true, nil
}

func (boot *testBootService) GetFinalVersion() string {
	return "2.0.0"
}

func (boot *testBootService) GetFinalVersionOffset() int32 {
	//TODO implement me
	panic("implement me")
}

func (boot *testBootService) Close() error {
	boot.closeCount++
	return boot.closeErr
}

func TestServiceStartBootstrapFailureCanBeRolledBack(t *testing.T) {
	moruntime.RunTest(
		t.Name(),
		func(rt moruntime.Runtime) {
			bootstrapErr := errors.New("bootstrap connection reset")
			bootstrapEntered := make(chan struct{})
			releaseBootstrap := make(chan struct{})
			boot := &testBootService{
				bootstrapErr: bootstrapErr,
				bootstrapHook: func() {
					close(bootstrapEntered)
					<-releaseBootstrap
				},
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			listenerAddr := listener.Addr().String()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ls := mock_lock.NewMockLockService(ctrl)
			ls.EXPECT().Close().Return(nil).Times(2)
			cfg := &Config{UUID: t.Name()}
			cfg.Txn.Trace.BufferSize = 1
			s := &service{
				cfg:                cfg,
				logger:             zap.NewNop(),
				stopper:            stopper.NewStopper("test-bootstrap-failure"),
				bootstrapService:   boot,
				mo:                 &listeningMOServer{listener: listener},
				cancelMoServerFunc: func() {},
				server:             closeOnlyRPCServer{},
				lockService:        ls,
			}
			s.options.traceDataPath = t.TempDir()

			stopped := make(chan struct{})
			require.NoError(t, s.stopper.RunTask(func(ctx context.Context) {
				<-ctx.Done()
				close(stopped)
			}))

			startDone := make(chan error, 1)
			go func() {
				startDone <- s.Start()
			}()
			<-bootstrapEntered
			if s.lifecycleMu.TryLock() {
				s.lifecycleMu.Unlock()
				t.Fatal("Start did not own the lifecycle transition")
			}
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- s.Close()
			}()
			close(releaseBootstrap)

			err = <-startDone
			require.ErrorIs(t, err, bootstrapErr)
			require.NoError(t, <-closeDone)
			require.Nil(t, s.incrservice)
			require.Nil(t, s.txnTraceService)
			_, ok := rt.GetGlobalVariables(moruntime.AutoIncrementService)
			require.False(t, ok)
			_, ok = rt.GetGlobalVariables(moruntime.TxnTraceService)
			require.False(t, ok)

			err = s.Start()
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
			require.Equal(t, int32(1), boot.bootstrapCount.Load())
			require.Nil(t, s.incrservice)
			require.Nil(t, s.txnTraceService)
			_, ok = rt.GetGlobalVariables(moruntime.AutoIncrementService)
			require.False(t, ok)
			_, ok = rt.GetGlobalVariables(moruntime.TxnTraceService)
			require.False(t, ok)

			require.NoError(t, s.Close())
			require.Equal(t, 1, boot.closeCount)
			reused, err := net.Listen("tcp", listenerAddr)
			require.NoError(t, err)
			require.NoError(t, reused.Close())
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("rollback did not stop CN tasks")
			}
		},
	)
}

func TestBootstrapRetirementWaitsForTenantUpgradeConsumer(t *testing.T) {
	consumerEntered := make(chan struct{})
	releaseConsumer := make(chan struct{})
	closeAttempted := make(chan struct{})
	boot := &testBootService{
		maybeUpgrade: func() {
			close(consumerEntered)
			<-releaseConsumer
		},
	}
	s := &service{
		bootstrapService: boot,
		beforeBootstrapClose: func() {
			close(closeAttempted)
		},
	}

	upgradeDone := make(chan error, 1)
	go func() {
		upgradeDone <- s.CheckTenantUpgrade(context.Background(), 1)
	}()
	<-consumerEntered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.closeBootstrapService()
	}()
	<-closeAttempted
	select {
	case err := <-closeDone:
		t.Fatalf("bootstrap retired while tenant upgrade was active: %v", err)
	default:
	}

	close(releaseConsumer)
	require.NoError(t, <-upgradeDone)
	require.NoError(t, <-closeDone)
	require.Equal(t, 1, boot.closeCount)
	require.Empty(t, s.GetFinalVersion())
	require.Error(t, s.CheckTenantUpgrade(context.Background(), 1))
}

func TestServiceCloseWaitsForTraceProducers(t *testing.T) {
	moruntime.RunTest(
		t.Name(),
		func(rt moruntime.Runtime) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ls := mock_lock.NewMockLockService(ctrl)
			ls.EXPECT().Close().Return(nil).Times(2)

			traceService, err := trace.NewService(
				t.TempDir(),
				t.Name(),
				nil,
				rt.Clock(),
				nil,
				trace.WithEnable(true, []uint64{1}),
				trace.WithBufferSize(8),
			)
			require.NoError(t, err)
			recordingTrace := &closeRecordingTraceService{
				Service: traceService,
				closed:  make(chan struct{}),
			}
			rt.SetGlobalVariables(moruntime.TxnTraceService, recordingTrace)

			startFinalEvent := make(chan struct{})
			finalEventSubmitted := make(chan struct{})
			s := &service{
				cfg:                &Config{UUID: t.Name()},
				logger:             zap.NewNop(),
				stopper:            stopper.NewStopper("test-trace-close-order"),
				txnTraceService:    recordingTrace,
				mo:                 closeErrorMOServer{},
				cancelMoServerFunc: func() {},
				server: closeOnlyRPCServer{onClose: func() {
					close(startFinalEvent)
				}},
				lockService: ls,
			}

			s.pipelines.wg.Add(1)
			go func() {
				defer s.pipelines.wg.Done()
				<-startFinalEvent
				select {
				case <-recordingTrace.closed:
					t.Error("trace closed before pipeline producer stopped")
				default:
				}
				recordingTrace.ApplyFlush(
					[]byte("txn"),
					1,
					timestamp.Timestamp{PhysicalTime: 1},
					timestamp.Timestamp{PhysicalTime: 2},
					1,
				)
				close(finalEventSubmitted)
			}()

			require.NoError(t, s.Close())
			select {
			case <-finalEventSubmitted:
			default:
				t.Fatal("CN close returned before the final trace event was submitted")
			}
			select {
			case <-recordingTrace.closed:
			default:
				t.Fatal("trace service was not closed")
			}
		},
	)
}

func txnTraceTestDirectoryKey(serviceID string) string {
	digest := sha256.Sum256([]byte(serviceID))
	return "cn-" + hex.EncodeToString(digest[:])
}

func TestInitTxnTraceServiceAcceptsMaximumServiceID(t *testing.T) {
	serviceID := strings.Repeat("é", 63) + "x"
	require.Equal(t, 127, len(serviceID))
	root := t.TempDir()
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	cfg := &Config{UUID: serviceID}
	cfg.Txn.Trace.BufferSize = 8
	cfg.Txn.Trace.Enable = false
	s := &service{cfg: cfg}
	s.options.traceDataPath = root
	t.Cleanup(func() {
		require.NoError(t, s.closeTxnTraceService())
	})

	s.initTxnTraceService()

	key := txnTraceTestDirectoryKey(serviceID)
	require.Len(t, key, len("cn-")+sha256.Size*2)
	require.LessOrEqual(t, len(key), 255)
	require.DirExists(t, filepath.Join(root, key))
}

func TestInitTxnTraceServiceUsesCNOwnedDirectory(t *testing.T) {
	for _, enable := range []bool{false, true} {
		t.Run(fmt.Sprintf("enable=%t", enable), func(t *testing.T) {
			root := t.TempDir()
			ids := []string{"trace-dir-cn-1", "trace-dir-cn-2", "trace-dir-cn-3"}
			services := make([]*service, 0, len(ids))
			markers := make([]string, 0, len(ids))

			newService := func(id string) *service {
				moruntime.SetupServiceBasedRuntime(id, moruntime.DefaultRuntime())
				cfg := &Config{UUID: id}
				cfg.Txn.Trace.BufferSize = 8
				cfg.Txn.Trace.Enable = enable
				s := &service{cfg: cfg}
				s.options.traceDataPath = root
				t.Cleanup(func() {
					require.NoError(t, s.closeTxnTraceService())
				})
				s.initTxnTraceService()
				return s
			}

			for i, id := range ids {
				s := newService(id)
				services = append(services, s)
				for _, marker := range markers {
					require.FileExists(t, marker)
				}

				dir := filepath.Join(root, txnTraceTestDirectoryKey(id))
				require.DirExists(t, dir)
				marker := filepath.Join(dir, fmt.Sprintf("owner-%d", i))
				require.NoError(t, os.WriteFile(marker, []byte(id), 0644))
				markers = append(markers, marker)
			}

			require.NoError(t, services[1].closeTxnTraceService())
			services[1] = newService(ids[1])
			require.FileExists(t, markers[0])
			require.NoFileExists(t, markers[1])
			require.FileExists(t, markers[2])
			require.DirExists(t, filepath.Join(root, txnTraceTestDirectoryKey(ids[1])))
		})
	}
}

func TestInitTxnTraceServiceUsesFilesystemDistinctDirectoryKeys(t *testing.T) {
	testCases := []struct {
		name string
		ids  []string
	}{
		{
			name: "case variants",
			ids:  []string{"trace-dir-cn-alias", "TRACE-DIR-CN-ALIAS"},
		},
		{
			name: "unicode normalization variants",
			ids:  []string{"trace-dir-cn-caf\u00e9", "trace-dir-cn-cafe\u0301"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			services := make([]*service, 0, len(testCase.ids))
			paths := make([]string, 0, len(testCase.ids))
			marker := ""

			for i, id := range testCase.ids {
				path, err := resolveTxnTraceDataPath(root, id)
				require.NoError(t, err)
				require.Equal(
					t,
					filepath.Join(root, txnTraceTestDirectoryKey(id)),
					path,
				)
				paths = append(paths, path)

				moruntime.SetupServiceBasedRuntime(id, moruntime.DefaultRuntime())
				cfg := &Config{UUID: id}
				cfg.Txn.Trace.BufferSize = 8
				s := &service{cfg: cfg}
				s.options.traceDataPath = root
				s.initTxnTraceService()
				services = append(services, s)

				if i == 0 {
					marker = filepath.Join(path, "owner")
					require.NoError(t, os.WriteFile(marker, []byte(id), 0644))
				} else {
					require.FileExists(t, marker)
				}
			}
			t.Cleanup(func() {
				for _, s := range services {
					require.NoError(t, s.closeTxnTraceService())
				}
			})

			require.NotEqual(t, paths[0], paths[1])

			require.NoError(t, services[1].closeTxnTraceService())
			services = services[:1]
			moruntime.SetupServiceBasedRuntime(testCase.ids[1], moruntime.DefaultRuntime())
			cfg := &Config{UUID: testCase.ids[1]}
			cfg.Txn.Trace.BufferSize = 8
			s := &service{cfg: cfg}
			s.options.traceDataPath = root
			s.initTxnTraceService()
			services = append(services, s)

			require.FileExists(t, marker)
			require.DirExists(t, paths[1])
		})
	}
}

func TestInitTxnTraceServiceFailurePreservesSiblingDirectory(t *testing.T) {
	root := t.TempDir()

	newService := func(id string) *service {
		moruntime.SetupServiceBasedRuntime(id, moruntime.DefaultRuntime())
		cfg := &Config{UUID: id}
		cfg.Txn.Trace.BufferSize = 8
		s := &service{cfg: cfg}
		s.options.traceDataPath = root
		t.Cleanup(func() {
			require.NoError(t, s.closeTxnTraceService())
		})
		return s
	}

	first := newService("trace-dir-cn-a")
	first.initTxnTraceService()
	marker := filepath.Join(root, txnTraceTestDirectoryKey(first.cfg.UUID), "owner")
	require.NoError(t, os.WriteFile(marker, []byte(first.cfg.UUID), 0644))

	require.NoError(t, os.Chmod(root, 0555))
	t.Cleanup(func() {
		require.NoError(t, os.Chmod(root, 0755))
	})
	probe := filepath.Join(root, "permission-probe")
	if err := os.WriteFile(probe, []byte("probe"), 0644); err == nil {
		require.NoError(t, os.Remove(probe))
		t.Skip("filesystem does not enforce read-only directory permissions")
	}

	second := newService("trace-dir-cn-b")
	require.Panics(t, second.initTxnTraceService)
	require.FileExists(t, marker)
}

func TestInitTxnTraceServiceRejectsUnsafeServiceID(t *testing.T) {
	testCases := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "dot", id: "."},
		{name: "parent", id: ".."},
		{name: "trace root alias", id: "../trace"},
		{name: "shared service directory", id: "../shared2"},
		{name: "dotted child", id: "./cn"},
		{name: "slash separator", id: "cn/child"},
		{name: "backslash separator", id: `cn\child`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dataDir := t.TempDir()
			traceRoot := filepath.Join(dataDir, "trace")
			targetDir := filepath.Join(traceRoot, testCase.id)
			targetMarker := filepath.Join(targetDir, "target-owner")
			siblingMarker := filepath.Join(dataDir, "sibling", "owner")
			require.NoError(t, os.MkdirAll(targetDir, 0755))
			require.NoError(t, os.MkdirAll(filepath.Dir(siblingMarker), 0755))
			require.NoError(t, os.WriteFile(targetMarker, []byte("target"), 0644))
			require.NoError(t, os.WriteFile(siblingMarker, []byte("sibling"), 0644))

			cfg := &Config{UUID: testCase.id}
			if testCase.id == "" {
				require.Panics(t, func() { _ = cfg.Validate() })
			} else {
				err := cfg.Validate()
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
			}
			cfg.Txn.Trace.BufferSize = 8
			s := &service{cfg: cfg}
			s.options.traceDataPath = traceRoot

			panicValue := func() (value any) {
				defer func() {
					value = recover()
				}()
				s.initTxnTraceService()
				return nil
			}()
			require.NotNil(t, panicValue)
			err, ok := panicValue.(error)
			require.True(t, ok)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
			require.FileExists(t, targetMarker)
			require.FileExists(t, siblingMarker)
		})
	}
}

func TestServiceCloseDrainsAutoIncrementBeforeTxnClient(t *testing.T) {
	moruntime.RunTest(
		t.Name(),
		func(rt moruntime.Runtime) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ls := mock_lock.NewMockLockService(ctrl)
			ls.EXPECT().Close().Return(nil).Times(2)

			incrCloseStarted := make(chan struct{})
			releaseIncrClose := make(chan struct{})
			txnClientClosed := make(chan struct{})
			s := &service{
				cfg:                &Config{UUID: t.Name()},
				logger:             zap.NewNop(),
				stopper:            stopper.NewStopper("test-incr-close-order"),
				mo:                 closeErrorMOServer{},
				cancelMoServerFunc: func() {},
				server:             closeOnlyRPCServer{},
				lockService:        ls,
				incrservice: closeOnlyIncrService{onClose: func() {
					close(incrCloseStarted)
					<-releaseIncrClose
				}},
				_txnClient: closeOnlyTxnClient{onClose: func() error {
					close(txnClientClosed)
					return nil
				}},
			}

			closeDone := make(chan error, 1)
			go func() {
				closeDone <- s.Close()
			}()

			select {
			case <-incrCloseStarted:
			case <-time.After(time.Second):
				t.Fatal("auto-increment service close did not start")
			}
			txnClientClosedEarly := false
			select {
			case <-txnClientClosed:
				txnClientClosedEarly = true
			default:
			}

			close(releaseIncrClose)
			require.NoError(t, <-closeDone)
			require.False(t, txnClientClosedEarly, "transaction client closed before auto-increment service drained")
			select {
			case <-txnClientClosed:
			default:
				t.Fatal("transaction client was not closed")
			}
		},
	)
}

func TestServiceCloseDrainsQueryHandlersBeforeDependencies(t *testing.T) {
	moruntime.RunTest(
		t.Name(),
		func(rt moruntime.Runtime) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ls := mock_lock.NewMockLockService(ctrl)
			ls.EXPECT().Close().Return(nil).Times(2)

			reloadStarted := make(chan struct{})
			releaseReload := make(chan struct{})
			incrServiceClosed := make(chan struct{})
			txnClientClosed := make(chan struct{})
			var reloadCalls atomic.Int32
			queryService := &closeRecordingQueryService{
				handlers: make(map[querypb.CmdMethod]func(context.Context, *querypb.Request, *querypb.Response, *morpc.Buffer) error),
				closed:   make(chan struct{}),
			}
			s := &service{
				cfg:                &Config{UUID: t.Name()},
				logger:             zap.NewNop(),
				stopper:            stopper.NewStopper("test-query-close-order"),
				mo:                 closeErrorMOServer{},
				cancelMoServerFunc: func() {},
				server:             closeOnlyRPCServer{},
				lockService:        ls,
				queryService:       queryService,
				incrservice: closeOnlyIncrService{
					onClose: func() {
						close(incrServiceClosed)
					},
					onReload: func(context.Context, uint64) error {
						if reloadCalls.Add(1) == 1 {
							close(reloadStarted)
							<-releaseReload
						}
						return nil
					},
				},
				_txnClient: closeOnlyTxnClient{onClose: func() error {
					close(txnClientClosed)
					return nil
				}},
			}
			s.initQueryCommandHandler()
			reloadHandler := queryService.handlers[querypb.CmdMethod_ReloadAutoIncrementCache]
			require.NotNil(t, reloadHandler)

			reloadDone := make(chan error, 1)
			go func() {
				reloadDone <- reloadHandler(
					context.Background(),
					&querypb.Request{ReloadAutoIncrementCache: &querypb.ReloadAutoIncrementCacheRequest{TableID: 1}},
					&querypb.Response{},
					nil,
				)
			}()
			<-reloadStarted

			closeDone := make(chan error, 1)
			go func() {
				closeDone <- s.Close()
			}()
			dependencyClosedEarly := false
			select {
			case <-queryService.closed:
			case <-incrServiceClosed:
				dependencyClosedEarly = true
				select {
				case <-queryService.closed:
				case <-time.After(time.Second):
					t.Fatal("query service close did not start")
				}
			case <-time.After(time.Second):
				t.Fatal("query service close did not start")
			}

			lateErr := reloadHandler(
				context.Background(),
				&querypb.Request{ReloadAutoIncrementCache: &querypb.ReloadAutoIncrementCacheRequest{TableID: 2}},
				&querypb.Response{},
				nil,
			)
			lateRequestRejected := moerr.IsMoErrCode(lateErr, moerr.ErrServiceUnavailable)
			reloadCallCount := reloadCalls.Load()
			dependencyClosedWhileActive := false
			select {
			case <-incrServiceClosed:
				dependencyClosedWhileActive = true
			default:
			}

			close(releaseReload)
			require.NoError(t, <-reloadDone)
			require.NoError(t, <-closeDone)
			require.False(t, dependencyClosedEarly, "auto-increment service closed before query ingress drained")
			require.True(t, lateRequestRejected, "query request admitted after shutdown started")
			require.Equal(t, int32(1), reloadCallCount)
			require.False(t, dependencyClosedWhileActive, "auto-increment service closed while a query handler was active")
			select {
			case <-incrServiceClosed:
			default:
				t.Fatal("auto-increment service was not closed")
			}
			select {
			case <-txnClientClosed:
			default:
				t.Fatal("transaction client was not closed")
			}
		},
	)
}

func TestPipelineAdmissionRejectsRequestAlreadyReadDuringClose(t *testing.T) {
	moruntime.RunTest(t.Name(), func(moruntime.Runtime) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		requestEntered := make(chan struct{})
		allowAdmission := make(chan struct{})
		requestHandled := make(chan struct{}, 1)
		s := &service{}
		_, release, admitted := s.admitPipelineHandler(context.Background())
		require.True(t, admitted)
		release()
		s.pipelines.beforeAdmission = func() {
			close(requestEntered)
			<-allowAdmission
		}
		s.requestHandler = func(
			context.Context,
			string,
			morpc.Message,
			morpc.ClientSession,
			engine.Engine,
			fileservice.FileService,
			lockservice.LockService,
			qclient.QueryClient,
			logservice.CNHAKeeperClient,
			udf.Service,
			client.TxnClient,
			*defines.AutoIncrCacheManager,
			func() morpc.Message,
		) error {
			requestHandled <- struct{}{}
			return nil
		}

		rpcServer, err := morpc.NewRPCServer(
			"test-pipeline-admission",
			addr,
			morpc.NewMessageCodec(t.Name(), func() morpc.Message {
				return cnclient.AcquireMessage()
			}),
			morpc.WithServerGoettyOptions(
				goetty.WithSessionReleaseMsgFunc(func(v any) {
					message := v.(morpc.RPCMessage)
					if !message.InternalMessage() {
						cnclient.ReleaseMessage(message.Message.(*pipeline.Message))
					}
				}),
			),
		)
		require.NoError(t, err)
		rpcServer.RegisterRequestHandler(s.handleRequest)
		require.NoError(t, rpcServer.Start())

		pipelineClient, err := cnclient.NewPipelineClient(t.Name(), "", &cnclient.PipelineConfig{})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pipelineClient.Close())
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stream, err := pipelineClient.NewStream(ctx, addr)
		require.NoError(t, err)
		receiveC, err := stream.Receive()
		require.NoError(t, err)
		defer func() {
			require.NoError(t, stream.Close(true))
		}()

		message := &pipeline.Message{
			Id:  stream.ID(),
			Sid: pipeline.Status_Last,
		}
		require.NoError(t, stream.Send(ctx, message))
		select {
		case <-requestEntered:
		case <-ctx.Done():
			t.Fatal("RPC request did not reach the pre-admission hook")
		}

		require.NoError(t, s.closePipelineAdmission())
		close(allowAdmission)
		require.NoError(t, rpcServer.Close())
		require.NoError(t, s.waitPipelineHandlers())

		select {
		case response := <-receiveC:
			require.Nil(t, response)
		case <-ctx.Done():
			t.Fatal("RPC session did not terminate after admission rejected the request")
		}
		select {
		case <-requestHandled:
			t.Fatal("request entered pipeline execution after admission was closed")
		default:
		}
	})
}

func TestFragmentedPipelineCacheSurvivesHandlerRelease(t *testing.T) {
	moruntime.RunTest(t.Name(), func(moruntime.Runtime) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		firstFragmentAdmitted := make(chan struct{})
		cacheScanned := make(chan struct{}, 4)
		assembled := make(chan []byte, 1)
		var admissions atomic.Int32
		s := &service{cfg: &Config{UUID: t.Name()}}
		s.pipelines.beforeAdmission = func() {
			if admissions.Add(1) == 1 {
				close(firstFragmentAdmitted)
			}
		}
		s.requestHandler = func(
			_ context.Context,
			_ string,
			message morpc.Message,
			_ morpc.ClientSession,
			_ engine.Engine,
			_ fileservice.FileService,
			_ lockservice.LockService,
			_ qclient.QueryClient,
			_ logservice.CNHAKeeperClient,
			_ udf.Service,
			_ client.TxnClient,
			_ *defines.AutoIncrCacheManager,
			_ func() morpc.Message,
		) error {
			assembled <- append([]byte(nil), message.(*pipeline.Message).GetData()...)
			return nil
		}

		rpcServer, err := morpc.NewRPCServer(
			"test-fragmented-pipeline-cache",
			addr,
			morpc.NewMessageCodec(t.Name(), func() morpc.Message {
				return cnclient.AcquireMessage()
			}),
			morpc.WithServerGoettyOptions(
				goetty.WithSessionReleaseMsgFunc(func(v any) {
					message := v.(morpc.RPCMessage)
					if !message.InternalMessage() {
						cnclient.ReleaseMessage(message.Message.(*pipeline.Message))
					}
				}),
			),
			morpc.WithServerDisableAutoCancelContext(),
			morpc.WithServerMessageCacheScanHookForTesting(func() {
				select {
				case cacheScanned <- struct{}{}:
				default:
				}
			}),
		)
		require.NoError(t, err)
		rpcServer.RegisterRequestHandler(s.handleRequest)
		require.NoError(t, rpcServer.Start())
		defer func() { require.NoError(t, rpcServer.Close()) }()

		pipelineClient, err := cnclient.NewPipelineClient(t.Name(), "", &cnclient.PipelineConfig{})
		require.NoError(t, err)
		defer func() { require.NoError(t, pipelineClient.Close()) }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stream, err := pipelineClient.NewStream(ctx, addr)
		require.NoError(t, err)
		defer func() { require.NoError(t, stream.Close(true)) }()

		require.NoError(t, stream.Send(ctx, &pipeline.Message{
			Id:   stream.ID(),
			Cmd:  pipeline.Method_PipelineMessage,
			Sid:  pipeline.Status_WaitingNext,
			Data: []byte("first-"),
		}))
		select {
		case <-firstFragmentAdmitted:
		case <-ctx.Done():
			t.Fatal("first fragment was not admitted")
		}
		// Observe two completed MORPC cleanup scans. The fragmented cache must
		// survive both after the first per-handler context has been released.
		for range 2 {
			select {
			case <-cacheScanned:
			case <-ctx.Done():
				t.Fatal("message cache timeout scan did not run")
			}
		}
		require.NoError(t, stream.Send(ctx, &pipeline.Message{
			Id:   stream.ID(),
			Cmd:  pipeline.Method_PipelineMessage,
			Sid:  pipeline.Status_Last,
			Data: []byte("last"),
		}))
		select {
		case data := <-assembled:
			require.Equal(t, []byte("first-last"), data)
		case <-ctx.Done():
			t.Fatal("fragmented pipeline was not assembled")
		}
	})
}

func TestServiceCloseCancelsAdmittedPipeline(t *testing.T) {
	moruntime.RunTest(t.Name(), func(moruntime.Runtime) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ls := mock_lock.NewMockLockService(ctrl)
		ls.EXPECT().Close().Return(nil).Times(2)

		handlerStarted := make(chan struct{})
		handlerExited := make(chan struct{})
		var cancelCount atomic.Int32
		s := &service{
			cfg:                &Config{UUID: t.Name()},
			logger:             zap.NewNop(),
			stopper:            stopper.NewStopper("test-pipeline-cancel"),
			mo:                 closeErrorMOServer{},
			cancelMoServerFunc: func() {},
			server:             closeOnlyRPCServer{},
			lockService:        ls,
		}
		s.requestHandler = func(
			ctx context.Context,
			_ string,
			_ morpc.Message,
			_ morpc.ClientSession,
			_ engine.Engine,
			_ fileservice.FileService,
			_ lockservice.LockService,
			_ qclient.QueryClient,
			_ logservice.CNHAKeeperClient,
			_ udf.Service,
			_ client.TxnClient,
			_ *defines.AutoIncrCacheManager,
			_ func() morpc.Message,
		) error {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerExited)
			return ctx.Err()
		}

		err := s.handleRequest(
			context.Background(),
			morpc.RPCMessage{
				Message: &pipeline.Message{Sid: pipeline.Status_Last},
				Cancel:  func() { cancelCount.Add(1) },
			},
			0,
			nil,
		)
		require.NoError(t, err)
		select {
		case <-handlerStarted:
		case <-time.After(time.Second):
			t.Fatal("pipeline handler did not start")
		}

		closed := make(chan error, 1)
		go func() {
			closed <- s.Close()
		}()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("CN close did not cancel the admitted pipeline")
		}
		select {
		case <-handlerExited:
		default:
			t.Fatal("CN close returned before the canceled pipeline exited")
		}
		require.Equal(t, int32(1), cancelCount.Load())
	})
}

func TestPipelineAdmissionRejectCancelsRequestOnce(t *testing.T) {
	s := &service{}
	require.NoError(t, s.closePipelineAdmission())
	var cancelCount atomic.Int32
	err := s.handleRequest(
		context.Background(),
		morpc.RPCMessage{
			Message: &pipeline.Message{Sid: pipeline.Status_Last},
			Cancel:  func() { cancelCount.Add(1) },
		},
		0,
		nil,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable))
	require.Equal(t, int32(1), cancelCount.Load())
}

func TestPipelineEarlyReturnCancelsRequestOnce(t *testing.T) {
	t.Run("invalid fragment command", func(t *testing.T) {
		s := &service{}
		var cancelCount atomic.Int32
		err := s.handleRequest(
			context.Background(),
			morpc.RPCMessage{
				Message: &pipeline.Message{Sid: pipeline.Status_WaitingNext},
				Cancel:  func() { cancelCount.Add(1) },
			},
			0,
			nil,
		)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, int32(1), cancelCount.Load())
	})

	t.Run("assembly failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		session := mock_morpc.NewMockClientSession(ctrl)
		cache := &testMessageCache{cache: []morpc.Message{
			&pipeline.Message{
				Cmd:                   pipeline.Method_PipelineMessage,
				RequestedTeardownMode: pipeline.StreamTeardownMode_FinishAck,
			},
		}}
		session.EXPECT().CreateCache(gomock.Any(), uint64(1)).Return(cache, nil)
		session.EXPECT().DeleteCache(uint64(1))
		s := &service{}
		var cancelCount atomic.Int32
		err := s.handleRequest(
			context.Background(),
			morpc.RPCMessage{
				Message: &pipeline.Message{
					Id:  1,
					Cmd: pipeline.Method_PipelineMessage,
					Sid: pipeline.Status_Last,
				},
				Cancel: func() { cancelCount.Add(1) },
			},
			0,
			session,
		)
		require.Error(t, err)
		require.Equal(t, int32(1), cancelCount.Load())
	})
}

func Test_tenant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	boot := &testBootService{}

	sv := &service{
		bootstrapService: boot,
	}

	err := sv.CheckTenantUpgrade(ctx, 3)
	assert.Nil(t, err)

	err = sv.UpgradeTenant(ctx, "acc3", 1, true)
	assert.Nil(t, err)

	boot.choice = 1
	err = sv.CheckTenantUpgrade(ctx, 3)
	assert.Error(t, err)

	err = sv.UpgradeTenant(ctx, "acc3", 1, true)
	assert.Error(t, err)
}
