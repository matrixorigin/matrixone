// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"go.uber.org/zap"
)

type operator struct {
	sync.RWMutex

	sid         string
	cfg         ServiceConfig
	index       int
	serviceType metadata.ServiceType
	state       state
	testing     bool

	reset struct {
		svc        service
		shutdownC  chan struct{}
		stopper    *stopper.Stopper
		rt         runtime.Runtime
		gossipNode *gossip.Node
		clock      clock.Clock
		logger     *zap.Logger
	}
}

type service interface {
	Start() error
	Close() error
}

func newService(
	file string,
	index int,
	adjust func(*operator),
	testing bool,
) (*operator, error) {
	cfg := newServiceConfig()
	if err := parseConfigFromFile(file, &cfg); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	op := &operator{
		cfg:         cfg,
		index:       index,
		sid:         cfg.mustGetServiceUUID(),
		serviceType: cfg.mustGetServiceType(),
		testing:     testing,
	}
	if adjust != nil {
		adjust(op)
	}
	op.sid = op.cfg.mustGetServiceUUID()
	return op, nil
}

func (op *operator) Index() int {
	return op.index
}

func (op *operator) ServiceID() string {
	return op.sid
}

func (op *operator) ServiceType() metadata.ServiceType {
	return op.serviceType
}

func (op *operator) RawService() interface{} {
	op.RLock()
	defer op.RUnlock()
	return op.reset.svc
}

func (op *operator) Close() error {
	op.Lock()
	defer op.Unlock()

	if op.state == stopped {
		return moerr.NewInvalidStateNoCtx("service already stopped")
	}

	if err := op.reset.svc.Close(); err != nil {
		return err
	}

	op.reset.stopper.Stop()
	op.state = stopped
	return nil
}

func (op *operator) Start() error {
	op.Lock()
	defer op.Unlock()

	if op.state == started {
		return moerr.NewInvalidStateNoCtx("service already started")
	}

	if err := op.init(); err != nil {
		return err
	}

	fs, err := op.cfg.createFileService(
		context.Background(),
		op.serviceType,
		op.sid,
	)
	if err != nil {
		return err
	}

	// start up system module to do some calculation.
	system.Run(op.reset.stopper)

	switch op.serviceType {
	case metadata.ServiceType_CN:
		err = op.startCNServiceLocked(fs)
	case metadata.ServiceType_TN:
		err = op.startTNServiceLocked(fs)
	case metadata.ServiceType_LOG:
		err = op.startLogServiceLocked(fs)
	case metadata.ServiceType_PROXY:
		err = op.startProxyServiceLocked()
	case metadata.ServiceType_PYTHON_UDF:
		err = op.startPythonUDFServiceLocked()
	default:
		panic("unknown service type")
	}

	if err != nil {
		return err
	}

	op.state = started
	return nil
}

func (op *operator) Adjust(
	fn func(*ServiceConfig),
) {
	op.Lock()
	defer op.Unlock()

	fn(&op.cfg)
}

func (op *operator) GetServiceConfig() ServiceConfig {
	op.RLock()
	defer op.RUnlock()
	return op.cfg
}

func (op *operator) startLogServiceLocked(
	fs fileservice.FileService,
) error {
	commonConfigKVMap, _ := dumpCommonConfig(op.cfg)
	s, err := logservice.NewService(
		op.cfg.getLogServiceConfig(),
		fs,
		op.reset.shutdownC,
		logservice.WithRuntime(op.reset.rt),
		logservice.WithConfigData(commonConfigKVMap),
	)
	if err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	if op.cfg.LogService.BootstrapConfig.BootstrapCluster {
		op.reset.logger.Info("bootstrapping hakeeper...")
		if err := s.BootstrapHAKeeper(context.Background(), op.cfg.LogService); err != nil {
			return err
		}
	}
	op.reset.svc = s
	return nil
}

func (op *operator) startTNServiceLocked(
	fs fileservice.FileService,
) error {
	if err := op.waitClusterConditionLocked(op.waitHAKeeperRunningLocked); err != nil {
		return err
	}
	op.cfg.initMetaCache()
	c := op.cfg.getTNServiceConfig()
	//notify the tn service it is in the standalone cluster
	c.InStandalone = op.cfg.IsStandalone
	commonConfigKVMap, _ := dumpCommonConfig(op.cfg)
	s, err := tnservice.NewService(
		&c,
		op.reset.rt,
		fs,
		op.reset.shutdownC,
		tnservice.WithConfigData(commonConfigKVMap),
	)
	if err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	op.reset.svc = s
	return nil
}

func (op *operator) startCNServiceLocked(
	fs fileservice.FileService,
) error {
	if err := op.waitClusterConditionLocked(op.waitAnyShardReadyLocked); err != nil {
		return err
	}
	op.cfg.initMetaCache()
	c := op.cfg.getCNServiceConfig()
	commonConfigKVMap, _ := dumpCommonConfig(op.cfg)
	s, err := cnservice.NewService(
		&c,
		context.Background(),
		fs,
		op.reset.gossipNode,
		cnservice.WithLogger(op.reset.logger),
		cnservice.WithMessageHandle(compile.CnServerMessageHandler),
		cnservice.WithConfigData(commonConfigKVMap),
		cnservice.WithTxnTraceData(filepath.Join(op.cfg.DataDir, c.Txn.Trace.Dir)),
	)
	if err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	op.reset.svc = s
	return nil
}

func (op *operator) startProxyServiceLocked() error {
	if err := op.waitClusterConditionLocked(op.waitHAKeeperRunningLocked); err != nil {
		return err
	}
	return op.reset.stopper.RunNamedTask(
		"proxy-service",
		func(ctx context.Context) {
			s, err := proxy.NewServer(
				ctx,
				op.cfg.getProxyConfig(),
				proxy.WithRuntime(op.reset.rt),
			)
			if err != nil {
				panic(err)
			}
			if err := s.Start(); err != nil {
				panic(err)
			}
		},
	)
}

func (op *operator) startPythonUDFServiceLocked() error {
	if err := op.waitClusterConditionLocked(op.waitHAKeeperRunningLocked); err != nil {
		return err
	}
	return op.reset.stopper.RunNamedTask(
		"python-udf-service",
		func(ctx context.Context) {
			s, err := pythonservice.NewService(op.cfg.PythonUdfServerConfig)
			if err != nil {
				panic(err)
			}
			if err := s.Start(); err != nil {
				panic(err)
			}
		},
	)
}

func (op *operator) init() error {
	if err := op.initLogger(); err != nil {
		return err
	}
	if err := op.initStopper(); err != nil {
		return err
	}
	if err := op.initClock(); err != nil {
		return err
	}
	if err := op.initRuntime(); err != nil {
		return nil
	}
	if err := op.setupGossip(); err != nil {
		return err
	}

	malloc.SetDefaultConfig(op.cfg.Malloc)

	op.reset.rt.SetGlobalVariables(
		runtime.StatusServer,
		status.NewServer(),
	)

	return nil
}

func (op *operator) initLogger() error {
	logger, err := logutil.NewMOLogger(&op.cfg.Log)
	if err != nil {
		return err
	}
	op.reset.logger = logger.With(zap.String("service", op.sid))
	return nil
}

func (op *operator) initStopper() error {
	op.reset.shutdownC = make(chan struct{})
	op.reset.stopper = stopper.NewStopper(
		fmt.Sprintf("%s/%s", op.serviceType.String(), op.sid),
		stopper.WithLogger(op.reset.logger),
	)
	return nil
}

func (op *operator) initClock() error {
	clock, err := newClock(
		op.cfg,
		op.reset.stopper,
	)
	if err != nil {
		return err
	}
	op.reset.clock = clock
	return nil
}

func (op *operator) initRuntime() error {
	rt := runtime.NewRuntime(
		op.serviceType,
		op.sid,
		op.reset.logger,
		runtime.WithClock(op.reset.clock),
	)
	runtime.SetupServiceBasedRuntime(op.sid, rt)
	if op.testing {
		runtime.SetupServiceRuntimeTestingContext(op.sid)
	}
	catalog.SetupDefines(op.sid)
	op.reset.rt = rt
	return nil
}

func (op *operator) setupGossip() error {
	if op.serviceType != metadata.ServiceType_CN {
		return nil
	}

	gossipNode, err := gossip.NewNode(
		context.Background(),
		op.sid,
	)
	if err != nil {
		return err
	}
	for i := range op.cfg.FileServices {
		op.cfg.FileServices[i].Cache.KeyRouterFactory = gossipNode.DistKeyCacheGetter()
		op.cfg.FileServices[i].Cache.QueryClient, err = client.NewQueryClient(
			op.cfg.CN.UUID,
			op.cfg.FileServices[i].Cache.RPC,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (op *operator) waitClusterConditionLocked(
	waitFunc func(logservice.CNHAKeeperClient) error,
) error {
	client, err := op.waitHAKeeperReadyLocked()
	if err != nil {
		return err
	}
	if err := waitFunc(client); err != nil {
		return err
	}
	if err := client.Close(); err != nil {
		op.reset.logger.Error("close hakeeper client failed", zap.Error(err))
	}
	return nil
}

func (op *operator) waitHAKeeperRunningLocked(
	client logservice.CNHAKeeperClient,
) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*2)
	defer cancel()

	// wait HAKeeper running
	for {
		state, err := client.GetClusterState(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) ||
			state.State != logpb.HAKeeperRunning {
			// not ready
			op.reset.logger.Info("hakeeper not ready, retry")
			time.Sleep(time.Second)
			continue
		}
		return err
	}
}

func (op *operator) waitAnyShardReadyLocked(client logservice.CNHAKeeperClient) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
	defer cancel()

	// wait shard ready
	for {
		if ok, err := func() (bool, error) {
			details, err := client.GetClusterDetails(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					op.reset.logger.Error(
						"wait TN ready timeout",
						zap.Error(err),
					)
					return false, err
				}
				op.reset.logger.Error(
					"failed to get cluster details",
					zap.Error(err),
				)
				return false, nil
			}
			for _, store := range details.TNStores {
				if len(store.Shards) > 0 {
					return true, nil
				}
			}
			op.reset.logger.Info("shard not ready")
			return false, nil
		}(); err != nil {
			return err
		} else if ok {
			op.reset.logger.Info("shard ready")
			return nil
		}
		time.Sleep(time.Second)
	}
}

func (op *operator) waitHAKeeperReadyLocked() (logservice.CNHAKeeperClient, error) {
	getClient := func() (logservice.CNHAKeeperClient, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		client, err := logservice.NewCNHAKeeperClient(
			ctx,
			op.sid,
			op.cfg.HAKeeperClient,
		)
		if err != nil {
			op.reset.logger.Error(
				"hakeeper not ready",
				zap.Error(err),
			)
			return nil, err
		}
		return client, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, moerr.NewInternalErrorNoCtx("wait hakeeper ready timeout")
		default:
			client, err := getClient()
			if err == nil {
				return client, nil
			}
			time.Sleep(time.Second)
		}
	}
}
