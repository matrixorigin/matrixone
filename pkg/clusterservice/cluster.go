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

package clusterservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

// GetMOCluster get mo cluster from process level runtime
func GetMOCluster() MOCluster {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.ClusterService)
	if !ok {
		panic("no mocluster service")
	}
	return v.(MOCluster)
}

// Option options for create cluster
type Option func(*cluster)

// WithServices set init cn and tn services
func WithServices(
	cnServices []metadata.CNService,
	tnServices []metadata.TNService) Option {
	return func(c *cluster) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range tnServices {
			c.mu.tnServices[s.ServiceID] = s
		}
		for _, s := range cnServices {
			c.mu.cnServices[s.ServiceID] = s
		}
	}
}

// WithDisableRefresh disable refresh from hakeeper
func WithDisableRefresh() Option {
	return func(c *cluster) {
		c.options.disableRefresh = true
	}
}

type cluster struct {
	logger          *log.MOLogger
	stopper         *stopper.Stopper
	client          ClusterClient
	refreshInterval time.Duration
	forceRefreshC   chan struct{}
	readyOnce       sync.Once
	readyC          chan struct{}
	mu              struct {
		sync.RWMutex
		cnServices map[string]metadata.CNService
		tnServices map[string]metadata.TNService
	}
	options struct {
		disableRefresh bool
	}
}

// NewMOCluster create a MOCluter by HAKeeperClient. MoCluster synchronizes
// information from HAKeeper and forcibly refreshes the information once every
// refreshInterval.
//
// TODO(fagongzi): extend hakeeper to support event-driven original message changes
func NewMOCluster(
	client ClusterClient,
	refreshInterval time.Duration,
	opts ...Option) MOCluster {
	logger := runtime.ProcessLevelRuntime().Logger().Named("mo-cluster")
	c := &cluster{
		logger:          logger,
		stopper:         stopper.NewStopper("mo-cluster", stopper.WithLogger(logger.RawLogger())),
		client:          client,
		forceRefreshC:   make(chan struct{}, 1),
		readyC:          make(chan struct{}),
		refreshInterval: refreshInterval,
	}
	c.mu.cnServices = make(map[string]metadata.CNService, 1024)
	c.mu.tnServices = make(map[string]metadata.TNService, 1024)

	for _, opt := range opts {
		opt(c)
	}
	if !c.options.disableRefresh {
		if err := c.stopper.RunTask(c.refreshTask); err != nil {
			panic(err)
		}
	} else {
		close(c.readyC)
	}
	return c
}

func (c *cluster) GetCNService(selector Selector, apply func(metadata.CNService) bool) {
	c.waitReady()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cn := range c.mu.cnServices {
		// If the all field is false, the work state of CN service MUST be
		// working, and then we could do the filter job. If the state is not
		// working, means that the CN may be marked as draining and is going
		// to be removed, or has been removed.
		// The state Unknown is allowed here to make many test cases pass, and
		// it does not affect the function.
		if (selector.all || cn.WorkState == metadata.WorkState_Working ||
			cn.WorkState == metadata.WorkState_Unknown) &&
			selector.filterCN(cn) {
			if !apply(cn) {
				return
			}
		}
	}
}

func (c *cluster) GetCNServiceWithoutWorkingState(selector Selector, apply func(metadata.CNService) bool) {
	c.waitReady()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cn := range c.mu.cnServices {
		if selector.filterCN(cn) {
			if !apply(cn) {
				return
			}
		}
	}
}

func (c *cluster) GetTNService(selector Selector, apply func(metadata.TNService) bool) {
	c.waitReady()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, tn := range c.mu.tnServices {
		if selector.filterTN(tn) {
			if !apply(tn) {
				return
			}
		}
	}
}

func (c *cluster) ForceRefresh(sync bool) {
	if c.options.disableRefresh {
		return
	}
	if sync {
		c.refresh()
		return
	}

	select {
	case c.forceRefreshC <- struct{}{}:
	default:
	}
}

func (c *cluster) Close() {
	c.waitReady()
	c.stopper.Stop()
	close(c.forceRefreshC)
}

// DebugUpdateCNLabel implements the MOCluster interface.
func (c *cluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()
	convert := make(map[string]metadata.LabelList)
	for k, v := range kvs {
		convert[k] = metadata.LabelList{Labels: v}
	}
	label := logpb.CNStoreLabel{
		UUID:   uuid,
		Labels: convert,
	}
	proxyClient := c.client.(labelSupportedClient)
	if err := proxyClient.UpdateCNLabel(ctx, label); err != nil {
		return err
	}
	return nil
}

func (c *cluster) waitReady() {
	<-c.readyC
}

func (c *cluster) refreshTask(ctx context.Context) {
	c.ForceRefresh(false)

	timer := time.NewTimer(c.refreshInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("refresh cluster details task stopped")
			return
		case <-timer.C:
			c.refresh()
			timer.Reset(c.refreshInterval)
		case <-c.forceRefreshC:
			c.refresh()
		}
	}
}

func (c *cluster) refresh() {
	defer c.logger.LogAction("refresh from hakeeper",
		log.DefaultLogOptions().WithLevel(zap.DebugLevel))()

	ctx, cancel := context.WithTimeout(context.Background(), c.refreshInterval)
	defer cancel()

	details, err := c.client.GetClusterDetails(ctx)
	if err != nil {
		c.logger.Error("failed to refresh cluster details from hakeeper",
			zap.Error(err))
		return
	}

	c.logger.Debug("refresh cluster details from hakeeper",
		zap.Int("cn-count", len(details.CNStores)),
		zap.Int("dn-count", len(details.TNStores)))

	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.mu.cnServices {
		delete(c.mu.cnServices, k)
	}
	for k := range c.mu.tnServices {
		delete(c.mu.tnServices, k)
	}
	for _, cn := range details.CNStores {
		v := newCNService(cn)
		c.mu.cnServices[cn.UUID] = v
		if c.logger.Enabled(zap.DebugLevel) {
			c.logger.Debug("cn service added", zap.String("cn", v.DebugString()))
		}
	}
	for _, tn := range details.TNStores {
		v := newTNService(tn)
		c.mu.tnServices[tn.UUID] = v
		if c.logger.Enabled(zap.DebugLevel) {
			c.logger.Debug("dn service added", zap.String("dn", v.DebugString()))
		}
	}
	c.readyOnce.Do(func() {
		close(c.readyC)
	})
}

func newCNService(cn logpb.CNStore) metadata.CNService {
	return metadata.CNService{
		ServiceID:              cn.UUID,
		PipelineServiceAddress: cn.ServiceAddress,
		SQLAddress:             cn.SQLAddress,
		LockServiceAddress:     cn.LockServiceAddress,
		CtlAddress:             cn.CtlAddress,
		WorkState:              cn.WorkState,
		Labels:                 cn.Labels,
		QueryAddress:           cn.QueryAddress,
	}
}

func newTNService(tn logpb.TNStore) metadata.TNService {
	v := metadata.TNService{
		ServiceID:             tn.UUID,
		TxnServiceAddress:     tn.ServiceAddress,
		LogTailServiceAddress: tn.LogtailServerAddress,
		LockServiceAddress:    tn.LockServiceAddress,
		CtlAddress:            tn.CtlAddress,
	}
	v.Shards = make([]metadata.TNShard, 0, len(tn.Shards))
	for _, s := range tn.Shards {
		v.Shards = append(v.Shards, metadata.TNShard{
			TNShardRecord: metadata.TNShardRecord{ShardID: s.ShardID},
			ReplicaID:     s.ReplicaID,
		})
	}
	return v
}
