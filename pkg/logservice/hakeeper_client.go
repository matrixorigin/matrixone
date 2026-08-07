// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	defaultBackendReadTimeout = time.Second * 8

	// ScheduleCommandPollInterval bounds the start-to-start delay between
	// degraded-path command reads. ScheduleCommandPollTimeout bounds each read.
	// Neither value inherits the heartbeat RPC timeout.
	ScheduleCommandPollInterval           = time.Second
	ScheduleCommandPollTimeout            = 3 * time.Second
	scheduleCommandInitialPollJitterRange = 250 * time.Millisecond
)

// ScheduleCommandInitialPollDelay spreads the first degraded-path poll across
// the final quarter of the one-second progress budget. Subsequent polls keep a
// one-second start-to-start cadence, so this only avoids fleet-wide bursts and
// never weakens the discovery bound.
func ScheduleCommandInitialPollDelay(serviceID string) time.Duration {
	hash := uint64(14695981039346656037)
	for i := range len(serviceID) {
		hash ^= uint64(serviceID[i])
		hash *= 1099511628211
	}
	return ScheduleCommandPollInterval - scheduleCommandInitialPollJitterRange +
		time.Duration(hash%uint64(scheduleCommandInitialPollJitterRange+1))
}

var hakeeperClientRetryInterval = 10 * time.Millisecond

// ScheduleCommandBatchFingerprint identifies the command content independently
// of its delivery ID. It is used only on the rare command path to suppress one
// legacy heartbeat replay after a rolling-upgrade leader change.
type ScheduleCommandFingerprint [sha256.Size]byte

func scheduleCommandFingerprint(message proto.Message) ScheduleCommandFingerprint {
	// gogo's compact text encoder sorts map keys and preserves unknown fields.
	// Its generated binary fast path does not honor deterministic marshaling.
	return sha256.Sum256([]byte(proto.CompactTextString(message)))
}

func ScheduleCommandBatchFingerprint(batch pb.CommandBatch) ScheduleCommandFingerprint {
	normalized := pb.CommandBatch{
		Commands: batch.Commands,
	}
	return scheduleCommandFingerprint(&normalized)
}

// ScheduleCommandIdentity is the comparable client-side form of the protobuf
// identity assigned by HAKeeper.
type ScheduleCommandIdentity struct {
	OriginBatchID uint64
	CommandIndex  uint64
}

// ScheduleCommandBatchHasStableIDs validates the delivery boundary. A batch
// without a complete set of identities must not be acknowledged: doing so
// would make an inherited command indistinguishable from intentionally new,
// identical work.
func ScheduleCommandBatchHasStableIDs(batch pb.CommandBatch) bool {
	if batch.BatchID == 0 || len(batch.CommandIDs) != len(batch.Commands) {
		return false
	}
	seen := make(map[ScheduleCommandIdentity]struct{}, len(batch.CommandIDs))
	for i := range batch.CommandIDs {
		if batch.CommandIDs[i].OriginBatchID == 0 {
			return false
		}
		identity := ScheduleCommandIdentity{
			OriginBatchID: batch.CommandIDs[i].OriginBatchID,
			CommandIndex:  batch.CommandIDs[i].CommandIndex,
		}
		if _, ok := seen[identity]; ok {
			return false
		}
		seen[identity] = struct{}{}
	}
	return true
}

// FilterUnappliedScheduleCommands suppresses commands inherited by a newer
// batch generation while preserving the existing retry-until-state-confirmed
// contract for bootstrap commands. Stable identities distinguish a newly
// scheduled command from earlier work with identical payload. The returned set
// is bounded by the current pending batch and replaces, rather than grows,
// caller state.
func FilterUnappliedScheduleCommands(
	batch pb.CommandBatch,
	applied map[ScheduleCommandIdentity]struct{},
) ([]pb.ScheduleCommand, map[ScheduleCommandIdentity]struct{}, bool) {
	if batch.BatchID == 0 || len(batch.CommandIDs) != len(batch.Commands) {
		return nil, applied, false
	}
	filtered := make([]pb.ScheduleCommand, 0, len(batch.Commands))
	current := make(map[ScheduleCommandIdentity]struct{}, len(batch.Commands))
	for i := range batch.Commands {
		if batch.CommandIDs[i].OriginBatchID == 0 {
			return nil, applied, false
		}
		identity := ScheduleCommandIdentity{
			OriginBatchID: batch.CommandIDs[i].OriginBatchID,
			CommandIndex:  batch.CommandIDs[i].CommandIndex,
		}
		if _, ok := current[identity]; ok {
			return nil, applied, false
		}
		current[identity] = struct{}{}
		if IsRetryableScheduleCommand(batch.Commands[i]) {
			filtered = append(filtered, batch.Commands[i])
			continue
		}
		if _, ok := applied[identity]; !ok {
			filtered = append(filtered, batch.Commands[i])
		}
	}
	return filtered, current, true
}

// IsRetryableScheduleCommand reports commands whose existing HAKeeper
// delivery contract is at-least-once until store state acknowledges them.
func IsRetryableScheduleCommand(command pb.ScheduleCommand) bool {
	return command.Bootstrapping &&
		command.ConfigChange != nil &&
		command.ConfigChange.ChangeType == pb.StartReplica
}

type basicHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// AllocateID allocate a globally unique ID
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error)
	// GetClusterDetails queries the HAKeeper and return CN and TN nodes that are
	// known to the HAKeeper.
	GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error)
	// GetClusterState queries the cluster state
	GetClusterState(ctx context.Context) (pb.CheckerState, error)
	// CheckLogServiceHealth checks if the log-service is healthy or not.
	CheckLogServiceHealth(ctx context.Context) error
}

// ClusterHAKeeperClient used to get cluster detail
type ClusterHAKeeperClient interface {
	basicHAKeeperClient
}

// CNHAKeeperClient is the HAKeeper client used by a CN store.
type CNHAKeeperClient interface {
	basicHAKeeperClient
	BRHAKeeperClient
	// SendCNHeartbeat sends the specified heartbeat message to the HAKeeper.
	SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error)
	// UpdateNonVotingReplicaNum updates the non-voting-replica-num which is stores in HAKeeper.
	UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error
	// UpdateNonVotingLocality updates the non-voting-locality which is stores in HAKeeper.
	UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error
}

// TNHAKeeperClient is the HAKeeper client used by a TN store.
type TNHAKeeperClient interface {
	basicHAKeeperClient
	// SendTNHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// TN store.
	SendTNHeartbeat(ctx context.Context, hb pb.TNStoreHeartbeat) (pb.CommandBatch, error)
}

// ScheduleCommandHAKeeperClient is implemented by clients that can poll
// schedule commands independently from heartbeat RPCs. CN and TN services use
// a type assertion so existing test doubles remain source-compatible.
type ScheduleCommandHAKeeperClient interface {
	GetScheduleCommands(ctx context.Context, serviceType pb.ServiceType) (pb.CommandBatch, error)
}

// LogHAKeeperClient is the HAKeeper client used by a Log store.
type LogHAKeeperClient interface {
	basicHAKeeperClient
	// SendLogHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// Log store.
	SendLogHeartbeat(ctx context.Context, hb pb.LogStoreHeartbeat) (pb.CommandBatch, error)
}

// ProxyHAKeeperClient is the HAKeeper client used by proxy service.
type ProxyHAKeeperClient interface {
	basicHAKeeperClient
	// GetCNState gets CN state from HAKeeper.
	GetCNState(ctx context.Context) (pb.CNState, error)
	// UpdateCNLabel updates the labels of CN.
	UpdateCNLabel(ctx context.Context, label pb.CNStoreLabel) error
	// UpdateCNWorkState updates the work state of CN.
	UpdateCNWorkState(ctx context.Context, state pb.CNWorkState) error
	// PatchCNStore updates the work state and labels of CN.
	PatchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error
	// DeleteCNStore deletes a CN store from HAKeeper.
	DeleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error
	// SendProxyHeartbeat sends the heartbeat of proxy to HAKeeper.
	SendProxyHeartbeat(ctx context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error)
}

// BRHAKeeperClient is the HAKeeper client for backup and restore.
type BRHAKeeperClient interface {
	GetBackupData(ctx context.Context) ([]byte, error)
}

// TODO: HAKeeper discovery to be implemented

var _ CNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ TNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ LogHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ ProxyHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ ScheduleCommandHAKeeperClient = (*managedHAKeeperClient)(nil)

var newHAKeeperClientFunc = newHAKeeperClient
var sendCNAllocateIDFunc = (*hakeeperClient).sendCNAllocateID
var sendCNAllocateIDWithRequestIDFunc = (*hakeeperClient).sendCNAllocateIDWithRequestID

// NewClusterHAKeeperClient creates a HAKeeper client to query cluster details.
//
// NB: caller must set a deadline on ctx and could specify options for morpc.Client via ctx.
func NewClusterHAKeeperClient(
	ctx context.Context, sid string, cfg HAKeeperClientConfig,
) (ClusterHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewCNHAKeeperClient creates a HAKeeper client to be used by a CN node.
//
// NB: caller must set a deadline on ctx and could specify options for morpc.Client via ctx.
func NewCNHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (CNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewTNHAKeeperClient creates a HAKeeper client to be used by a TN node.
//
// NB: caller must set a deadline on ctx and could specify options for morpc.Client via ctx.
func NewTNHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (TNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewLogHAKeeperClient creates a HAKeeper client to be used by a Log Service node.
//
// NB: caller must set a deadline on ctx and could specify options for morpc.Client via ctx.
func NewLogHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (LogHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

// NewLogHAKeeperClientWithRetry creates a HAKeeper client with retry.
func NewLogHAKeeperClientWithRetry(
	ctx context.Context, sid string, cfg HAKeeperClientConfig,
) ClusterHAKeeperClient {
	var c ClusterHAKeeperClient
	createFn := func() error {
		ctx, cancel := context.WithTimeoutCause(
			ctx, time.Second*5, moerr.CauseNewLogHAKeeperClientWithRetry,
		)
		defer cancel()
		client, err := NewClusterHAKeeperClient(ctx, sid, cfg)
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Errorf("failed to create HAKeeper client: %v", err)
			return err
		}
		c = client
		return nil
	}
	timer := time.NewTimer(time.Minute * 2)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			panic("failed to create HAKeeper client")

		default:
			if err := createFn(); err != nil {
				retryTimer := time.NewTimer(time.Second * 3)
				select {
				case <-ctx.Done():
					retryTimer.Stop()
					return nil
				case <-retryTimer.C:
				}
				continue
			}
			return c
		}
	}
}

// NewProxyHAKeeperClient creates a HAKeeper client to be used by a proxy service.
//
// NB: caller must set a deadline on ctx and could specify options for morpc.Client via ctx.
func NewProxyHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (ProxyHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, sid, cfg)
}

func validateHAKeeperClientContext(ctx context.Context) error {
	if ctx == nil {
		return moerr.NewInvalidInputNoCtx("nil context")
	}
	if _, ok := ctx.Deadline(); !ok {
		return moerr.NewInvalidInput(ctx, "HAKeeper client context deadline not set")
	}
	return nil
}

func newManagedHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (*managedHAKeeperClient, error) {
	c, err := newHAKeeperClientFunc(ctx, sid, cfg)
	if err != nil {
		return nil, normalizeHAKeeperClientError(ctx, err)
	}

	mc := &managedHAKeeperClient{
		cfg:            cfg,
		sid:            sid,
		backendOptions: GetBackendOptions(ctx),
		clientOptions:  GetClientOptions(ctx),
	}
	mc.mu.client = c
	mc.mu.allocIDByKey = make(map[string]*allocID)
	return mc, nil
}

// allocID contains nextID and lastID.
type allocID struct {
	nextID uint64
	lastID uint64
}

type managedHAKeeperClient struct {
	sid string
	cfg HAKeeperClientConfig

	// Method `getPreparedClient` may update morpc.Client.
	// So we need to keep options for morpc.Client.
	backendOptions []morpc.BackendOption
	clientOptions  []morpc.ClientOption

	mu struct {
		sync.RWMutex
		// allocIDByKey is used to alloc IDs by different key.
		allocIDByKey map[string]*allocID
		// sharedAllocID is used to alloc global IDs.
		sharedAllocID allocID

		client *hakeeperClient
	}
}

func (c *managedHAKeeperClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.client == nil {
		return nil
	}
	return c.mu.client.close()
}

// CheckLogServiceHealth implements the ClusterHAKeeperClient interface.
func (c *managedHAKeeperClient) CheckLogServiceHealth(ctx context.Context) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		details, err := client.getClusterDetails(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				c.resetClientIfCurrent(client)
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if len(details.TNStores) == 0 {
			// there are no tn stores yet.
			return nil
		}
		var err1 error
		for _, tnStore := range details.TNStores {
			for _, shard := range tnStore.Shards {
				err1 = firstError(err1, client.checkLogServiceHealth(
					ctx,
					pb.CheckHealth{
						ShardID: shard.ShardID,
					},
				))
			}
		}
		return err1
	}
}

func (c *managedHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.ClusterDetails{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.ClusterDetails{}, err
				}
				continue
			}
			return pb.ClusterDetails{}, err
		}
		cd, err := client.getClusterDetails(ctx)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.ClusterDetails{}, err
			}
			continue
		}
		return cd, err
	}
}

func (c *managedHAKeeperClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CheckerState{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CheckerState{}, err
				}
				continue
			}
			return pb.CheckerState{}, err
		}
		s, err := client.getClusterState(ctx)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CheckerState{}, err
			}
			continue
		}
		return s, err
	}
}

func (c *managedHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	batchSize := c.cfg.AllocateIDBatch
	for {
		if c.mu.sharedAllocID.nextID != 0 &&
			c.mu.sharedAllocID.nextID <= c.mu.sharedAllocID.lastID {
			v := c.mu.sharedAllocID.nextID
			c.mu.sharedAllocID.nextID++
			if v == 0 {
				logutil.Error("id should not be 0",
					zap.Uint64("nextID", c.mu.sharedAllocID.nextID),
					zap.Uint64("lastID", c.mu.sharedAllocID.lastID))
			}
			return v, nil
		}

		if err := c.prepareClientLocked(ctx); err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}
		firstID, err := sendCNAllocateIDFunc(c.mu.client, ctx, "", batchSize)

		if err != nil {
			if shouldResetHAKeeperClient(err) {
				c.resetClientLocked()
			}
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			logutil.Error("failed to allocate id",
				zap.Error(err),
				zap.Uint64("batch", c.cfg.AllocateIDBatch),
				zap.Uint64("nextID", c.mu.sharedAllocID.nextID),
				zap.Uint64("lastID", c.mu.sharedAllocID.lastID),
			)
			return 0, err
		}

		c.mu.sharedAllocID.nextID = firstID + 1
		c.mu.sharedAllocID.lastID = firstID + batchSize - 1
		return firstID, err
	}
}

// AllocateIDByKey implements the basicHAKeeperClient interface.
func (c *managedHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return 0, err
	}
	if key == "" {
		return c.AllocateID(ctx)
	}
	return c.AllocateIDByKeyWithBatch(ctx, key, c.cfg.AllocateIDBatch)
}

func (c *managedHAKeeperClient) AllocateIDByKeyWithBatch(
	ctx context.Context,
	key string,
	batchSize uint64) (uint64, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return 0, err
	}
	// empty key is used in shared allocated IDs.
	if len(key) == 0 {
		return 0, moerr.NewInternalError(ctx, "key should not be empty")
	}
	if batchSize == 0 {
		return 0, moerr.NewInvalidInput(ctx, "batch size must be greater than zero")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		allocIDs, ok := c.mu.allocIDByKey[key]
		if !ok {
			allocIDs = &allocID{nextID: 0, lastID: 0}
			c.mu.allocIDByKey[key] = allocIDs
		}

		if allocIDs.nextID != 0 && allocIDs.nextID <= allocIDs.lastID {
			v := allocIDs.nextID
			allocIDs.nextID++
			return v, nil
		}

		if err := c.prepareClientLocked(ctx); err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}
		firstID, err := sendCNAllocateIDFunc(c.mu.client, ctx, key, batchSize)
		if err != nil {
			if shouldResetHAKeeperClient(err) {
				c.resetClientLocked()
			}
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}

		allocIDs.nextID = firstID + 1
		allocIDs.lastID = firstID + batchSize - 1
		return firstID, err
	}
}

// AllocateIDByKeyWithRequestID allocates keyed IDs idempotently for callers
// that retain requestID across retries and restarts. This is intentionally not
// part of basicHAKeeperClient: only bootstrap lock acquisition needs the
// persistent request identity.
func (c *managedHAKeeperClient) AllocateIDByKeyWithRequestID(
	ctx context.Context,
	key string,
	batchSize uint64,
	requestID string,
) (uint64, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return 0, err
	}
	if key == "" {
		return 0, moerr.NewInternalError(ctx, "key should not be empty")
	}
	if batchSize != 1 {
		return 0, moerr.NewInvalidInput(ctx, "idempotent keyed allocation batch size must be one")
	}
	if requestID == "" {
		return 0, moerr.NewInvalidInput(ctx, "request ID should not be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if err := c.prepareClientLocked(ctx); err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}
		id, err := sendCNAllocateIDWithRequestIDFunc(c.mu.client, ctx, key, batchSize, requestID)
		if err != nil {
			if shouldResetHAKeeperClient(err) {
				c.resetClientLocked()
			}
			if c.isRetryableError(err) {
				if err := c.waitRetryLocked(ctx); err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}
		return id, nil
	}
}

func (c *managedHAKeeperClient) SendCNHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CommandBatch{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CommandBatch{}, err
				}
				continue
			}
			return pb.CommandBatch{}, err
		}
		result, err := client.sendCNHeartbeat(ctx, hb)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CommandBatch{}, err
			}
			continue
		}
		return result, err
	}
}

func (c *managedHAKeeperClient) SendTNHeartbeat(ctx context.Context,
	hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CommandBatch{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CommandBatch{}, err
				}
				continue
			}
			return pb.CommandBatch{}, err
		}
		cb, err := client.sendTNHeartbeat(ctx, hb)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CommandBatch{}, err
			}
			continue
		}
		return cb, err
	}
}

// GetScheduleCommands reads pending commands without mutating HAKeeper state.
// Capability negotiation turns this into a local no-op against an older
// HAKeeper, whose heartbeat responses preserve the legacy delivery path.
func (c *managedHAKeeperClient) GetScheduleCommands(
	ctx context.Context,
	serviceType pb.ServiceType,
) (pb.CommandBatch, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CommandBatch{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CommandBatch{}, err
				}
				continue
			}
			return pb.CommandBatch{}, err
		}
		if !client.commandDeliverySupported.Load() {
			return pb.CommandBatch{}, nil
		}
		// The read-only endpoint is safe as soon as the server binary advertises
		// delivery support. Before the replicated barrier it hides zero-ID legacy
		// batches; during preparation it can already return durable batches while
		// the heartbeat RPC is blocked. Do not gate this on CommandPollSupported,
		// which is intentionally false until phase two and would reintroduce the
		// startup liveness dependency this independent read is meant to remove.
		batch, err := client.getScheduleCommands(ctx, c.sid, serviceType)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CommandBatch{}, err
			}
			continue
		}
		return batch, err
	}
}

func (c *managedHAKeeperClient) SendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CommandBatch{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CommandBatch{}, err
				}
				continue
			}
			return pb.CommandBatch{}, err
		}
		cb, err := client.sendLogHeartbeat(ctx, hb)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CommandBatch{}, err
			}
			continue
		}
		return cb, err
	}
}

// GetCNState implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) GetCNState(ctx context.Context) (pb.CNState, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CNState{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CNState{}, err
				}
				continue
			}
			return pb.CNState{}, err
		}
		s, err := client.getCNState(ctx)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CNState{}, err
			}
			continue
		}
		return s, err
	}
}

// UpdateCNLabel implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateCNLabel(
	ctx context.Context, label pb.CNStoreLabel,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.updateCNLabel(ctx, label)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// UpdateCNWorkState implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateCNWorkState(
	ctx context.Context, state pb.CNWorkState,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.updateCNWorkState(ctx, state)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// PatchCNStore implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) PatchCNStore(
	ctx context.Context, stateLabel pb.CNStateLabel,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.patchCNStore(ctx, stateLabel)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// DeleteCNStore implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) DeleteCNStore(
	ctx context.Context, cnStore pb.DeleteCNStore,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.deleteCNStore(ctx, cnStore)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// SendProxyHeartbeat implements the ProxyHAKeeperClient interface.
func (c *managedHAKeeperClient) SendProxyHeartbeat(
	ctx context.Context, hb pb.ProxyHeartbeat,
) (pb.CommandBatch, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.CommandBatch{}, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return pb.CommandBatch{}, err
				}
				continue
			}
			return pb.CommandBatch{}, err
		}
		cb, err := client.sendProxyHeartbeat(ctx, hb)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return pb.CommandBatch{}, err
			}
			continue
		}
		return cb, err
	}
}

// GetBackupData implements the BRHAKeeperClient interface.
func (c *managedHAKeeperClient) GetBackupData(ctx context.Context) ([]byte, error) {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return nil, err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		s, err := client.getBackupData(ctx)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return nil, err
			}
			continue
		}
		return s, err
	}
}

// UpdateNonVotingReplicaNum implements the CNHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateNonVotingReplicaNum(
	ctx context.Context, num uint64,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.updateNonVotingReplicaNum(ctx, num)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

// UpdateNonVotingLocality implements the CNHAKeeperClient interface.
func (c *managedHAKeeperClient) UpdateNonVotingLocality(
	ctx context.Context, locality pb.Locality,
) error {
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return err
	}
	for {
		client, err := c.getPreparedClient(ctx)
		if err != nil {
			if c.isRetryableError(err) {
				if err := c.waitRetry(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		err = client.updateNonVotingLocality(ctx, locality)
		if shouldResetHAKeeperClient(err) {
			c.resetClientIfCurrent(client)
		}
		if c.isRetryableError(err) {
			if err := c.waitRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return err
	}
}

func (c *managedHAKeeperClient) isRetryableError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		logutil.IsExpectedConnectionCloseError(err) ||
		moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) ||
		moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF)
}

func shouldResetHAKeeperClient(err error) bool {
	// A caller-scoped cancellation does not mean the shared transport is broken.
	// MORPC discards the timed-out future, while its read loop independently
	// detects and reconnects a failed transport.
	return err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

// resetClientIfCurrent invalidates only the client generation used by the
// failed request. A late failure from an older request must not close a client
// that another goroutine has already prepared and published.
func (c *managedHAKeeperClient) resetClientIfCurrent(client *hakeeperClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.client != client {
		return
	}
	c.resetClientLocked()
}

// getPreparedClient prepares and snapshots one client generation under the
// same lock. The caller may use the snapshot without holding c.mu; a concurrent
// reset can close it, in which case the request returns an error and retries.
func (c *managedHAKeeperClient) getPreparedClient(ctx context.Context) (*hakeeperClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.prepareClientLocked(ctx); err != nil {
		return nil, err
	}
	if c.mu.client == nil {
		return nil, moerr.NewNoHAKeeper(ctx)
	}
	return c.mu.client, nil
}

func (c *managedHAKeeperClient) resetClientLocked() {
	if c.mu.client != nil {
		cc := c.mu.client
		c.mu.client = nil
		if err := cc.close(); err != nil {
			logutil.Error("failed to close client", zap.Error(err))
		}
	}
}

func (c *managedHAKeeperClient) prepareClientLocked(ctx context.Context) error {
	if c.mu.client != nil {
		return nil
	}

	// we must use the recoreded options for morpc.Client
	ctx = SetBackendOptions(ctx, c.backendOptions...)
	ctx = SetClientOptions(ctx, c.clientOptions...)

	cc, err := newHAKeeperClientFunc(ctx, c.sid, c.cfg)
	if err != nil {
		return normalizeHAKeeperClientError(ctx, err)
	}
	c.mu.client = cc
	return nil
}

func (c *managedHAKeeperClient) waitRetry(ctx context.Context) error {
	timer := time.NewTimer(hakeeperClientRetryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *managedHAKeeperClient) waitRetryLocked(ctx context.Context) error {
	c.mu.Unlock()
	defer c.mu.Lock()
	return c.waitRetry(ctx)
}

func normalizeHAKeeperClientError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*moerr.Error); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		logutil.IsExpectedConnectionCloseError(err) {
		return moerr.NewUnexpectedEOF(ctx, err.Error())
	}
	return err
}

type hakeeperClient struct {
	cfg                      HAKeeperClientConfig
	client                   morpc.RPCClient
	addr                     string
	sid                      string
	pool                     *sync.Pool
	respPool                 *sync.Pool
	backendOptions           []morpc.BackendOption
	clientOptions            []morpc.ClientOption
	pollMu                   sync.Mutex
	pollClient               morpc.RPCClient
	closed                   bool
	commandPollSupported     atomic.Bool
	commandDeliverySupported atomic.Bool
}

func newHAKeeperClient(
	ctx context.Context,
	sid string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	var err error
	var c *hakeeperClient
	// If the discovery address is configured, we used it first.
	if len(cfg.DiscoveryAddress) > 0 {
		c, err = connectByReverseProxy(ctx, sid, cfg.DiscoveryAddress, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	} else if len(cfg.ServiceAddresses) > 0 {
		c, err = connectToHAKeeper(ctx, sid, cfg.ServiceAddresses, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewNoHAKeeper(ctx)
}

func connectByReverseProxy(
	ctx context.Context,
	sid string,
	discoveryAddress string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	si, ok, err := GetShardInfo(sid, discoveryAddress, hakeeper.DefaultHAKeeperShardID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	addresses := make([]string, 0)
	leaderAddress, ok := si.Replicas[si.ReplicaID]
	if ok {
		addresses = append(addresses, leaderAddress)
	}
	for replicaID, address := range si.Replicas {
		if replicaID != si.ReplicaID {
			addresses = append(addresses, address)
		}
	}
	return connectToHAKeeper(ctx, sid, addresses, cfg)
}

func connectToHAKeeper(
	ctx context.Context,
	sid string,
	targets []string,
	cfg HAKeeperClientConfig,
) (*hakeeperClient, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	c := &hakeeperClient{
		cfg:            cfg,
		sid:            sid,
		pool:           pool,
		respPool:       respPool,
		backendOptions: append([]morpc.BackendOption(nil), GetBackendOptions(ctx)...),
		clientOptions:  append([]morpc.ClientOption(nil), GetClientOptions(ctx)...),
	}
	var e error
	addresses := append([]string{}, targets...)
	rand.Shuffle(len(addresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	for _, addr := range addresses {
		cc, err := getRPCClient(
			ctx,
			sid,
			addr,
			c.respPool,
			defaultMaxMessageSize,
			cfg.EnableCompress,
			defaultBackendReadTimeout,
			"connectToHAKeeper",
		)
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		isHAKeeper, err := c.checkIsHAKeeper(ctx)
		logutil.Info(fmt.Sprintf("isHAKeeper: %t, err: %v", isHAKeeper, err))
		if err == nil && isHAKeeper {
			return c, nil
		} else if err != nil {
			e = err
		}
		if err := cc.Close(); err != nil {
			logutil.Error("failed to close the client", zap.Error(err))
		}
	}
	if e == nil {
		// didn't encounter any error
		return nil, moerr.NewNoHAKeeper(ctx)
	}
	return nil, e
}

func (c *hakeeperClient) close() error {
	if c == nil {
		panic("!!!")
	}

	c.pollMu.Lock()
	c.closed = true
	pollClient := c.pollClient
	c.pollClient = nil
	c.pollMu.Unlock()

	var err error
	if c.client != nil {
		err = c.client.Close()
	}
	if pollClient != nil {
		err = errors.Join(err, pollClient.Close())
	}
	return err
}

func (c *hakeeperClient) getClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_DETAILS,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.ClusterDetails{}, err
	}
	return *resp.ClusterDetails, nil
}

func (c *hakeeperClient) getClusterState(ctx context.Context) (pb.CheckerState, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_STATE,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CheckerState{}, err
	}
	return *resp.CheckerState, nil
}

func (c *hakeeperClient) checkLogServiceHealth(ctx context.Context, checkHealth pb.CheckHealth) error {
	req := pb.Request{
		Method:      pb.CHECK_HEALTH,
		CheckHealth: &checkHealth,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		return moerr.NewInternalError(ctx, resp.ErrorMessage)
	}
	return nil
}

func (c *hakeeperClient) sendCNHeartbeat(
	ctx context.Context, hb pb.CNStoreHeartbeat,
) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.CN_HEARTBEAT,
		CNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendCNAllocateID(
	ctx context.Context, key string, batch uint64,
) (uint64, error) {
	req := pb.Request{
		Method:       pb.CN_ALLOCATE_ID,
		CNAllocateID: &pb.CNAllocateID{Key: key, Batch: batch},
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.AllocateID.FirstID, nil
}

func (c *hakeeperClient) sendCNAllocateIDWithRequestID(
	ctx context.Context, key string, batch uint64, requestID string,
) (uint64, error) {
	req := pb.Request{
		Method: pb.CN_ALLOCATE_ID,
		CNAllocateID: &pb.CNAllocateID{
			Key:       key,
			Batch:     batch,
			RequestID: requestID,
		},
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.AllocateID.FirstID, nil
}

func (c *hakeeperClient) sendTNHeartbeat(ctx context.Context,
	hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.TN_HEARTBEAT,
		TNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) getScheduleCommands(
	ctx context.Context,
	serviceID string,
	serviceType pb.ServiceType,
) (pb.CommandBatch, error) {
	req := pb.Request{
		Method: pb.GET_SCHEDULE_COMMANDS,
		ScheduleCommandQuery: &pb.ScheduleCommandQuery{
			UUID:        serviceID,
			ServiceType: serviceType,
		},
	}
	client, err := c.getScheduleCommandClient(ctx)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	return c.sendHeartbeatWithClient(ctx, client, req)
}

// getScheduleCommandClient owns a transport that is independent from the
// heartbeat transport. MORPC dispatches requests synchronously per connection,
// so a second goroutine on the primary client would still queue behind a blocked
// heartbeat handler on the server.
func (c *hakeeperClient) getScheduleCommandClient(ctx context.Context) (morpc.RPCClient, error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	if c.closed {
		return nil, moerr.NewNoHAKeeper(ctx)
	}
	if c.pollClient != nil {
		return c.pollClient, nil
	}
	pollCtx := ctx
	if len(c.backendOptions) > 0 {
		pollCtx = SetBackendOptions(pollCtx, c.backendOptions...)
	}
	if len(c.clientOptions) > 0 {
		pollCtx = SetClientOptions(pollCtx, c.clientOptions...)
	}
	client, err := getRPCClient(
		pollCtx,
		c.sid,
		c.addr,
		c.respPool,
		defaultMaxMessageSize,
		c.cfg.EnableCompress,
		defaultBackendReadTimeout,
		"schedule-command-poll",
	)
	if err != nil {
		return nil, err
	}
	c.pollClient = client
	return client, nil
}

func (c *hakeeperClient) sendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:       pb.LOG_HEARTBEAT,
		LogHeartbeat: &hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	for _, cmd := range cb.Commands {
		logutil.Info("hakeeper client received cmd", zap.String("cmd", cmd.LogString()))
	}
	return cb, nil
}

func (c *hakeeperClient) sendHeartbeat(ctx context.Context,
	req pb.Request) (pb.CommandBatch, error) {
	return c.sendHeartbeatWithClient(ctx, c.client, req)
}

func (c *hakeeperClient) sendHeartbeatWithClient(
	ctx context.Context,
	client morpc.RPCClient,
	req pb.Request,
) (pb.CommandBatch, error) {
	resp, err := c.requestWithClient(ctx, client, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	if resp.CommandPollSupported {
		c.commandPollSupported.Store(true)
		// A server can be upgraded in place while this client generation stays
		// connected. Poll support is stronger evidence than the initial
		// CHECK_HAKEEPER capability bit, so promote the delivery capability too.
		c.commandDeliverySupported.Store(true)
	}
	if resp.CommandBatch == nil {
		return pb.CommandBatch{}, nil
	}
	return *resp.CommandBatch, nil
}

func (c *hakeeperClient) getCNState(ctx context.Context) (pb.CNState, error) {
	s, err := c.getClusterState(ctx)
	if err != nil {
		return pb.CNState{}, err
	}
	return s.CNState, nil
}

func (c *hakeeperClient) updateCNLabel(ctx context.Context, label pb.CNStoreLabel) error {
	req := pb.Request{
		Method:       pb.UPDATE_CN_LABEL,
		CNStoreLabel: &label,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) updateCNWorkState(ctx context.Context, state pb.CNWorkState) error {
	req := pb.Request{
		Method:      pb.UPDATE_CN_WORK_STATE,
		CNWorkState: &state,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) patchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error {
	req := pb.Request{
		Method:       pb.PATCH_CN_STORE,
		CNStateLabel: &stateLabel,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) deleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error {
	req := pb.Request{
		Method:        pb.DELETE_CN_STORE,
		DeleteCNStore: &cnStore,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) sendProxyHeartbeat(
	ctx context.Context, hb pb.ProxyHeartbeat,
) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:         pb.PROXY_HEARTBEAT,
		ProxyHeartbeat: &hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	return cb, nil
}

func (c *hakeeperClient) updateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	req := pb.Request{
		Method:              pb.UPDATE_NON_VOTING_REPLICA_NUM,
		NonVotingReplicaNum: num,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) updateNonVotingLocality(
	ctx context.Context, locality pb.Locality,
) error {
	req := pb.Request{
		Method:            pb.UPDATE_NON_VOTING_LOCALITY,
		NonVotingLocality: &locality,
	}
	_, err := c.request(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *hakeeperClient) checkIsHAKeeper(ctx context.Context) (bool, error) {
	req := pb.Request{
		Method: pb.CHECK_HAKEEPER,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return false, err
	}
	return c.acceptHAKeeperResponse(resp), nil
}

func (c *hakeeperClient) acceptHAKeeperResponse(resp pb.Response) bool {
	if !resp.IsHAKeeper {
		return false
	}
	// connectToHAKeeper reuses this object while trying candidate addresses.
	// Publish capabilities only for the endpoint that passed admission, and
	// replace both bits so a rejected/newer candidate cannot contaminate the
	// next accepted generation.
	c.commandPollSupported.Store(resp.CommandPollSupported)
	c.commandDeliverySupported.Store(resp.CommandDeliverySupported)
	return true
}

func (c *hakeeperClient) request(ctx context.Context, req pb.Request) (pb.Response, error) {
	if c == nil {
		return pb.Response{}, moerr.NewNoHAKeeper(ctx)
	}
	return c.requestWithClient(ctx, c.client, req)
}

func (c *hakeeperClient) requestWithClient(
	ctx context.Context,
	client morpc.RPCClient,
	req pb.Request,
) (pb.Response, error) {
	if c == nil {
		return pb.Response{}, moerr.NewNoHAKeeper(ctx)
	}
	if client == nil {
		return pb.Response{}, moerr.NewNoHAKeeper(ctx)
	}
	if err := validateHAKeeperClientContext(ctx); err != nil {
		return pb.Response{}, err
	}
	ctx, span := trace.Debug(ctx, "hakeeperClient.request")
	defer span.End()
	r := c.pool.Get().(*RPCRequest)
	r.Request = req
	future, err := client.Send(ctx, c.addr, r)
	if err != nil {
		return pb.Response{}, normalizeHAKeeperClientError(ctx, err)
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.Response{}, normalizeHAKeeperClientError(ctx, err)
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(ctx, response.Response)
	if err != nil {
		return pb.Response{}, err
	}
	return resp, nil
}

func (c *hakeeperClient) getBackupData(ctx context.Context) ([]byte, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_STATE,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return nil, err
	}
	p := pb.BackupData{
		NextID:      resp.CheckerState.NextId,
		NextIDByKey: resp.CheckerState.NextIDByKey,
	}
	bs, err := p.Marshal()
	if err != nil {
		return nil, err
	}
	return bs, nil
}
