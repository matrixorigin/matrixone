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

package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	errDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/error"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle"

	"github.com/matrixorigin/matrixcube/aware"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	cstorage "github.com/matrixorigin/matrixcube/storage"
)

const (
	defaultRPCTimeout     = time.Second * 10
	defaultStartupTimeout = time.Second * 300
)

// CubeDriver implements distributed kv and aoe.
type CubeDriver interface {
	// Start the driver.
	Start() error
	// Close close the driver.
	Close()
	// GetShardPool return ShardsPool instance.
	GetShardPool() raftstore.ShardsPool
	// Set set key value.
	Set([]byte, []byte) error
	// SetWithGroup set key value in specific group.
	SetWithGroup([]byte, []byte, pb.Group) error
	// Set async set key value.
	AsyncSet([]byte, []byte, func(server.CustomRequest, []byte, error), interface{})
	// AsyncSetIfNotExist async set key value if key not exists.
	AsyncSetIfNotExist([]byte, []byte, func(server.CustomRequest, []byte, error), interface{})
	// Set async set key value in specific group.
	AsyncSetWithGroup([]byte, []byte, pb.Group, func(server.CustomRequest, []byte, error), interface{})
	// SetIfNotExist set key value if key not exists.
	SetIfNotExist([]byte, []byte) error
	// Get returns the value of key.
	Get([]byte) ([]byte, error)
	// GetWithGroup returns the value of key from specific group.
	GetWithGroup([]byte, pb.Group) ([]byte, error)
	// Delete remove the key from the store.
	Delete([]byte) error
	// DeleteIfExist remove the key from the store if key exists.
	DeleteIfExist([]byte) error
	// Scan scan [start,end) data
	Scan([]byte, []byte, uint64) ([][]byte, error)
	// ScanWithGroup scan [start,end) data in specific group.
	ScanWithGroup([]byte, []byte, uint64, pb.Group) ([][]byte, error)
	// PrefixScan scan k-vs which k starts with prefix.
	PrefixScan([]byte, uint64) ([][]byte, error)
	// PrefixScanWithGroup scan k-vs which k starts with prefix
	PrefixScanWithGroup([]byte, uint64, pb.Group) ([][]byte, error)
	// PrefixScan returns the values whose key starts with prefix.
	PrefixKeys([]byte, uint64) ([][]byte, error)
	// PrefixKeysWithGroup scans prefix with specific group.
	PrefixKeysWithGroup([]byte, uint64, pb.Group) ([][]byte, error)
	// AllocID allocs id.
	AllocID([]byte, uint64) (uint64, error)
	// AsyncAllocID async alloc id.
	AsyncAllocID([]byte, uint64, func(server.CustomRequest, []byte, error), interface{})
	// Append appends the data in the table
	Append(string, uint64, []byte) error
	//GetSnapshot gets the snapshot from the table.
	//If there's no segment, it returns an empty snapshot.
	GetSnapshot(dbi.GetSnapshotCtx) (*handle.Snapshot, error)
	//GetSegmentIds returns the ids of segments of the table.
	GetSegmentIds(string, uint64) (dbi.IDS, error)
	//GetSegmentedId returns the smallest segmente id among the tables with the shard.
	GetSegmentedId(uint64) (uint64, error)
	//CreateTablet creates a table in the storage.
	CreateTablet(name string, shardId uint64, tbl *aoe.TableInfo) error
	//DropTablet drops the table in the storage.
	DropTablet(string, uint64) (uint64, error)
	//CreateIndex creates an index
	CreateIndex(tableName string, indexInfo *aoe.IndexInfo, toShard uint64) error
	//DropIndex drops an index
	DropIndex(tableName, indexName string, toShard uint64) error
	// TabletIDs returns the ids of all the tables in the storage.
	TabletIDs() ([]uint64, error)
	// TabletNames returns the names of all the tables in the storage.
	TabletNames(uint64) ([]string, error)
	// Exec exec command
	Exec(cmd interface{}) ([]byte, error)
	// AsyncExec async exec command
	AsyncExec(interface{}, func(server.CustomRequest, []byte, error), interface{})
	// ExecWithGroup exec command with group
	ExecWithGroup(interface{}, pb.Group) ([]byte, error)
	// AsyncExecWithGroup async exec command with group
	AsyncExecWithGroup(interface{}, pb.Group, func(server.CustomRequest, []byte, error), interface{})
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
	//AOEStore returns h.aoeDB
	AOEStore() *aoedb.DB
}

type driver struct {
	cfg   *config.Config
	app   *server.Application
	store raftstore.Store
	spool raftstore.ShardsPool
	aoeDB *aoedb.DB
	cmds  map[uint64]rpc.CmdType
}

// NewCubeDriver returns a aoe request handler
func NewCubeDriver(
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage) (CubeDriver, error) {
	return NewCubeDriverWithOptions(kvDataStorage, aoeDataStorage, &config.Config{})
}

// NewCubeDriverWithOptions returns an aoe request handler
func NewCubeDriverWithOptions(
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage,
	c *config.Config) (CubeDriver, error) {

	return NewCubeDriverWithFactory(kvDataStorage, aoeDataStorage, c, func(cfg *cConfig.Config) (raftstore.Store, error) {
		return raftstore.NewStore(cfg), nil
	})
}
func ErrorResp1(err error, infos string) (CubeDriver, []byte) {
	buf := bytes.Buffer{}

	buf.Write(codec.String2Bytes(err.Error()))
	return nil, buf.Bytes()
}

// NewCubeDriverWithFactory creates the cube driver with raftstore factory
func NewCubeDriverWithFactory(
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage,
	c *config.Config,
	raftStoreFactory func(*cConfig.Config) (raftstore.Store, error)) (CubeDriver, error) {

	h := &driver{
		cfg:   c,
		aoeDB: aoeDataStorage.(*aoe3.Storage).DB,
		cmds:  make(map[uint64]rpc.CmdType),
	}
	c.CubeConfig.Customize.CustomSplitCompletedFuncFactory = func(group uint64) func(old *meta.Shard, news []meta.Shard) {
		switch group {
		case uint64(pb.AOEGroup):
			return func(old *meta.Shard, news []meta.Shard) {
				//TODO: Not impl
			}
		default:
			return func(old *meta.Shard, news []meta.Shard) {

			}
		}
	}
	c.CubeConfig.Storage.DataStorageFactory = func(group uint64) cstorage.DataStorage {
		switch group {
		case uint64(pb.KVGroup):
			return kvDataStorage
		case uint64(pb.AOEGroup):
			return aoeDataStorage
		}
		return nil
	}
	c.CubeConfig.Storage.ForeachDataStorageFunc = func(cb func(cstorage.DataStorage)) {
		cb(kvDataStorage)
		cb(aoeDataStorage)
	}
	c.CubeConfig.Prophet.Replication.Groups = []uint64{uint64(pb.KVGroup), uint64(pb.AOEGroup)}
	c.CubeConfig.ShardGroups = 2

	c.CubeConfig.Customize.CustomInitShardsFactory = func() []meta.Shard {
		var initialGroups []meta.Shard
		initialGroups = append(initialGroups, meta.Shard{
			Group: uint64(pb.KVGroup),
		})
		return initialGroups
	}

	c.CubeConfig.Customize.CustomShardPoolShardFactory = func(g uint64, start, end []byte, unique string, offsetInPool uint64) meta.Shard {
		return meta.Shard{
			Group:        g,
			Start:        start,
			End:          end,
			Unique:       unique,
			DisableSplit: true,
		}
	}

	c.CubeConfig.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
		return h
	}

	c.CubeConfig.Customize.CustomAdjustCompactFuncFactory = func(group uint64) func(shard meta.Shard, compactIndex uint64) (newCompactIdx uint64, err error) {
		return func(shard meta.Shard, compactIndex uint64) (newCompactIdx uint64, err error) {
			defer func() {
				logutil.Debugf("CompactIndex of [%d]shard-%d is adjusted from %d to %d", group, shard.ID, compactIndex, newCompactIdx)
			}()
			if group != uint64(pb.AOEGroup) {
				newCompactIdx = compactIndex
			} else {
				newCompactIdx = h.aoeDB.GetShardCheckpointId(shard.ID)
				if newCompactIdx == 0 {
					newCompactIdx = compactIndex
				}
			}
			return newCompactIdx, nil
		}
	}

	store, err := raftStoreFactory(&c.CubeConfig)
	if err != nil {
		return nil, err
	}
	h.store = store

	c.ServerConfig.Store = h.store
	pConfig.DefaultSchedulers = nil

	h.app = server.NewApplicationWithDispatcher(c.ServerConfig, func(req rpc.Request, cmd server.CustomRequest, proxy raftstore.ShardsProxy) error {
		if req.Group == uint64(pb.KVGroup) {
			return proxy.Dispatch(req)
		}
		args := cmd.Args.(pb.Request)
		if args.Shard == 0 {
			return proxy.Dispatch(req)
		}
		req.ToShard = args.Shard
		return proxy.DispatchTo(req, c.ServerConfig.Store.GetRouter().GetShard(req.ToShard),
			c.ServerConfig.Store.GetRouter().LeaderReplicaStore(req.ToShard).ClientAddr)
	})
	return h, nil
}

//Start starts h.app add initial the shard pool
func (h *driver) Start() error {
	err := h.app.Start()
	if err != nil {
		return err
	}
	timeoutC := time.After(defaultStartupTimeout)
	for {
		select {
		case <-timeoutC:
			logutil.Error("wait for available shard timeout")
			return errDriver.ErrStartupTimeout
		default:
			err := h.initShardPool()
			if err == nil {
				return err
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

//initShardPool creates a shard pool by h.store and record it in h.spool
func (h *driver) initShardPool() error {
	p, err := h.store.CreateResourcePool(metapb.ResourcePool{Group: uint64(pb.AOEGroup), Capacity: h.cfg.ClusterConfig.PreAllocatedGroupNum, RangePrefix: codec.String2Bytes("aoe-")})
	if err != nil {
		return err
	}
	h.spool = p
	return nil
}

//Close stop h.app
func (h *driver) Close() {
	h.app.Stop()
}

//AOEStore returns h.aoeDB
func (h *driver) AOEStore() *aoedb.DB {
	return h.aoeDB
}

//GetShardPool returns h.spool
func (h *driver) GetShardPool() raftstore.ShardsPool {
	return h.spool
}

// Set set key value.
func (h *driver) Set(key, value []byte) error {
	return h.SetWithGroup(key, value, pb.KVGroup)
}

// SetWithGroup set key value in specific group.
func (h *driver) SetWithGroup(key, value []byte, group pb.Group) error {
	req := pb.Request{
		Type:  pb.Set,
		Group: group,
		Set: pb.SetRequest{
			Key:   key,
			Value: value,
		},
	}
	_, err := h.ExecWithGroup(req, group)
	return err
}

//AsyncSet sets key and value in KVGroup asynchronously.
func (h *driver) AsyncSet(key, value []byte, cb func(server.CustomRequest, []byte, error), data interface{}) {
	h.AsyncSetWithGroup(key, value, pb.KVGroup, cb, data)
}

//AsyncSetWithGroup sets key and value in specific group asynchronously by calling h.AsyncExecWithGroup.
func (h *driver) AsyncSetWithGroup(key, value []byte, group pb.Group, cb func(server.CustomRequest, []byte, error), data interface{}) {
	req := pb.Request{
		Type:  pb.Set,
		Group: group,
		Set: pb.SetRequest{
			Key:   key,
			Value: value,
		},
	}
	h.AsyncExecWithGroup(req, group, cb, data)
}

func (h *driver) AsyncSetIfNotExist(key, value []byte, cb func(server.CustomRequest, []byte, error), data interface{}) {
	req := pb.Request{
		Type:  pb.SetIfNotExist,
		Group: pb.KVGroup,
		Set: pb.SetRequest{
			Key:   key,
			Value: value,
		},
	}
	h.AsyncExecWithGroup(req, pb.KVGroup, func(i server.CustomRequest, bytes []byte, err error) {
		if bytes != nil || len(bytes) != 0 {
			err = errors.New(string(bytes))
		}
		cb(i, bytes, err)
	}, data)
}

//SetIfNotExist sets key and value in KVGroup if the key doesn't exist.
func (h *driver) SetIfNotExist(key, value []byte) error {
	req := pb.Request{
		Type:  pb.SetIfNotExist,
		Group: pb.KVGroup,
		Set: pb.SetRequest{
			Key:   key,
			Value: value,
		},
	}
	rsp, err := h.ExecWithGroup(req, pb.KVGroup)
	if rsp != nil || len(rsp) != 0 {
		err = errors.New(string(rsp))
	}
	return err
}

//Get gets the key from KVGroup
func (h *driver) Get(key []byte) ([]byte, error) {
	return h.GetWithGroup(key, pb.KVGroup)
}

// GetWithGroup returns the value of key
func (h *driver) GetWithGroup(key []byte, group pb.Group) ([]byte, error) {
	req := pb.Request{
		Type:  pb.Get,
		Group: group,
		Get: pb.GetRequest{
			Key: key,
		},
	}
	value, err := h.ExecWithGroup(req, group)
	return value, err
}

//Delete deletes the key in KVGroup.
func (h *driver) Delete(key []byte) error {
	req := pb.Request{
		Type:  pb.Del,
		Group: pb.KVGroup,
		Delete: pb.DeleteRequest{
			Key: key,
		},
	}
	_, err := h.ExecWithGroup(req, pb.KVGroup)
	return err
}

//DeleteIfExist deletes the key if it exists in KVGroup
func (h *driver) DeleteIfExist(key []byte) error {
	req := pb.Request{
		Type:  pb.DelIfNotExist,
		Group: pb.KVGroup,
		Delete: pb.DeleteRequest{
			Key: key,
		},
	}
	_, err := h.ExecWithGroup(req, pb.KVGroup)
	return err
}

//Scan scans in KVGroup.
//It returns the keys and values whose key is between start and end.
func (h *driver) Scan(start []byte, end []byte, limit uint64) ([][]byte, error) {
	return h.ScanWithGroup(start, end, limit, pb.KVGroup)
}

//ScanWithGroup returns the keys and values whose key is between start and end.
func (h *driver) ScanWithGroup(start []byte, end []byte, limit uint64, group pb.Group) ([][]byte, error) {
	req := pb.Request{
		Type:  pb.Scan,
		Group: group,
		Scan: pb.ScanRequest{
			Start: start,
			End:   end,
			Limit: limit,
		},
	}
	var pairs [][]byte
	var err error
	var data []byte
	i := 0
	for {
		i = i + 1
		data, err = h.ExecWithGroup(req, group)
		if data == nil || err != nil {
			break
		}
		var kvs [][]byte
		err = json.Unmarshal(data, &kvs)
		if err != nil || kvs == nil || len(kvs) == 0 {
			break
		}
		if len(kvs)%2 == 0 {
			pairs = append(pairs, kvs...)
			break
		}

		pairs = append(pairs, kvs[0:len(kvs)-1]...)
		req.Scan.Start = kvs[len(kvs)-1]
	}
	return pairs, err
}

//PrefixScan scans in KVGroup
//It returns the kv pairs with specific prefix
func (h *driver) PrefixScan(prefix []byte, limit uint64) ([][]byte, error) {
	return h.PrefixScanWithGroup(prefix, limit, pb.KVGroup)
}

//PrefixScanWithGroup scan the kv pairs with the prefix in specific group
func (h *driver) PrefixScanWithGroup(prefix []byte, limit uint64, group pb.Group) ([][]byte, error) {
	req := pb.Request{
		Type:  pb.PrefixScan,
		Group: group,
		PrefixScan: pb.PrefixScanRequest{
			Prefix:   prefix,
			StartKey: prefix,
			Limit:    limit,
		},
	}
	var pairs [][]byte
	var err error
	var data []byte
	i := 0
	for {
		i = i + 1
		data, err = h.ExecWithGroup(req, group)
		if data == nil || err != nil {
			break
		}
		var kvs [][]byte
		err = json.Unmarshal(data, &kvs)
		if err != nil || kvs == nil || len(kvs) == 0 {
			break
		}
		if len(kvs)%2 == 0 {
			pairs = append(pairs, kvs...)
			break
		}

		pairs = append(pairs, kvs[0:len(kvs)-1]...)
		req.PrefixScan.StartKey = kvs[len(kvs)-1]
	}
	return pairs, err
}

//PrefixKeys scans in KVGroup.
//It returns the values whose key has specific prefix.
func (h *driver) PrefixKeys(prefix []byte, limit uint64) ([][]byte, error) {
	return h.PrefixKeysWithGroup(prefix, limit, pb.KVGroup)
}

func (h *driver) PrefixKeysWithGroup(prefix []byte, limit uint64, group pb.Group) ([][]byte, error) {
	req := pb.Request{
		Type:  pb.PrefixScan,
		Group: group,
		PrefixScan: pb.PrefixScanRequest{
			Prefix:   prefix,
			StartKey: prefix,
			Limit:    limit,
		},
	}
	var values [][]byte
	var err error
	var data []byte
	i := 0
	for {
		i = i + 1

		data, err = h.ExecWithGroup(req, group)
		if data == nil || err != nil {
			break
		}
		var kvs [][]byte
		err = json.Unmarshal(data, &kvs)
		if err != nil || kvs == nil || len(kvs) == 0 {
			break
		}

		for i := 0; i < len(kvs)-1; i += 2 {
			values = append(values, kvs[i])
		}

		if len(kvs)%2 == 0 {
			break
		}
		//req.PrefixScan.StartKey = raftstore.EncodeDataKey(uint64(group), kvs[len(kvs)-1])
		req.PrefixScan.StartKey = kvs[len(kvs)-1]
	}
	return values, err
}

func (h *driver) AllocID(idkey []byte, batch uint64) (uint64, error) {
	req := pb.Request{
		Type:  pb.Incr,
		Group: pb.KVGroup,
		AllocID: pb.AllocIDRequest{
			Key:   idkey,
			Batch: batch,
		},
	}
	data, err := h.ExecWithGroup(req, pb.KVGroup)
	if err != nil {
		return 0, err
	}
	resp, err := codec.Bytes2Uint64(data)
	if err != nil {
		return 0, err
	}
	return resp, nil
}

func (h *driver) AsyncAllocID(idkey []byte, batch uint64, cb func(server.CustomRequest, []byte, error), param interface{}) {
	req := pb.Request{
		Type:  pb.Incr,
		Group: pb.KVGroup,
		AllocID: pb.AllocIDRequest{
			Key:   idkey,
			Batch: batch,
		},
	}
	h.AsyncExecWithGroup(req, pb.KVGroup, cb, param)
}

func (h *driver) Append(name string, shardId uint64, data []byte) error {
	req := pb.Request{
		Type:  pb.Append,
		Group: pb.AOEGroup,
		Shard: shardId,
		Append: pb.AppendRequest{
			Data:       data,
			TabletName: name,
		},
	}
	rsp, err := h.ExecWithGroup(req, pb.AOEGroup)
	if rsp != nil || len(rsp) != 0 {
		err = errors.New(string(rsp))
	}
	return err
}

func (h *driver) GetSnapshot(ctx dbi.GetSnapshotCtx) (*handle.Snapshot, error) {
	ctxStr, err := json.Marshal(ctx)
	req := pb.Request{
		Type:  pb.GetSnapshot,
		Group: pb.AOEGroup,
		GetSnapshot: pb.GetSnapshotRequest{
			Ctx: ctxStr,
		},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return nil, err
	}
	var s handle.Snapshot
	err = json.Unmarshal(value, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (h *driver) GetSegmentIds(tabletName string, toShard uint64) (ids dbi.IDS, err error) {
	req := pb.Request{
		Type:  pb.GetSegmentIds,
		Group: pb.AOEGroup,
		Shard: toShard,
		GetSegmentIds: pb.GetSegmentIdsRequest{
			Name: tabletName,
		},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return ids, err
	}
	err = json.Unmarshal(value, &ids)
	if err != nil {
		return ids, err
	}
	return ids, nil
}

func (h *driver) GetSegmentedId(shardId uint64) (index uint64, err error) {
	req := pb.Request{
		Type:  pb.GetSegmentedId,
		Group: pb.AOEGroup,
		Shard: shardId,
		GetSegmentedId: pb.GetSegmentedIdRequest{
			ShardId: shardId,
		},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return index, err
	}
	return codec.Bytes2Uint64(value)
}

func (h *driver) CreateTablet(name string, toShard uint64, tbl *aoe.TableInfo) (err error) {
	info, _ := helper.EncodeTable(*tbl)
	req := pb.Request{
		Shard: toShard,
		Group: pb.AOEGroup,
		Type:  pb.CreateTablet,
		CreateTablet: pb.CreateTabletRequest{
			Name:      name,
			TableInfo: info,
		},
	}
	rsp, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return err
	}
	_, err = codec.Bytes2Uint64(rsp)
	if err != nil {
		err = errors.New(string(rsp))
	}
	return err
}

func (h *driver) DropTablet(name string, toShard uint64) (id uint64, err error) {
	req := pb.Request{
		Shard: toShard,
		Type:  pb.DropTablet,
		Group: pb.AOEGroup,
		DropTablet: pb.DropTabletRequest{
			Name: name,
		},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return id, err
	}
	return codec.Bytes2Uint64(value)
}
func (h *driver) CreateIndex(tableName string, indexInfo *aoe.IndexInfo, toShard uint64) error {
	idx, _ := helper.EncodeIndex(*indexInfo)
	req := pb.Request{
		Shard: toShard,
		Type:  pb.CreateIndex,
		Group: pb.AOEGroup,
		CreateIndex: pb.CreateIndexRequest{
			TableName: tableName,
			Indices:   idx,
		},
	}
	rsp, err := h.ExecWithGroup(req, pb.AOEGroup)
	if rsp != nil || len(rsp) != 0 {
		err = errors.New(string(rsp))
	}
	return err
}

func (h *driver) DropIndex(tableName, indexName string, toShard uint64) error {
	req := pb.Request{
		Shard: toShard,
		Type:  pb.DropIndex,
		Group: pb.AOEGroup,
		DropIndex: pb.DropIndexRequest{
			TableName: tableName,
			IndexName: indexName,
		},
	}
	rsp, err := h.ExecWithGroup(req, pb.AOEGroup)
	if rsp != nil || len(rsp) != 0 {
		err = errors.New(string(rsp))
	}
	return err
}

func (h *driver) TabletIDs() ([]uint64, error) {
	req := pb.Request{
		Type:      pb.TabletIds,
		Group:     pb.AOEGroup,
		TabletIds: pb.TabletIDsRequest{},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return nil, err
	}
	var rsp []uint64
	err = json.Unmarshal(value, &rsp)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (h *driver) TabletNames(toShard uint64) ([]string, error) {
	req := pb.Request{
		Shard:     toShard,
		Group:     pb.AOEGroup,
		Type:      pb.TabletNames,
		TabletIds: pb.TabletIDsRequest{},
	}
	value, err := h.ExecWithGroup(req, pb.AOEGroup)
	if err != nil {
		return nil, err
	}
	var rsp []string
	err = json.Unmarshal(value, &rsp)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (h *driver) Exec(cmd interface{}) ([]byte, error) {
	t0 := time.Now()
	cr := &server.CustomRequest{}
	h.BuildRequest(cr, cmd)
	defer func() {
		logutil.Debugf("Exec of %v cost %d ms", cmd.(pb.Request).Type, time.Since(t0).Milliseconds())
	}()
	return h.app.Exec(*cr, defaultRPCTimeout)
}

func (h *driver) AsyncExec(cmd interface{}, cb func(server.CustomRequest, []byte, error), arg interface{}) {
	cr := &server.CustomRequest{}
	h.BuildRequest(cr, cmd)
	h.app.AsyncExec(*cr, cb, defaultRPCTimeout)
}

func (h *driver) AsyncExecWithGroup(cmd interface{}, group pb.Group, cb func(server.CustomRequest, []byte, error), arg interface{}) {
	cr := &server.CustomRequest{}
	h.BuildRequest(cr, cmd)
	h.app.AsyncExec(*cr, cb, defaultRPCTimeout)
}

func (h *driver) ExecWithGroup(cmd interface{}, group pb.Group) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("Exec of %v cost %d ms", cmd.(pb.Request).Type, time.Since(t0).Milliseconds())
	}()
	cr := &server.CustomRequest{}
	h.BuildRequest(cr, cmd)
	return h.app.Exec(*cr, defaultRPCTimeout)
}

func (h *driver) RaftStore() raftstore.Store {
	return h.store
}
