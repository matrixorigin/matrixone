package dist

import (
	"encoding/json"
	"errors"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/config"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	adb "matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle"
	"time"

	"github.com/matrixorigin/matrixcube/aware"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/proxy"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	cstorage "github.com/matrixorigin/matrixcube/storage"
)

const (
	defaultRPCTimeout = time.Second * 10
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
	PrefixKeys([]byte, uint64) ([][]byte, error)
	PrefixKeysWithGroup([]byte, uint64, pb.Group) ([][]byte, error)
	AllocID([]byte) (uint64, error)
	Append(string, uint64, []byte) error
	GetSnapshot(dbi.GetSnapshotCtx) (*handle.Snapshot, error)
	GetSegmentIds(string, uint64) (adb.IDS, error)
	GetSegmentedId(uint64) (uint64, error)
	CreateTablet(name string, shardId uint64, tbl *aoe.TableInfo) error
	DropTablet(string, uint64) (uint64, error)
	TabletIDs() ([]uint64, error)
	TabletNames(uint64) ([]string, error)
	// Exec exec command
	Exec(cmd interface{}) ([]byte, error)
	// AsyncExec async exec command
	AsyncExec(interface{}, func(interface{}, []byte, error), interface{})
	// ExecWithGroup exec command with group
	ExecWithGroup(interface{}, pb.Group) ([]byte, error)
	// AsyncExecWithGroup async exec command with group
	AsyncExecWithGroup(interface{}, pb.Group, func(interface{}, []byte, error), interface{})
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
	AOEStore() *adb.DB
}

type driver struct {
	cfg   *config.Config
	app   *server.Application
	store raftstore.Store
	spool raftstore.ShardsPool
	aoeDB *adb.DB
	cmds  map[uint64]raftcmdpb.CMDType
}

// NewCubeDriver returns a aoe request handler
func NewCubeDriver(
	metadataStorage cstorage.MetadataStorage,
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage) (CubeDriver, error) {
	return NewCubeDriverWithOptions(metadataStorage, kvDataStorage, aoeDataStorage, &config.Config{})
}

// NewCubeDriverWithOptions returns an aoe request handler
func NewCubeDriverWithOptions(
	metaStorage cstorage.MetadataStorage,
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage,
	c *config.Config) (CubeDriver, error) {

	return NewCubeDriverWithFactory(metaStorage, kvDataStorage, aoeDataStorage, c, func(cfg *cConfig.Config) (raftstore.Store, error) {
		return raftstore.NewStore(cfg), nil
	})
}

// NewCubeDriverWithFactory creates the cube driver with  raftstore factory
func NewCubeDriverWithFactory(
	metaStorage cstorage.MetadataStorage,
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage,
	c *config.Config,
	raftStoreFactory func(*cConfig.Config) (raftstore.Store, error)) (CubeDriver, error) {

	h := &driver{
		cfg:   c,
		aoeDB: aoeDataStorage.(*daoe.Storage).DB,
		cmds:  make(map[uint64]raftcmdpb.CMDType),
	}
	c.CubeConfig.Customize.CustomSplitCompletedFuncFactory = func(group uint64) func(old *bhmetapb.Shard, news []bhmetapb.Shard) {
		switch group {
		case uint64(pb.AOEGroup):
			return func(old *bhmetapb.Shard, news []bhmetapb.Shard) {
				//panic("not impl")
			}
		default:
			return func(old *bhmetapb.Shard, news []bhmetapb.Shard) {

			}
		}
	}
	c.CubeConfig.Storage.MetaStorage = metaStorage
	c.CubeConfig.Storage.DataStorageFactory = func(group, shardID uint64) cstorage.DataStorage {
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

	c.CubeConfig.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard {
		var initialGroups []bhmetapb.Shard
		initialGroups = append(initialGroups, bhmetapb.Shard{
			Group: uint64(pb.KVGroup),
		})
		return initialGroups
	}

	c.CubeConfig.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
		return h
	}

	/*c.CubeConfig.Customize.CustomAdjustCompactFuncFactory = func(group uint64) func(shard bhmetapb.Shard, compactIndex uint64) (newCompactIdx uint64, err error) {
		return func(shard bhmetapb.Shard, compactIndex uint64) (newCompactIdx uint64, err error) {
			if group != uint64(pb.AOEGroup) {
				return compactIndex, nil
			}
			return h.GetSegmentedId(shard.ID)
		}
	}*/

	/*c.CubeConfig.Customize.CustomAdjustInitAppliedIndexFactory = func(group uint64) func(shard bhmetapb.Shard, initAppliedIndex uint64) (adjustAppliedIndex uint64) {
		return func(shard bhmetapb.Shard, initAppliedIndex uint64) (adjustAppliedIndex uint64) {
			 if group != uint64(pb.AOEGroup) {
			 	return initAppliedIndex
			 }
			 adjustAppliedIndex, err := h.aoeDB.GetSegmentedId(dbi.GetSegmentedIdCtx{
			 	Matchers: []*dbi.StringMatcher{
			 		{
			 			Type:    dbi.MTPrefix,
			 			Pattern: codec.Uint642String(shard.ID),
			 		},
			 	},
			 })
			 if err != nil {
				if err == adb.ErrNotFound {
			 		log.Errorf("shard not found, %d, %d", group, shard.ID)
			 		return initAppliedIndex
			 	}
			 	panic(err)
			 }
			 return adjustAppliedIndex
			return initAppliedIndex
		}
	}
	*/
	store, err := raftStoreFactory(&c.CubeConfig)
	if err != nil {
		return nil, err
	}
	h.store = store

	c.ServerConfig.Store = h.store
	c.ServerConfig.Handler = h
	pConfig.DefaultSchedulers = nil

	h.app = server.NewApplicationWithDispatcher(c.ServerConfig, func(req *raftcmdpb.Request, cmd interface{}, proxy proxy.ShardsProxy) error {
		if req.Group == uint64(pb.KVGroup) {
			return proxy.Dispatch(req)
		}
		args := cmd.(pb.Request)
		if args.Shard == 0 {
			return proxy.Dispatch(req)
		}
		req.ToShard = args.Shard
		return proxy.DispatchTo(req, args.Shard, c.ServerConfig.Store.GetRouter().LeaderPeerStore(req.ToShard).ClientAddr)
	})
	h.init()
	return h, nil
}

func (h *driver) Start() error {
	err := h.app.Start()
	if err != nil {
		return err
	}
	return h.initShardPool()
}

func (h *driver) initShardPool() error {
	p, err := h.store.CreateResourcePool(metapb.ResourcePool{Group: uint64(pb.AOEGroup), Capacity: h.cfg.ClusterConfig.PreAllocatedGroupNum, RangePrefix: codec.String2Bytes("aoe-")})
	if err != nil {
		return err
	}
	h.spool = p
	return nil
}

func (h *driver) Close() {
	h.app.Stop()
}

func (h *driver) AOEStore() *adb.DB {
	return h.aoeDB
}

func (h *driver) GetShardPool() raftstore.ShardsPool {
	return h.spool
}
func (h *driver) Set(key, value []byte) error {
	return h.SetWithGroup(key, value, pb.KVGroup)
}

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

func (h *driver) Scan(start []byte, end []byte, limit uint64) ([][]byte, error) {
	return h.ScanWithGroup(start, end, limit, pb.KVGroup)
}

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

func (h *driver) PrefixScan(prefix []byte, limit uint64) ([][]byte, error) {
	return h.PrefixScanWithGroup(prefix, limit, pb.KVGroup)
}

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

func (h *driver) AllocID(idkey []byte) (uint64, error) {
	req := pb.Request{
		Type:  pb.Incr,
		Group: pb.KVGroup,
		AllocID: pb.AllocIDRequest{
			Key: idkey,
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

func (h *driver) GetSegmentIds(tabletName string, toShard uint64) (ids adb.IDS, err error) {
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
	defer func() {
		logutil.Debugf("Exec of %v cost %d ms", cmd.(pb.Request).Type, time.Since(t0))
	}()
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *driver) AsyncExec(cmd interface{}, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithTimeout(cmd, cb, defaultRPCTimeout, arg)
}

func (h *driver) AsyncExecWithGroup(cmd interface{}, group pb.Group, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithGroupAndTimeout(cmd, uint64(group), cb, defaultRPCTimeout, arg)
}

func (h *driver) ExecWithGroup(cmd interface{}, group pb.Group) ([]byte, error) {
	return h.app.ExecWithGroup(cmd, uint64(group), defaultRPCTimeout)
}

func (h *driver) RaftStore() raftstore.Store {
	return h.store
}
