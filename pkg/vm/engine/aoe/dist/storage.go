package dist

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/proxy"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	cstorage "github.com/matrixorigin/matrixcube/storage"
	"matrixone/pkg/vm/engine/aoe"
	"sync"
	"time"
)

const (
	defaultRPCTimeout = time.Second * 3
)

// Storage storage
type Storage interface {
	// Start the storage
	Start() error
	// Close close the storage
	Close()
	// Set set key value
	Set([]byte, []byte) error
	// SetWithTTL Set set key value with a TTL in seconds
	SetWithTTL([]byte, []byte, int64) error
	// SetWithGroupWithTTL set key value with a TTL in seconds
	SetWithGroupWithTTL([]byte, []byte, aoe.Group, int64) error
	// SetWithGroup set key value
	SetWithGroup([]byte, []byte, aoe.Group) error

	// Get returns the value of key
	Get([]byte) ([]byte, error)
	// GetWithGroup returns the value of key
	GetWithGroup([]byte, aoe.Group) ([]byte, error)
	// Delete remove the key from the store
	Delete([]byte) error
	// Scan scan [start,end) data
	Scan([]byte, []byte, uint64) ([][]byte, error)
	// ScanWithGroup Scan scan [start,end) data
	ScanWithGroup([]byte, []byte, uint64, aoe.Group) ([][]byte, error)
	// PrefixScan scan k-vs which k starts with prefix
	PrefixScan([]byte, uint64) ([][]byte, error)
	// PrefixScanWithGroup scan k-vs which k starts with prefix
	PrefixScanWithGroup([]byte, uint64, aoe.Group) ([][]byte, error)
	AllocID([]byte) (uint64, error)

	// Exec exec command
	Exec(cmd interface{}) ([]byte, error)
	// AsyncExec async exec command
	AsyncExec(interface{}, func(interface{}, []byte, error), interface{})
	// ExecWithGroup exec command with group
	ExecWithGroup(interface{}, aoe.Group) ([]byte, error)
	// AsyncExecWithGroup async exec command with group
	AsyncExecWithGroup(interface{}, aoe.Group, func(interface{}, []byte, error), interface{})
	// RaftStore returns the raft store
	RaftStore() raftstore.Store
}

type aoeStorage struct {
	app   *server.Application
	store raftstore.Store
	locks sync.Map // key -> lock
	cmds  map[uint64]raftcmdpb.CMDType
}

func (h *aoeStorage) Start() error {
	return h.app.Start()
}

func (h *aoeStorage) Close() {
	h.app.Stop()
}

// NewStorage returns a beehive request handler
func NewStorage(
	metadataStorage cstorage.MetadataStorage,
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage) (Storage, error) {
	return NewStorageWithOptions(metadataStorage, kvDataStorage, aoeDataStorage, nil, server.Cfg{})
}

// NewStorageWithOptions returns a beehive request handler
func NewStorageWithOptions(
	metaStorage cstorage.MetadataStorage,
	kvDataStorage cstorage.DataStorage,
	aoeDataStorage cstorage.DataStorage,
	adjustFunc func(cfg *config.Config),
	scfg server.Cfg) (Storage, error) {

	h := &aoeStorage{
		cmds: make(map[uint64]raftcmdpb.CMDType),
	}

	cfg := &config.Config{}
	cfg.Customize.CustomSplitCompletedFuncFactory = func(group uint64) func(old *bhmetapb.Shard, news []bhmetapb.Shard) {
		switch group {
		case uint64(aoe.AOEGroup):
			return func(old *bhmetapb.Shard, news []bhmetapb.Shard) {
				//panic("not impl")
			}
		default:
			return func(old *bhmetapb.Shard, news []bhmetapb.Shard) {

			}
		}
	}
	cfg.Storage.MetaStorage = metaStorage
	cfg.Storage.DataStorageFactory = func(group, shardID uint64) cstorage.DataStorage {
		switch group {
		case uint64(aoe.KVGroup):
			return kvDataStorage
		case uint64(aoe.AOEGroup):
			return aoeDataStorage
		}
		return nil
	}
	cfg.Storage.ForeachDataStorageFunc = func(cb func(cstorage.DataStorage)) {
		cb(kvDataStorage)
		cb(aoeDataStorage)
	}
	cfg.Prophet.Replication.Groups = []uint64{uint64(aoe.KVGroup), uint64(aoe.AOEGroup)}
	cfg.ShardGroups = 2
	cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard {
		return []bhmetapb.Shard{
			{
				Group: uint64(aoe.KVGroup),
			},
			{
				Group: uint64(aoe.AOEGroup),
				Start: []byte("0"),
				End:   []byte("1"),
			},
		}
	}

	cfg.Prophet.ResourceStateChangedHandler = func(res metadata.Resource, from metapb.ResourceState, to metapb.ResourceState) {
		if from == metapb.ResourceState_WaittingCreate && to == metapb.ResourceState_Running {

			if res.Data() == nil {
				return
			}
			header := format.MustBytesToUint64(res.Data()[0:8])
			keys := bytes.Split(res.Data()[8:8+header], []byte("#"))
			tKey := keys[0]
			rKey := []byte(fmt.Sprintf("%s%d", string(keys[1]), res.ID()))
			// TODO: Call local interface to create new tablet
			// TODO: Re-design group store and set value to <partition, segment_ids>
			_ = h.Set(rKey, []byte(res.Unique()))
			t, _ := aoe.DecodeTable(res.Data()[8+header:])
			t.State = aoe.StatePublic
			meta, _ := aoe.EncodeTable(t)
			_ = h.Set(tKey, meta)
		}
	}

	cfg.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
		return h
	}

	if adjustFunc != nil {
		adjustFunc(cfg)
	}

	h.store = raftstore.NewStore(cfg)
	scfg.Store = h.store
	scfg.Handler = h
	h.app = server.NewApplicationWithDispatcher(scfg, func(req *raftcmdpb.Request, cmd interface{}, proxy proxy.ShardsProxy) error {
		switch req.Group {
		case uint64(aoe.KVGroup):
			return proxy.Dispatch(req)
		case uint64(aoe.AOEGroup):
			args := cmd.(Args)
			req.ToShard = args.ShardId
			return proxy.DispatchTo(req, args.ShardId, proxy.Router().LeaderAddress(args.ShardId))
		}
		return ErrDispatchFailed

	})
	h.init()
	if err := h.app.Start(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *aoeStorage) Set(key, value []byte) error {
	return h.SetWithTTL(key, value, 0)
}

func (h *aoeStorage) SetWithTTL(key, value []byte, ttl int64) error {
	req := Args{
		Op: uint64(Set),
		Args: [][]byte{
			key,
			value,
		},
	}
	_, err := h.Exec(req)
	return err
}

func (h *aoeStorage) SetWithGroup(key, value []byte, group aoe.Group) error {
	return h.SetWithGroupWithTTL(key, value, group, 0)
}

func (h *aoeStorage) SetWithGroupWithTTL(key, value []byte, group aoe.Group, ttl int64) error {
	req := Args{
		Op: uint64(Set),
		Args: [][]byte{
			key,
			value,
		},
	}
	_, err := h.ExecWithGroup(req, group)
	return err
}

func (h *aoeStorage) Get(key []byte) ([]byte, error) {
	return h.GetWithGroup(key, aoe.KVGroup)
}

// GetWithGroup returns the value of key
func (h *aoeStorage) GetWithGroup(key []byte, group aoe.Group) ([]byte, error) {
	req := Args{
		Op: uint64(Get),
		Args: [][]byte{
			key,
		},
	}
	value, err := h.ExecWithGroup(req, group)
	return value, err
}

func (h *aoeStorage) Delete(key []byte) error {
	req := Args{
		Op: uint64(Del),
		Args: [][]byte{
			key,
		},
	}

	_, err := h.Exec(req)
	return err
}

func (h *aoeStorage) Scan(start []byte, end []byte, limit uint64) ([][]byte, error) {
	return h.ScanWithGroup(start, end, limit, aoe.KVGroup)
}

func (h *aoeStorage) ScanWithGroup(start []byte, end []byte, limit uint64, group aoe.Group) ([][]byte, error) {
	req := Args{
		Op: uint64(Scan),
		Args: [][]byte{
			start,
			end,
		},
		Limit: limit,
	}
	data, err := h.ExecWithGroup(req, group)
	if err != nil {
		return nil, err
	}
	var pairs [][]byte
	err = json.Unmarshal(data, &pairs)
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

func (h *aoeStorage) PrefixScan(prefix []byte, limit uint64) ([][]byte, error) {
	return h.PrefixScanWithGroup(prefix, limit, aoe.KVGroup)
}

func (h *aoeStorage) PrefixScanWithGroup(prefix []byte, limit uint64, group aoe.Group) ([][]byte, error) {
	startKey := prefix
	req := Args{
		Op: uint64(PrefixScan),
		Args: [][]byte{
			startKey,
			prefix,
		},
		Limit: limit,
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
		req.Args[0] = kvs[len(kvs)-1]
	}
	return pairs, err
}

func (h *aoeStorage) AllocID(idkey []byte) (uint64, error) {
	req := Args{
		Op: uint64(Incr),
		Args: [][]byte{
			idkey,
		},
	}
	data, err := h.Exec(req)
	if err != nil {
		return 0, err
	}
	resp := format.MustBytesToUint64(data)
	return resp, nil
}

func (h *aoeStorage) Exec(cmd interface{}) ([]byte, error) {
	return h.app.Exec(cmd, defaultRPCTimeout)
}

func (h *aoeStorage) AsyncExec(cmd interface{}, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithTimeout(cmd, cb, defaultRPCTimeout, arg)
}

func (h *aoeStorage) AsyncExecWithGroup(cmd interface{}, group aoe.Group, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithGroupAndTimeout(cmd, uint64(group), cb, defaultRPCTimeout, arg)
}

func (h *aoeStorage) ExecWithGroup(cmd interface{}, group aoe.Group) ([]byte, error) {
	return h.app.ExecWithGroup(cmd, uint64(group), defaultRPCTimeout)
}

func (h *aoeStorage) RaftStore() raftstore.Store {
	return h.store
}

func (h *aoeStorage) getStoreByGroup(group uint64, shard uint64) cstorage.DataStorage {
	return h.store.DataStorageByGroup(group, shard)
}
