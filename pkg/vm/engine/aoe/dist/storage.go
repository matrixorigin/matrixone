package dist

import (
	"encoding/json"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	cstorage "github.com/matrixorigin/matrixcube/storage"
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
	SetWithGroupWithTTL([]byte, []byte, Group, int64) error
	// SetWithGroup set key value
	SetWithGroup([]byte, []byte, Group) error

	// Get returns the value of key
	Get([]byte) ([]byte, error)
	// GetWithGroup returns the value of key
	GetWithGroup([]byte, Group) ([]byte, error)
	// Delete remove the key from the store
	Delete([]byte) error
	// Scan scan [start,end) data
	Scan([]byte, []byte, uint64) ([][]byte, error)
	// ScanWithGroup Scan scan [start,end) data
	ScanWithGroup([]byte, []byte, uint64, Group) ([][]byte, error)
	// PrefixScan scan k-vs which k starts with prefix
	PrefixScan([]byte, uint64) ([][]byte, error)
	// PrefixScanWithGroup scan k-vs which k starts with prefix
	PrefixScanWithGroup([]byte, uint64, Group) ([][]byte, error)
	AllocID([]byte) (uint64, error)

	// Exec exec command
	Exec(cmd interface{}) ([]byte, error)
	// AsyncExec async exec command
	AsyncExec(interface{}, func(interface{}, []byte, error), interface{})
	// ExecWithGroup exec command with group
	ExecWithGroup(interface{}, Group) ([]byte, error)
	// AsyncExecWithGroup async exec command with group
	AsyncExecWithGroup(interface{}, Group, func(interface{}, []byte, error), interface{})
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
		case uint64(AOEGroup):
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
		case uint64(KVGroup):
			return kvDataStorage
		case uint64(AOEGroup):
			return aoeDataStorage
		}
		return nil
	}
	cfg.Storage.ForeachDataStorageFunc = func(cb func(cstorage.DataStorage)) {
		cb(kvDataStorage)
		cb(aoeDataStorage)
	}
	cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard {
		return []bhmetapb.Shard{
			{
				Group: uint64(KVGroup),
			},
			{
				Group: uint64(AOEGroup),
			},
		}
	}
	cfg.ShardGroups = 2

	if adjustFunc != nil {
		adjustFunc(cfg)
	}

	h.store = raftstore.NewStore(cfg)
	scfg.Store = h.store
	scfg.Handler = h
	h.app = server.NewApplication(scfg)
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

func (h *aoeStorage) SetWithGroup(key, value []byte, group Group) error {
	return h.SetWithGroupWithTTL(key, value, group, 0)
}

func (h *aoeStorage) SetWithGroupWithTTL(key, value []byte, group Group, ttl int64) error {
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
	return h.GetWithGroup(key, KVGroup)
}

// GetWithGroup returns the value of key
func (h *aoeStorage) GetWithGroup(key []byte, group Group) ([]byte, error) {
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
	return h.ScanWithGroup(start, end, limit, KVGroup)
}

func (h *aoeStorage) ScanWithGroup(start []byte, end []byte, limit uint64, group Group) ([][]byte, error) {
	req := &Args{
		Op: uint64(Incr),
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
	return h.PrefixScanWithGroup(prefix, limit, KVGroup)
}

func (h *aoeStorage) PrefixScanWithGroup(prefix []byte, limit uint64, group Group) ([][]byte, error) {
	req := &Args{
		Op: uint64(Incr),
		Args: [][]byte{
			prefix,
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

func (h *aoeStorage) AllocID(idkey []byte) (uint64, error) {
	req := &Args{
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

func (h *aoeStorage) AsyncExecWithGroup(cmd interface{}, group Group, cb func(interface{}, []byte, error), arg interface{}) {
	h.app.AsyncExecWithGroupAndTimeout(cmd, uint64(group), cb, defaultRPCTimeout, arg)
}

func (h *aoeStorage) ExecWithGroup(cmd interface{}, group Group) ([]byte, error) {
	return h.app.ExecWithGroup(cmd, uint64(group), defaultRPCTimeout)
}

func (h *aoeStorage) RaftStore() raftstore.Store {
	return h.store
}

func (h *aoeStorage) getStoreByGroup(group uint64, shard uint64) cstorage.DataStorage {
	return h.store.DataStorageByGroup(group, shard)
}
