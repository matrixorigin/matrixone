package catalogkv

import (
	"encoding/json"
	"fmt"
	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	stdLog "log"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test/"
)

func recreateTestTempDir() (err error) {
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tmpDir, os.ModeDir)
	return err
}

func cleanupTmpDir() error {
	return os.RemoveAll(tmpDir)
}

type testCluster struct {
	t            *testing.T
	applications []*server.Application
}

func newTestClusterStore(t *testing.T, initShardsFunc func() []bhmetapb.Shard) (*testCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	util.SetLogger(&emptyLog{})
	c := &testCluster{t: t}
	for i := 0; i < 3; i++ {
		dataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		cfg := &config.Config{}
		cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

		cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
		cfg.Prophet.StorageNode = true
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
		if i != 0 {
			cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
		cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
		cfg.Prophet.Schedule.EnableJointConsensus = true

		cfg.Storage.MetaStorage, err = pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
			return dataStorage
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
			cb(dataStorage)
		}
		cfg.Customize.CustomInitShardsFactory = initShardsFunc
		s := raftstore.NewStore(cfg)
		h := &testHandler{
			store: s,
		}
		c.applications = append(c.applications, server.NewApplication(server.Cfg{
			Addr:    fmt.Sprintf("127.0.0.1:808%d", i),
			Store:   s,
			Handler: h,
		}))
		s.RegisterWriteFunc(1, h.set)
		s.RegisterReadFunc(2, h.get)
	}
	return c, nil
}

func (c *testCluster) start() error {
	for idx, app := range c.applications {
		stdLog.Printf("start app no.%v\n", idx)
		if idx == 2 {
			time.Sleep(time.Second * 5)
		}

		if err := app.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (c *testCluster) stop() {
	for _, s := range c.applications {
		s.Stop()
	}
}

func TestClusterStartAndStop(t *testing.T) {
	defer cleanupTmpDir()
	c, err := newTestClusterStore(t, nil)
	assert.NoError(t, err)
	defer c.stop()
	assert.NoError(t, c.start())

	stdLog.Printf("app all started.")

	cmdSet := testRequest{
		Op:    "SET",
		Key:   "hello",
		Value: "world",
	}
	resp, err := c.applications[0].Exec(&cmdSet, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	cmdGet := testRequest{
		Op:  "GET",
		Key: "hello",
	}
	value, err := c.applications[0].Exec(&cmdGet, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, value, []byte("world"))
	println("hello: ", string(value))
}

type testRequest struct {
	Op    string `json:"json:op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type testHandler struct {
	store raftstore.Store
}

func (h *testHandler) BuildRequest(req *raftcmdpb.Request, msg interface{}) error {
	cmd := msg.(*testRequest)

	cmdName := strings.ToUpper(cmd.Op)

	if cmdName != "SET" && cmdName != "GET" {
		return fmt.Errorf("%s not support", cmd)
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	req.Key = []byte(cmd.Key)
	switch cmdName {
	case "SET":
		req.CustemType = 1
		req.Type = raftcmdpb.CMDType_Write
	case "GET":
		req.CustemType = 2
		req.Type = raftcmdpb.CMDType_Read

	}
	req.Key = []byte(cmd.Key)
	req.Cmd = data
	return nil
}

func (h *testHandler) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

func (h *testHandler) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *testHandler) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *testHandler) set(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	cmd := testRequest{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	err = ctx.WriteBatch().Set(req.Key, []byte(cmd.Value))
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key) + len(cmd.Value))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *testHandler) get(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()

	cmd := testRequest{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp, 0
	}

	value, err := h.store.DataStorageByGroup(0, 0).Get(req.Key)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp, 0
	}

	resp.Value = value
	return resp, uint64(len(value))
}

type emptyLog struct{}

func (l *emptyLog) Info(v ...interface{}) {

}

func (l *emptyLog) Infof(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}
func (l *emptyLog) Debug(v ...interface{}) {

}

func (l *emptyLog) Debugf(format string, v ...interface{}) {
}

func (l *emptyLog) Warning(v ...interface{}) {
}

func (l *emptyLog) Warningf(format string, v ...interface{}) {
}

func (l *emptyLog) Error(v ...interface{}) {
}

func (l *emptyLog) Errorf(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}

func (l *emptyLog) Fatal(v ...interface{}) {
	stdLog.Panic(v...)
}

func (l *emptyLog) Fatalf(format string, v ...interface{}) {
	stdLog.Panicf(format, v...)
}
