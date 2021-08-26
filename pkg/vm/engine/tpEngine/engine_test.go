package tpEngine

import (
	"fmt"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/metadata"
	"os"
	"reflect"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test"
)

func recreateTestTempDir() (err error) {
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tmpDir, 0755)
	return err
}

func cleanupTmpDir() error {
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	return nil
}

type testCluster struct {
	t            *testing.T
	applications []dist.CubeDriver
}

func newTestClusterStore(t *testing.T) (*testCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	c := &testCluster{t: t}
	for i := 0; i < 3; i++ {
		metaStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		memDataStorage := mem.NewStorage()
		if err != nil {
			return nil, err
		}
		a, err := dist.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, memDataStorage, func(cfg *config.Config) {
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

		}, server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:908%d", i),
		})
		if err != nil {
			return nil, err
		}
		err = a.Start()
		if err != nil {
			return nil, err
		}
		c.applications = append(c.applications, a)
	}
	return c, nil
}

func (c *testCluster) stop() {
	for _, s := range c.applications {
		s.Close()
	}
}

func getDatabaseListWrap(te *tpEngine, t *testing.T, info string) []string {
	dbs := te.Databases()

	fmt.Printf("---> db list (%s) : %v \n", info, dbs)
	return dbs
}

func TestEngine_1(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()
	c, err := newTestClusterStore(t)
	if err != nil {
		t.Errorf("new cube failed %v", err)
		return
	}

	defer c.stop()

	time.Sleep(2 * time.Second)

	kv := c.applications[0]

	te := NewTpEngine(tpEngineName, kv, nil)

	err = te.Init()
	if err != nil {
		t.Error(err)
		return
	}

	getDatabaseListWrap(te, t, "init")

	cnt := 1

	var dbs []string = nil
	for i := 0; i < cnt; i++ {
		dbs = append(dbs, fmt.Sprintf("db%d", i))
	}

	for i := 0; i < cnt; i++ {
		err = te.Create(dbs[i], engine.RSE)
		if err != nil {
			t.Error(err)
			return
		}
	}

	getDatabaseListWrap(te, t, "after create dbN")
	for i := 0; i < 1+cnt; i++ {
		err = te.getAllKvInTable(uint64(i), 0, 0)
		err = te.getAllKvInTable(uint64(i), 1, 0)
		err = te.getAllKvInTable(uint64(i), 2, 0)
		err = te.getAllKvInTable(uint64(i), 3, 0)
		err = te.getAllKvInTable(uint64(i), 4, 0)
		err = te.getAllKvInTable(uint64(i), 5, 0)
	}

	require.NoError(t, err)
	if err != nil {
		t.Error(err)
		return
	}
	//
	//err = te.Delete("db1")
	//if err != nil {
	//	t.Error(err)
	//	return
	//}

	getDatabaseListWrap(te, t, "last")

	time.Sleep(5 * time.Second)

	db0, err := te.Database(dbs[0])
	if err != nil {
		t.Error(err)
	}

	var attr = []engine.TableDef{
		&engine.AttributeDef{ //id
			Attr: metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "id",
				Type: types.Type{
					Oid:       types.T_uint64,
					Size:      8,
					Width:     8,
					Precision: 0,
				},
			},
		},
		&engine.AttributeDef{ //name 100bytes
			Attr: metadata.Attribute{
				Alg:  compress.Lz4,
				Name: "name",
				Type: types.Type{
					Oid:       types.T_char,
					Size:      24,
					Width:     100,
					Precision: 0,
				},
			},
		},
	}

	relName := "A"
	err = db0.Create(relName, attr, nil, nil, "comment info")
	if err != nil {
		t.Error(err)
	}

	rel, err := db0.Relation(relName)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < len(attr); i++ {
		a := attr[i].(*engine.AttributeDef)
		b := rel.Attribute()[i]
		if !reflect.DeepEqual(a.Attr, b) {
			t.Error(fmt.Errorf("attribute enc/dec not equal %v %v", a.Attr, b))
			return
		}
	}

	//test table engine
	//tab_skey := makeTableKey(tpEngineName,0,1,0,"engine")

}

func Test_tpTableKey_encode(t *testing.T) {
	type args struct {
		data []byte
	}

	t1 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(1), "def", uint64(2)},
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"xxx", "yyy", uint64(3)},
	)
	t2 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(1), "def", uint64(2)},
		nil,
		nil,
	)
	t3 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)
	t4 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abc"},
		NewTpSchema(TP_ENCODE_TYPE_UINT64),
		[]interface{}{uint64(3)},
	)
	tests := []struct {
		name string
		ttk  *tpTableKey
		args args
		want *tpTableKey
	}{
		{"t1", t1, args{nil}, t1},
		{"t2", t2, args{nil}, t2},
		{"t3", t3, args{nil}, t3},
		{"t4", t4, args{nil}, t4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ttk.encode(tt.args.data)
			tbk := NewTpTableKeyWithSchema(tt.ttk.primarySchema, tt.ttk.suffixSchema)
			_, err := tbk.decode(got)
			if err != nil {
				t.Errorf("tpTableKey decode failed. %v", err)
				return
			}
			if !reflect.DeepEqual(tbk, tt.want) {
				t.Errorf("encode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tpTableKey_encodePrefix(t *testing.T) {
	type args struct {
		data []byte
	}

	t1 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)
	t1_want := t1

	t2 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abcdef"},
		nil,
		nil,
	)

	t2_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	tests := []struct {
		name   string
		fields *tpTableKey
		args   args
		want   *tpTableKey
	}{
		{"t1", t1, args{nil}, t1_want},
		{"t2", t2, args{nil}, t2_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.encodePrefix(tt.args.data)
			tbk := NewTpTableKeyWithSchema(nil, nil)
			_, err := tbk.decodePrefix(got)
			if err != nil {
				t.Errorf("decode prefix failed. %v", err)
				return
			}
			if !tbk.isPrefixEqualTo(tt.want) {
				t.Errorf("encodePrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tpTableKey_encodePrimaryKeys(t *testing.T) {
	type args struct {
		data []byte
	}

	t1 := NewTpTableKey("cx", 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(10)},
		nil,
		nil,
	)
	t1_want := t1

	t2 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abcdef"},
		nil,
		nil,
	)

	t2_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abcdef"},
		nil,
		nil,
	)

	t3 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	t3_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	tests := []struct {
		name   string
		fields *tpTableKey
		args   args
		want   *tpTableKey
	}{
		{"t1", t1, args{nil}, t1_want},
		{"t2", t2, args{nil}, t2_want},
		{"t3", t3, args{nil}, t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.encodePrimaryKeys(tt.args.data)
			tbk := NewTpTableKeyWithSchema(tt.fields.primarySchema, nil)
			_, err := tbk.decodePrimaryKeys(got)
			if err != nil {
				t.Errorf("decode primary key failed. %v", err)
			}
			if !reflect.DeepEqual(tbk.primaries, tt.want.primaries) {
				t.Errorf("encodePrimaryKeys() = %v, want %v", tbk, tt.want)
			}
		})
	}
}

func Test_tpTableKey_encodePrefixAndPrimaryKeys(t *testing.T) {
	type args struct {
		data []byte
	}

	t1 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(10)},
		NewTpSchema(TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING),
		[]interface{}{uint64(0), "abc"},
	)

	t1_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(10)},
		nil,
		nil,
	)

	t2 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abc"},
		nil,
		nil,
	)

	t2_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abc"},
		nil,
		nil,
	)

	t3 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	t3_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	tests := []struct {
		name   string
		fields *tpTableKey
		args   args
		want   *tpTableKey
	}{
		{"t1", t1, args{nil}, t1_want},
		{"t2", t2, args{nil}, t2_want},
		{"t3", t3, args{nil}, t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.encodePrefixAndPrimaryKeys(tt.args.data)
			tbk := NewTpTableKeyWithSchema(tt.fields.primarySchema, nil)
			_, err := tbk.decodePrefixAndPrimaryKeys(got)
			if err != nil {
				t.Errorf("table key decode failed. %v", err)
				return
			}
			if !(tbk.isPrefixEqualTo(tt.want) && reflect.DeepEqual(tbk.primaries, tt.want.primaries)) {
				t.Errorf("encodePrefixAndPrimaryKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tpTableKey_encodeSuffix(t *testing.T) {
	type args struct {
		data []byte
	}

	t1 := NewTpTableKey(tpEngineName, 0, 1, 0,
		NewTpSchema(TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64),
		[]interface{}{"abc", uint64(10)},
		NewTpSchema(TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING),
		[]interface{}{uint64(0), "abc"},
	)

	t1_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		NewTpSchema(TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING),
		[]interface{}{uint64(0), "abc"},
	)

	t2 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	t2_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		nil,
		nil,
	)

	t3 := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abc"},
	)

	t3_want := NewTpTableKey(tpEngineName, 0, 1, 0,
		nil,
		nil,
		NewTpSchema(TP_ENCODE_TYPE_STRING),
		[]interface{}{"abc"},
	)

	tests := []struct {
		name   string
		fields *tpTableKey
		args   args
		want   *tpTableKey
	}{
		{"t1", t1, args{nil}, t1_want},
		{"t2", t2, args{nil}, t2_want},
		{"t3", t3, args{nil}, t3_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.encodeSuffix(tt.args.data)
			tbk := NewTpTableKeyWithSchema(tt.fields.primarySchema, tt.fields.suffixSchema)
			_, err := tbk.decodeSuffix(got)
			if err != nil {
				t.Errorf("decodeSuffix failed. %v", err)
				return
			}
			if !reflect.DeepEqual(tbk.suffix, tt.want.suffix) {
				t.Errorf("encodeSuffix() = %v, want %v", tbk, tt.want)
			}
		})
	}
}

func Test_mergeTpSchema(t *testing.T) {
	type args struct {
		schs []*tpSchema
	}
	t1_1 := NewTpSchema(TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING)
	t1_1.UnUsedInEncoding(1)
	t1_2 := NewTpSchema(TP_ENCODE_TYPE_UINT64)
	t1_want := NewTpSchema(TP_ENCODE_TYPE_UINT64, TP_ENCODE_TYPE_STRING, TP_ENCODE_TYPE_UINT64)
	t1_want.UnUsedInEncoding(1)
	tests := []struct {
		name string
		args args
		want *tpSchema
	}{
		{"t1", args{[]*tpSchema{t1_1, t1_2}}, t1_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeTpSchema(tt.args.schs...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeTpSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}
