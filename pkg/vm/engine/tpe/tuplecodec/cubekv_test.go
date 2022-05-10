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

package tuplecodec

import (
	"fmt"
	stdLog "log"
	"os"
	"reflect"
	"testing"
	"time"

	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	"github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap/zapcore"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
)

var preAllocShardNum = uint64(1)

var dataDir = "./clusterstore"

func cleanupDir(dir string) error {
	return os.RemoveAll(dir)
}

func NewTestCluster(t *testing.T) *testutil.TestAOECluster {
	cleanupDir(dataDir)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = preAllocShardNum
			c.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = nil
			return c
		},
		/*
			testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
				opts := &aoeStorage.Options{}
				mdCfg := &aoeStorage.MetaCfg{
					SegmentMaxBlocks: blockCntPerSegment,
					BlockMaxRows:     blockRows,
				}
				opts.CacheCfg = &aoeStorage.CacheCfg{
					IndexCapacity:  blockRows * blockCntPerSegment * 80,
					InsertCapacity: blockRows * uint64(colCnt) * 2000,
					DataCapacity:   blockRows * uint64(colCnt) * 2000,
				}
				opts.MetaCleanerCfg = &aoeStorage.MetaCleanerCfg{
					Interval: time.Duration(1) * time.Second,
				}
				opts.Meta.Conf = mdCfg
				return aoe3.NewStorageWithOptions(path, opts)
			}),*/
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
			}),
			raftstore.WithTestClusterLogLevel(zapcore.InfoLevel),
			raftstore.WithTestClusterDataPath(dataDir)))

	c.Start()
	c.RaftCluster.WaitLeadersByCount(int(preAllocShardNum+1), time.Second*60)
	stdLog.Printf("+++++++++++drivers all started.++++++++++++")
	return c
}

func CloseTestCluster(tc *testutil.TestAOECluster) {
	stdLog.Printf("-------call stop-------")
	tc.Stop()
}

func TestCubeKV_NextID(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("next id", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		typ := DATABASE_ID
		for i := 0; i < 100; i++ {
			id, err := kv.NextID(typ)
			convey.So(err, convey.ShouldBeNil)
			convey.So(id, convey.ShouldEqual, kv.dbIDPool.idStart)
		}
	})
}

func TestCubeKV_Set(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("set/get/delete", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		genkv := func(a, b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
			}
		}

		kases := []args{
			genkv("a", "b"),
			genkv("b", "b"),
			genkv("c", "b"),
			genkv("c", "c"),
		}

		//set / get
		for _, kase := range kases {
			err := kv.Set(kase.key, kase.value)
			convey.So(err, convey.ShouldBeNil)

			want, err := kv.Get(kase.key)
			convey.So(err, convey.ShouldBeNil)
			convey.So(want, convey.ShouldResemble, kase.value)
		}

		//delete / get
		for _, kase := range kases {
			err = kv.Delete(kase.key)
			convey.So(err, convey.ShouldBeNil)

			want, err := kv.Get(kase.key)
			convey.So(err, convey.ShouldBeNil)
			convey.So(want, convey.ShouldBeNil)
		}
	})
}

func TestCubeKV_SetBatch(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("setBatch/getBatch", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		genkv := func(a, b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
			}
		}

		kases := []args{
			genkv("a", "b"),
			genkv("b", "b"),
			genkv("c", "b"),
			genkv("c", "c"),
		}

		want := []TupleValue{
			genkv("a", "b").value,
			genkv("b", "b").value,
			genkv("c", "c").value,
			genkv("c", "c").value,
		}

		var keys []TupleKey
		var values []TupleValue
		for _, kase := range kases {
			keys = append(keys, kase.key)
			values = append(values, kase.value)
		}

		err = kv.SetBatch(keys, values)
		convey.So(err, convey.ShouldBeNil)

		gets, err2 := kv.GetBatch(keys)
		convey.So(err2, convey.ShouldBeNil)
		convey.So(len(gets), convey.ShouldEqual, len(want))

		for i, get := range gets {
			convey.So(get, convey.ShouldResemble, want[i])
		}
	})
}

func TestCubeKV_DedupSet(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("dedup set", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
			want  bool
		}

		genkv := func(a, b string, c bool) args {
			return args{
				TupleKey(a),
				TupleValue(b),
				c,
			}
		}

		kases := []args{
			genkv("a", "b", true),
			genkv("b", "b", true),
			genkv("c", "b", true),
			genkv("c", "c", false),
		}

		for _, kase := range kases {
			err := kv.DedupSet(kase.key, kase.value)
			if kase.want {
				convey.So(err, convey.ShouldBeNil)

				want, err := kv.Get(kase.key)
				convey.So(err, convey.ShouldBeNil)
				convey.So(want, convey.ShouldResemble, kase.value)
			} else {
				convey.So(err, convey.ShouldBeError)
			}
		}
	})
}

func TestCubeKV_DedupSetBatch(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("dedup set batch", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
			want  bool
		}

		genkv := func(a, b string, w bool) args {
			return args{
				TupleKey(a),
				TupleValue(b),
				w,
			}
		}

		kases := []args{
			genkv("a", "b", true),
			genkv("b", "b", true),
			genkv("c", "b", true),
			genkv("c", "c", false),
		}

		want := []TupleValue{
			genkv("a", "b", true).value,
			genkv("b", "b", true).value,
			genkv("c", "b", true).value,
			genkv("c", "b", true).value,
		}

		setFunc := func(kases []args, want []TupleValue, exist bool, hasKeys bool) {
			var keys []TupleKey
			var values []TupleValue
			for _, kase := range kases {
				keys = append(keys, kase.key)
				values = append(values, kase.value)
			}

			err = kv.DedupSetBatch(keys, values)
			if exist {
				convey.So(err, convey.ShouldBeError)
			} else {
				convey.So(err, convey.ShouldBeNil)
			}

			gets, err2 := kv.GetBatch(keys)
			convey.So(err2, convey.ShouldBeNil)
			convey.So(len(gets), convey.ShouldEqual, len(want))

			for i, get := range gets {
				if hasKeys {
					convey.So(get, convey.ShouldResemble, want[i])
				} else {
					convey.So(get, convey.ShouldBeNil)
				}
			}
		}

		setFunc(kases, want, true, false)

		//
		kases2 := []args{
			genkv("a", "b", true),
			genkv("b", "b", true),
			genkv("c", "b", true),
		}

		want2 := []TupleValue{
			genkv("a", "b", true).value,
			genkv("b", "b", true).value,
			genkv("c", "b", true).value,
		}

		setFunc(kases2, want2, false, true)

		setFunc(kases2, want2, true, true)
	})
}

func TestCubeKV_GetRange(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get range", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		genkv := func(a, b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
			}
		}

		kases := []args{
			genkv("a", "a"),
			genkv("b", "b"),
			genkv("c", "c"),
		}

		var keys []TupleKey
		var values []TupleValue
		for _, kase := range kases {
			keys = append(keys, kase.key)
			values = append(values, kase.value)
		}

		err = kv.SetBatch(keys, values)
		convey.So(err, convey.ShouldBeNil)

		type args2 struct {
			start TupleKey
			end   TupleKey
			want  []TupleValue
		}

		gen := func(a, b string, w ...TupleValue) args2 {
			return args2{
				start: TupleKey(a),
				end:   TupleKey(b),
				want:  w,
			}
		}

		kases2 := []args2{
			gen("a", "c",
				TupleValue("a"),
				TupleValue("b"),
				TupleValue("c")),
			gen("a", "b",
				TupleValue("a"),
				TupleValue("b")),
			gen("a", "a"),
			gen("b", "c",
				TupleValue("b"),
				TupleValue("c")),
			gen("b", "d",
				TupleValue("b"),
				TupleValue("c")), //skip "database_id"
		}

		for _, k2 := range kases2 {
			values, err := kv.GetRange(k2.start, k2.end)
			convey.So(err, convey.ShouldBeNil)
			if len(k2.want) == 0 {
				convey.So(values, convey.ShouldBeEmpty)
			} else {
				for i := 0; i < len(values); i++ {
					convey.So(reflect.DeepEqual(values[i], k2.want[i]),
						convey.ShouldBeTrue)
				}
			}
		}
	})
}

func TestCubeKV_GetRangeWithLimit(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get range with limit", t, func() {
		prefix := "xyz"
		cnt := 10

		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		var kases []args
		for i := 0; i < cnt; i++ {
			key := TupleKey(prefix + fmt.Sprintf("%d", i))
			value := TupleValue(fmt.Sprintf("v%d", i))

			kases = append(kases, args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err, convey.ShouldBeNil)
		}

		endKey := SuccessorOfPrefix(TupleKey(prefix))

		_, values1, _, _, err := kv.GetRangeWithLimit(TupleKey(prefix), endKey, uint64(cnt))
		convey.So(err, convey.ShouldBeNil)

		//for i, key := range keys1 {
		//	fmt.Printf("++++> %s \n[%v] \n%s \n",string(key),key,string(values1[i]))
		//}

		for i, kase := range kases {
			convey.So(values1[i], convey.ShouldResemble, kase.value)
		}

		step := 2
		last := TupleKey(prefix)
		readCnt := 0
		for i := 0; i < cnt; i += step {
			_, values, complete, nextScanKey, err := kv.GetRangeWithLimit(last, endKey, uint64(step))
			convey.So(err, convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j-i], convey.ShouldResemble, kases[j].value)
			}

			readCnt += len(values)
			last = nextScanKey
			if complete {
				break
			}
		}
		convey.So(readCnt, convey.ShouldEqual, cnt)
	})
}

func TestCubeKV_GetWithPrefix(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get with prefix", t, func() {
		prefix := "xyz"
		cnt := 10

		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		var kases []args
		for i := 0; i < cnt; i++ {
			key := TupleKey(prefix + fmt.Sprintf("%d", i))
			value := TupleValue(fmt.Sprintf("v%d", i))

			kases = append(kases, args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err, convey.ShouldBeNil)
		}

		prefixEnd := SuccessorOfPrefix([]byte(prefix))

		_, values, _, _, err := kv.GetWithPrefix(TupleKey(prefix), len(prefix), prefixEnd, false, uint64(cnt))
		convey.So(err, convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i], convey.ShouldResemble, kase.value)
		}

		step := 2
		last := TupleKey(prefix)
		prefixLen := len(prefix)
		readCnt := 0
		for i := 0; i < cnt; i += step {
			_, values, complete, nextScanKey, err := kv.GetWithPrefix(last, prefixLen, prefixEnd, false, uint64(step))
			convey.So(err, convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j-i], convey.ShouldResemble, kases[j].value)
			}

			readCnt += len(values)
			last = nextScanKey
			if complete {
				break
			}
		}
		convey.So(readCnt, convey.ShouldEqual, cnt)
	})
}

func TestCubeKV_GetShardsWithRange(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get shards with range", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)
		cnt := 10
		for i := 0; i < cnt; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			err = kv.Set(TupleKey(key), TupleValue(value))
			convey.So(err, convey.ShouldBeNil)
		}

		x, err := kv.GetShardsWithRange(nil, nil)
		convey.So(err, convey.ShouldBeNil)

		shards, ok := x.(*Shards)
		convey.So(ok, convey.ShouldBeTrue)

		for _, node := range shards.nodes {
			fmt.Printf("%d %v | %s\n",
				node.StoreID,
				node.StoreIDbytes, node.Addr)
		}

		for _, info := range shards.shardInfos {
			fmt.Printf("%v %v %d %v | %s \n",
				info.startKey, info.endKey,
				info.node.StoreID,
				info.node.StoreIDbytes, info.node.Addr)
		}
	})
}

func TestCubeKV_GetShardsWithPrefix(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get shards with prefix", t, func() {
		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		cnt := 10
		for i := 0; i < cnt; i++ {
			key := fmt.Sprintf("k%d", i)
			value := fmt.Sprintf("v%d", i)
			err = kv.Set(TupleKey(key), TupleValue(value))
			convey.So(err, convey.ShouldBeNil)
		}

		x, err := kv.GetShardsWithPrefix(TupleKey("k"))
		convey.So(err, convey.ShouldBeNil)

		shards, ok := x.(*Shards)
		convey.So(ok, convey.ShouldBeTrue)

		for _, node := range shards.nodes {
			fmt.Printf("%d %v | %s\n",
				node.StoreID,
				node.StoreIDbytes, node.Addr)
		}

		for _, info := range shards.shardInfos {
			fmt.Printf("%v %v %d %v | %s \n",
				info.startKey, info.endKey,
				info.node.StoreID,
				info.node.StoreIDbytes, info.node.Addr)
		}
	})
}

func TestCubeKV_DeleteWithPrefix(t *testing.T) {
	tc := NewTestCluster(t)
	defer CloseTestCluster(tc)

	convey.Convey("get with prefix", t, func() {
		prefix := "xyz"
		cnt := 10

		kv, err := NewCubeKV(tc.CubeDrivers[0], 10000, 30, 3, 30, 3)
		convey.So(err, convey.ShouldBeNil)

		type args struct {
			key   TupleKey
			value TupleValue
		}

		var kases []args
		for i := 0; i < cnt; i++ {
			key := TupleKey(prefix + fmt.Sprintf("%d", i))
			value := TupleValue(fmt.Sprintf("v%d", i))

			kases = append(kases, args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err, convey.ShouldBeNil)
		}

		prefixEnd := SuccessorOfPrefix([]byte(prefix))

		_, values, _, _, err := kv.GetWithPrefix(TupleKey(prefix), len(prefix), prefixEnd, false, uint64(cnt))
		convey.So(err, convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i], convey.ShouldResemble, kase.value)
		}

		err = kv.DeleteWithPrefix([]byte(prefix))
		convey.So(err, convey.ShouldBeNil)

		_, values, _, _, err = kv.GetWithPrefix(TupleKey(prefix), len(prefix), prefixEnd, false, uint64(cnt))
		convey.So(err, convey.ShouldBeNil)

		convey.So(len(values), convey.ShouldEqual, 0)
	})
}
