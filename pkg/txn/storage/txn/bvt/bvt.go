// copyright 2022 matrix origin
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
//
//      http://www.apache.org/licenses/license-2.0
//
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// without warranties or conditions of any kind, either express or implied.
// see the license for the specific language governing permissions and
// limitations under the license.

package main

// This is a standalone program for BVT test
// don't use this program in CI, use mo-service instead

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	txnstorage "github.com/matrixorigin/matrixone/pkg/txn/storage/txn"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
)

func init() {
	go func() {
		http.ListenAndServe(":7890", nil)
	}()
}

func main() {

	f, err := os.Create("cpu-profile")
	check(err)
	defer f.Close()
	check(pprof.StartCPUProfile(f))
	defer pprof.StopCPUProfile()

	defer func() {
		prof := pprof.Lookup("allocs")
		if prof != nil {
			f, err := os.Create("allocs-profile")
			check(err)
			defer f.Close()
			err = prof.WriteTo(f, 0)
			check(err)
		}
	}()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Hour,
	)
	defer cancel()

	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:        "debug",
		Format:       "console",
		DisableStore: true,
	})

	frontendParameters := &config.FrontendParameters{
		MoVersion:    "1",
		RootName:     "root",
		RootPassword: "111",
		DumpUser:     "dump",
		DumpPassword: "111",
		Host:         "127.0.0.1",
		Port:         6001,
	}
	frontendParameters.SetDefaultValues()

	hostMMU := host.New(frontendParameters.HostMmuLimitation)
	guestMMU := guest.New(frontendParameters.GuestMmuLimitation, hostMMU)
	heap := mheap.New(guestMMU)
	memoryPool := mempool.New()

	shard := logservicepb.DNShardInfo{
		ShardID:   2,
		ReplicaID: 2,
	}
	shards := []logservicepb.DNShardInfo{
		shard,
	}
	dnStore := logservicepb.DNStore{
		UUID:           uuid.NewString(),
		ServiceAddress: "1",
		Shards:         shards,
	}
	engine := txnengine.New(
		ctx,
		txnengine.NewDefaultShardPolicy(heap),
		func() (logservicepb.ClusterDetails, error) {
			return logservicepb.ClusterDetails{
				DNStores: []logservicepb.DNStore{
					dnStore,
				},
			}, nil
		},
	)

	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := txnstorage.NewMemoryStorage(
		testutil.NewMheap(),
		txnstorage.SnapshotIsolation,
		clock,
	)
	check(err)
	txnClient := txnstorage.NewStorageTxnClient(
		clock,
		storage,
	)

	fs := testutil.NewFS()
	pu := &config.ParameterUnit{
		SV:            frontendParameters,
		HostMmu:       hostMMU,
		Mempool:       memoryPool,
		StorageEngine: engine,
		TxnClient:     txnClient,
		FileService:   fs,
	}
	ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

	globalVars := new(frontend.GlobalSystemVariables)
	frontend.InitGlobalSystemVariables(globalVars)

	moServer := frontend.NewMOServer(
		ctx,
		net.JoinHostPort(
			frontendParameters.Host,
			fmt.Sprintf("%d", frontendParameters.Port),
		),
		pu,
	)
	err = moServer.Start()
	check(err)

	nodeID := uuid.NewString()
	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(pu)
	}
	err = trace.InitSchema(ctx, ieFactory)
	check(err)
	err = metric.InitSchema(ctx, ieFactory)
	check(err)
	writerFactory := export.GetFSWriterFactory(fs, nodeID, "STANDALONE")
	ctx, err = trace.Init(ctx,
		trace.WithMOVersion(frontendParameters.MoVersion),
		trace.WithNode(nodeID, "STANDALONE"),
		trace.EnableTracer(false),
		trace.WithBatchProcessMode("FileService"),
		trace.WithFSWriterFactory(writerFactory),
		trace.DebugMode(false),
		trace.WithSQLExecutor(nil),
	)
	check(err)
	observabilityParams := new(config.ObservabilityParameters)
	observabilityParams.SetDefaultValues(frontendParameters.MoVersion)
	metric.InitMetric(
		ctx,
		nil,
		observabilityParams,
		nodeID,
		metric.ALL_IN_ONE_MODE,
		metric.WithWriterFactory(writerFactory),
	)

	frontend.InitServerVersion(frontendParameters.MoVersion)

	err = frontend.InitSysTenant(ctx)
	check(err)

	// a test
	session := frontend.NewSession(
		frontend.NewMysqlClientProtocol(
			0,
			nil, // goetty IOSession
			1024,
			frontendParameters,
		),
		guestMMU,
		memoryPool,
		pu,
		globalVars,
	)
	session.SetRequestContext(ctx)
	_, err = session.AuthenticateUser("root")
	check(err)

	responsePool := &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	rpcServer, err := morpc.NewRPCServer(
		"mysql",
		"localhost:12345",
		morpc.NewMessageCodec(
			func() morpc.Message {
				return responsePool.Get().(*pipeline.Message)
			},
			8192,
		),
		morpc.WithServerGoettyOptions(
			goetty.WithSessionRWBUfferSize(8192, 8192),
		),
	)
	check(err)
	rpcServer.RegisterRequestHandler(compile.NewServer().HandleRequest)

	err = rpcServer.Start()
	check(err)

	ctx, cancel = signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	pt("OK\n")
	<-ctx.Done()
	pt(" Exit\n")

}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var pt = fmt.Printf
