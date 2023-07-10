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

package cnservice

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/ctlservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

var (
	defaultListenAddress    = "127.0.0.1:6002"
	defaultCtlListenAddress = "127.0.0.1:19958"
	// defaultTxnIsolation     = txn.TxnIsolation_SI
	defaultTxnMode             = txn.TxnMode_Optimistic
	maxForMaxPreparedStmtCount = 1000000
)

type Service interface {
	Start() error
	Close() error

	GetTaskRunner() taskservice.TaskRunner
	GetTaskService() (taskservice.TaskService, bool)
	WaitSystemInitCompleted(ctx context.Context) error
}

type EngineType string

const (
	EngineDistributedTAE       EngineType = "distributed-tae"
	EngineMemory               EngineType = "memory"
	EngineNonDistributedMemory EngineType = "non-distributed-memory"
	// ReservedTasks equals how many task must run background.
	// 1 for metric StorageUsage
	// 1 for trace ETLMerge
	ReservedTasks = 2
)

// Config cn service
type Config struct {
	// UUID cn store uuid
	UUID string `toml:"uuid"`
	// Role cn node role, [AP|TP]
	Role string `toml:"role"`

	// ListenAddress listening address for receiving external requests
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`
	// SQLAddress service address for receiving external sql client
	SQLAddress string `toml:"sql-address"`

	// FileService file service configuration

	Engine struct {
		Type                EngineType           `toml:"type"`
		Logstore            options.LogstoreType `toml:"logstore"`
		FlushInterval       toml.Duration        `toml:"flush-interval"`
		MinCount            int64                `toml:"min-count"`
		ScanInterval        toml.Duration        `toml:"scan-interval"`
		IncrementalInterval toml.Duration        `toml:"incremental-interval"`
		GlobalMinCount      int64                `toml:"global-min-count"`
	}

	// parameters for cn-server related buffer.
	ReadBufferSize  int
	WriteBufferSize int

	// Pipeline configuration
	Pipeline struct {
		// HostSize is the memory limit
		HostSize int64 `toml:"host-size"`
		// GuestSize is the memory limit for one query
		GuestSize int64 `toml:"guest-size"`
		// OperatorSize is the memory limit for one operator
		OperatorSize int64 `toml:"operator-size"`
		// BatchRows is the batch rows limit for one batch
		BatchRows int64 `toml:"batch-rows"`
		// BatchSize is the memory limit for one batch
		BatchSize int64 `toml:"batch-size"`
	}

	// Frontend parameters for the frontend
	Frontend config.FrontendParameters `toml:"frontend"`

	// HAKeeper configuration
	HAKeeper struct {
		// HeatbeatInterval heartbeat interval to send message to hakeeper. Default is 1s
		HeatbeatInterval toml.Duration `toml:"hakeeper-heartbeat-interval"`
		// HeatbeatTimeout heartbeat request timeout. Default is 500ms
		HeatbeatTimeout toml.Duration `toml:"hakeeper-heartbeat-timeout"`
		// DiscoveryTimeout discovery HAKeeper service timeout. Default is 30s
		DiscoveryTimeout toml.Duration `toml:"hakeeper-discovery-timeout"`
		// ClientConfig hakeeper client configuration
		ClientConfig logservice.HAKeeperClientConfig
	}

	// TaskRunner configuration
	TaskRunner struct {
		QueryLimit        int           `toml:"task-query-limit"`
		Parallelism       int           `toml:"task-parallelism"`
		MaxWaitTasks      int           `toml:"task-max-wait-tasks"`
		FetchInterval     toml.Duration `toml:"task-fetch-interval"`
		FetchTimeout      toml.Duration `toml:"task-fetch-timeout"`
		RetryInterval     toml.Duration `toml:"task-retry-interval"`
		HeartbeatInterval toml.Duration `toml:"task-heartbeat-interval"`
	}

	// RPC rpc config used to build txn sender
	RPC rpc.Config `toml:"rpc"`

	// Cluster configuration
	Cluster struct {
		// RefreshInterval refresh cluster info from hakeeper interval
		RefreshInterval toml.Duration `toml:"refresh-interval"`
	}

	// LockService lockservice
	LockService lockservice.Config `toml:"lockservice"`

	// Txn txn config
	Txn struct {
		// Isolation txn isolation. SI or RC
		// when Isolation is not set. we will set SI when Mode is optimistic, RC when Mode is pessimistic
		Isolation string `toml:"isolation"`
		// Mode txn mode. optimistic or pessimistic, default is optimistic
		Mode string `toml:"mode"`
		// EnableSacrificingFreshness In Push Mode, the transaction is not guaranteed
		// to see the latest commit data, and the latest Logtail commit timestamp received
		// by the current CN + 1 is used as the start time of the transaction. But it will
		// ensure that the transactions of the same database connection can see the writes
		// of the previous committed transactions.
		EnableSacrificingFreshness bool `toml:"enable-sacrificing-freshness"`
		// EnableCNBasedConsistency ensure that all the transactions on a CN can read
		// the writes of the previous committed transaction
		EnableCNBasedConsistency bool `toml:"enable-cn-based-consistency"`
		// EnableRefreshExpressionIn RC mode, in the event of a conflict, the later transaction
		// needs to see the latest data after the previous transaction commits. At this time we
		// need to re-read the data, re-read the latest data, and re-compute the expression. This
		// feature was turned off in 0.8 and is not supported for now. The replacement solution is
		// to return a retry error and let the whole computation re-execute.
		EnableRefreshExpression bool `toml:"enable-refresh-expression"`
		// EnableLeakCheck enable txn leak check
		EnableLeakCheck bool `toml:"enable-leak-check"`
		// MaxActiveAges a txn max active duration
		MaxActiveAges toml.Duration `toml:"max-active-ages"`
	} `toml:"txn"`

	// Ctl ctl service config. CtlService is used to handle ctl request. See mo_ctl for detail.
	Ctl ctlservice.Config `toml:"ctl"`

	// AutoIncrement auto increment config
	AutoIncrement incrservice.Config `toml:"auto-increment"`

	// PrimaryKeyCheck
	PrimaryKeyCheck bool `toml:"primary-key-check"`

	// MaxPreparedStmtCount
	MaxPreparedStmtCount int `toml:"max_prepared_stmt_count"`
}

func (c *Config) Validate() error {
	foundMachineHost := ""
	if c.UUID == "" {
		panic("missing cn store UUID")
	}
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	} else {
		foundMachineHost = strings.Split(c.ServiceAddress, ":")[0]
	}
	if c.Role == "" {
		c.Role = metadata.CNRole_TP.String()
	}
	if c.HAKeeper.DiscoveryTimeout.Duration == 0 {
		c.HAKeeper.DiscoveryTimeout.Duration = time.Second * 30
	}
	if c.HAKeeper.HeatbeatInterval.Duration == 0 {
		c.HAKeeper.HeatbeatInterval.Duration = time.Second
	}
	if c.HAKeeper.HeatbeatTimeout.Duration == 0 {
		c.HAKeeper.HeatbeatTimeout.Duration = time.Second * 3
	}
	if c.TaskRunner.Parallelism == 0 {
		c.TaskRunner.Parallelism = runtime.NumCPU() / 16
		if c.TaskRunner.Parallelism <= ReservedTasks {
			c.TaskRunner.Parallelism = 1 + ReservedTasks
		}
	}
	if c.TaskRunner.FetchInterval.Duration == 0 {
		c.TaskRunner.FetchInterval.Duration = time.Second * 10
	}
	if c.TaskRunner.FetchTimeout.Duration == 0 {
		c.TaskRunner.FetchTimeout.Duration = time.Second * 10
	}
	if c.TaskRunner.HeartbeatInterval.Duration == 0 {
		c.TaskRunner.HeartbeatInterval.Duration = time.Second * 5
	}
	if c.TaskRunner.MaxWaitTasks == 0 {
		c.TaskRunner.MaxWaitTasks = 256
	}
	if c.TaskRunner.QueryLimit == 0 {
		c.TaskRunner.QueryLimit = c.TaskRunner.Parallelism
	}
	if c.TaskRunner.RetryInterval.Duration == 0 {
		c.TaskRunner.RetryInterval.Duration = time.Second
	}
	if c.Engine.Type == "" {
		c.Engine.Type = EngineDistributedTAE
	}
	if c.Engine.Logstore == "" {
		c.Engine.Logstore = options.LogstoreLogservice
	}
	if c.Cluster.RefreshInterval.Duration == 0 {
		c.Cluster.RefreshInterval.Duration = time.Second * 10
	}

	if c.Txn.Mode == "" {
		c.Txn.Mode = defaultTxnMode.String()
	}
	if !txn.ValidTxnMode(c.Txn.Mode) {
		return moerr.NewBadDBNoCtx("not support txn mode: " + c.Txn.Mode)
	}

	if c.Txn.Isolation == "" {
		if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic {
			c.Txn.Isolation = txn.TxnIsolation_RC.String()
		} else {
			c.Txn.Isolation = txn.TxnIsolation_SI.String()
		}
	}
	if !txn.ValidTxnIsolation(c.Txn.Isolation) {
		return moerr.NewBadDBNoCtx("not support txn isolation: " + c.Txn.Isolation)
	}

	if c.Txn.MaxActiveAges.Duration == 0 {
		c.Txn.MaxActiveAges.Duration = time.Minute * 2
	}
	c.Ctl.Adjust(foundMachineHost, defaultCtlListenAddress)
	c.LockService.ServiceID = c.UUID
	c.LockService.Validate()

	// pessimistic mode implies primary key check
	if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic || c.PrimaryKeyCheck {
		plan.CNPrimaryCheck = true
	} else {
		plan.CNPrimaryCheck = false
	}

	if c.MaxPreparedStmtCount > 0 {
		if c.MaxPreparedStmtCount > maxForMaxPreparedStmtCount {
			frontend.MaxPrepareNumberInOneSession = maxForMaxPreparedStmtCount
		} else {
			frontend.MaxPrepareNumberInOneSession = c.MaxPreparedStmtCount
		}
	} else {
		frontend.MaxPrepareNumberInOneSession = 1024
	}
	return nil
}

func (c *Config) getLockServiceConfig() lockservice.Config {
	c.LockService.ServiceID = c.UUID
	c.LockService.RPC = c.RPC
	return c.LockService
}

type service struct {
	metadata       metadata.CNStore
	cfg            *Config
	responsePool   *sync.Pool
	logger         *zap.Logger
	server         morpc.RPCServer
	requestHandler func(ctx context.Context,
		cnAddr string,
		message morpc.Message,
		cs morpc.ClientSession,
		engine engine.Engine,
		fService fileservice.FileService,
		lockService lockservice.LockService,
		cli client.TxnClient,
		aicm *defines.AutoIncrCacheManager,
		messageAcquirer func() morpc.Message) error
	cancelMoServerFunc     context.CancelFunc
	mo                     *frontend.MOServer
	initHakeeperClientOnce sync.Once
	_hakeeperClient        logservice.CNHAKeeperClient
	initTxnSenderOnce      sync.Once
	_txnSender             rpc.TxnSender
	initTxnClientOnce      sync.Once
	_txnClient             client.TxnClient
	timestampWaiter        client.TimestampWaiter
	storeEngine            engine.Engine
	metadataFS             fileservice.ReplaceableFileService
	etlFS                  fileservice.FileService
	fileService            fileservice.FileService
	pu                     *config.ParameterUnit
	moCluster              clusterservice.MOCluster
	lockService            lockservice.LockService
	ctlservice             ctlservice.CtlService

	stopper *stopper.Stopper
	aicm    *defines.AutoIncrCacheManager

	task struct {
		sync.RWMutex
		holder         taskservice.TaskServiceHolder
		runner         taskservice.TaskRunner
		storageFactory taskservice.TaskStorageFactory
	}
}
