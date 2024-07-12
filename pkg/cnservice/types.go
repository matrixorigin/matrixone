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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

var (
	defaultListenAddress             = "127.0.0.1:6002"
	defaultQueryServiceListenAddress = "0.0.0.0:19998"
	// defaultTxnIsolation     = txn.TxnIsolation_SI
	defaultTxnMode             = txn.TxnMode_Pessimistic
	maxForMaxPreparedStmtCount = 1000000

	// Service ports related.
	defaultServiceHost = "127.0.0.1"
)

type Service interface {
	Start() error
	Close() error
	// ID returns UUID of the service.
	ID() string
	GetTaskRunner() taskservice.TaskRunner
	GetTaskService() (taskservice.TaskService, bool)
	GetSQLExecutor() executor.SQLExecutor
	GetBootstrapService() bootstrap.Service
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

	// PortBase is the base port for the service. We reserve reservedPorts for
	// the service to start internal server inside it.
	//
	// TODO(volgariver6): The value of this field is also used to determine the version
	// of MO. If it is not set, we use the old listen-address/service-address fields, and
	// if it is set, we use the new policy to distribute the ports to all services.
	PortBase int `toml:"port-base" user_setting:"basic"`
	// ServiceHost is the host name/IP for the service address of RPC request. There is
	// no port value in it.
	ServiceHost string `toml:"service-host" user_setting:"basic"`

	// FileService file service configuration

	Engine struct {
		Type     EngineType           `toml:"type"`
		Logstore options.LogstoreType `toml:"logstore"`
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
		HeartbeatTimeout  toml.Duration `toml:"task-heartbeat-timeout"`
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

	// ShardService shard service config
	ShardService shardservice.Config `toml:"shardservice"`

	// Txn txn config
	Txn struct {
		// Isolation txn isolation. SI or RC
		// when Isolation is not set. we will set SI when Mode is optimistic, RC when Mode is pessimistic
		Isolation string `toml:"isolation" user_setting:"advanced"`
		// Mode txn mode. optimistic or pessimistic, default is pessimistic
		Mode string `toml:"mode" user_setting:"advanced"`
		// EnableSacrificingFreshness In Push Mode, the transaction is not guaranteed
		// to see the latest commit data, and the latest Logtail commit timestamp received
		// by the current CN + 1 is used as the start time of the transaction. But it will
		// ensure that the transactions of the same database connection can see the writes
		// of the previous committed transactions.
		// -1: disable
		//	0: auto config based on txn mode
		//  1: enable
		EnableSacrificingFreshness int `toml:"enable-sacrificing-freshness"`
		// EnableCNBasedConsistency ensure that all the transactions on a CN can read
		// the writes of the previous committed transaction
		// -1: disable
		//	0: auto config based on txn mode
		//  1: enable
		EnableCNBasedConsistency int `toml:"enable-cn-based-consistency"`
		// EnableRefreshExpressionIn RC mode, in the event of a conflict, the later transaction
		// needs to see the latest data after the previous transaction commits. At this time we
		// need to re-read the data, re-read the latest data, and re-compute the expression. This
		// feature was turned off in 0.8 and is not supported for now. The replacement solution is
		// to return a retry error and let the whole computation re-execute.
		// -1: disable
		//	0: auto config based on txn mode
		//  1: enable
		EnableRefreshExpression int `toml:"enable-refresh-expression"`
		// EnableLeakCheck enable txn leak check
		// -1: disable
		//	0: auto config based on txn mode
		//  1: enable
		EnableLeakCheck int `toml:"enable-leak-check"`
		// MaxActiveAges a txn max active duration
		MaxActiveAges toml.Duration `toml:"max-active-ages"`
		// EnableCheckRCInvalidError this config is used to check and find RC bugs in pessimistic mode.
		// Will remove it later version.
		EnableCheckRCInvalidError bool `toml:"enable-check-rc-invalid-error"`
		// Limit flow control of transaction creation, maximum number of transactions per second. Default
		// is unlimited.
		Limit int `toml:"limit-per-second"`
		// MaxActive is the count of max active txn in current cn.  If reached max value, the txn
		// is added to a FIFO queue. Default is unlimited.
		MaxActive int `toml:"max-active"`
		// NormalStateNoWait is the config to control if it waits for the transaction client
		// to be normal state. If the value is false, it waits until the transaction client to be
		// normal state; if the value is true, it does not wait and just return an error to the
		// client. Default value is false.
		NormalStateNoWait bool `toml:"normal-state-no-wait"`
		//PKDedupCount check whether primary key in transaction's workspace is duplicated if the count of pk
		// is less than PKDedupCount when txn commits. Default value is 0 , which means don't do deduplication.
		PkDedupCount int `toml:"pk-dedup-count"`

		// Trace trace
		Trace struct {
			BufferSize    int           `toml:"buffer-size"`
			FlushBytes    toml.ByteSize `toml:"flush-bytes"`
			FlushDuration toml.Duration `toml:"force-flush-duration"`
			Dir           string        `toml:"dir"`
			Enable        bool          `toml:"enable"`
			Tables        []uint64      `toml:"tables"`
			LoadToMO      bool          `toml:"load-to-mo"`
		} `toml:"trace"`
	} `toml:"txn"`

	// AutoIncrement auto increment config
	AutoIncrement incrservice.Config `toml:"auto-increment"`

	// QueryServiceConfig is the config for query service.
	QueryServiceConfig queryservice.Config `toml:"query-service"`

	// PrimaryKeyCheck
	PrimaryKeyCheck bool `toml:"primary-key-check"`

	// LargestEntryLimit is the max size for reading file to buf
	LargestEntryLimit int `toml:"largest-entry-limit"`

	// MaxPreparedStmtCount
	MaxPreparedStmtCount int `toml:"max_prepared_stmt_count"`

	// InitWorkState is the initial work state for CN. Valid values are:
	// "working", "draining" and "drained".
	InitWorkState string `toml:"init-work-state"`

	PythonUdfClient pythonservice.ClientConfig `toml:"python-udf-client"`

	// LogtailUpdateWorkerFactor is the times of CPU number of this node
	// to start update workers.
	LogtailUpdateWorkerFactor int `toml:"logtail-update-worker-factor"`

	// Whether to automatically upgrade when system startup
	AutomaticUpgrade       bool `toml:"auto-upgrade"`
	UpgradeTenantBatchSize int  `toml:"upgrade-tenant-batch"`
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

	// Fix txn mode various config, simply override
	if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic {
		if c.Txn.EnableSacrificingFreshness == 0 {
			c.Txn.EnableSacrificingFreshness = 1
		}
		if c.Txn.EnableCNBasedConsistency == 0 {
			c.Txn.EnableCNBasedConsistency = -1
		}
		// We don't support the following now, so always disable
		c.Txn.EnableRefreshExpression = -1
		if c.Txn.EnableLeakCheck == 0 {
			c.Txn.EnableLeakCheck = -1
		}
	} else {
		if c.Txn.EnableSacrificingFreshness == 0 {
			c.Txn.EnableSacrificingFreshness = 1
		}
		if c.Txn.EnableCNBasedConsistency == 0 {
			c.Txn.EnableCNBasedConsistency = 1
		}
		// We don't support the following now, so always disable
		c.Txn.EnableRefreshExpression = -1
		if c.Txn.EnableLeakCheck == 0 {
			c.Txn.EnableLeakCheck = -1
		}
	}

	if c.Txn.MaxActiveAges.Duration == 0 {
		c.Txn.MaxActiveAges.Duration = time.Minute * 2
	}
	if c.Txn.MaxActive == 0 {
		c.Txn.MaxActive = runtime.NumCPU() * 4
	}
	c.LockService.ServiceID = c.UUID
	c.LockService.Validate()

	// pessimistic mode implies primary key check
	if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic || c.PrimaryKeyCheck {
		config.CNPrimaryCheck = true
	} else {
		config.CNPrimaryCheck = false
	}

	if c.LargestEntryLimit > 0 {
		config.LargestEntryLimit = c.LargestEntryLimit
	}

	if c.MaxPreparedStmtCount > 0 {
		if c.MaxPreparedStmtCount > maxForMaxPreparedStmtCount {
			frontend.MaxPrepareNumberInOneSession = maxForMaxPreparedStmtCount
		} else {
			frontend.MaxPrepareNumberInOneSession = c.MaxPreparedStmtCount
		}
	} else {
		frontend.MaxPrepareNumberInOneSession = 100000
	}
	c.QueryServiceConfig.Adjust(foundMachineHost, defaultQueryServiceListenAddress)

	if c.PortBase != 0 {
		if c.ServiceHost == "" {
			c.ServiceHost = defaultServiceHost
		}
	}

	if c.LogtailUpdateWorkerFactor == 0 {
		c.LogtailUpdateWorkerFactor = 4
	}

	if !metadata.ValidStateString(c.InitWorkState) {
		c.InitWorkState = metadata.WorkState_Working.String()
	}

	// TODO: remove this if rc is stable
	moruntime.ProcessLevelRuntime().SetGlobalVariables(moruntime.EnableCheckInvalidRCErrors,
		c.Txn.EnableCheckRCInvalidError)
	return nil
}

// SetDefaultValue setups the default of the config.
// most of the code are copied from the Validate.
// But, the Validate may change some global variables that the SetDefaultValue does not need.
// So, need a different function.
func (c *Config) SetDefaultValue() {
	foundMachineHost := ""
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

	if c.Txn.Isolation == "" {
		if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic {
			c.Txn.Isolation = txn.TxnIsolation_RC.String()
		} else {
			c.Txn.Isolation = txn.TxnIsolation_SI.String()
		}
	}
	// Fix txn mode various config, simply override
	if txn.GetTxnMode(c.Txn.Mode) == txn.TxnMode_Pessimistic {
		if c.Txn.EnableSacrificingFreshness == 0 {
			c.Txn.EnableSacrificingFreshness = 1
		}
		if c.Txn.EnableCNBasedConsistency == 0 {
			c.Txn.EnableCNBasedConsistency = -1
		}
		// We don't support the following now, so always disable
		c.Txn.EnableRefreshExpression = -1
		if c.Txn.EnableLeakCheck == 0 {
			c.Txn.EnableLeakCheck = -1
		}
	} else {
		if c.Txn.EnableSacrificingFreshness == 0 {
			c.Txn.EnableSacrificingFreshness = 1
		}
		if c.Txn.EnableCNBasedConsistency == 0 {
			c.Txn.EnableCNBasedConsistency = 1
		}
		// We don't support the following now, so always disable
		c.Txn.EnableRefreshExpression = -1
		if c.Txn.EnableLeakCheck == 0 {
			c.Txn.EnableLeakCheck = -1
		}
	}

	if c.Txn.MaxActiveAges.Duration == 0 {
		c.Txn.MaxActiveAges.Duration = time.Minute * 2
	}
	if c.Txn.MaxActive == 0 {
		c.Txn.MaxActive = runtime.NumCPU() * 4
	}
	c.Txn.NormalStateNoWait = false
	c.LockService.ServiceID = c.UUID

	c.ShardService.ServiceID = c.UUID

	c.QueryServiceConfig.Adjust(foundMachineHost, defaultQueryServiceListenAddress)

	if c.PortBase != 0 {
		if c.ServiceHost == "" {
			c.ServiceHost = defaultServiceHost
		}
	}

	if !metadata.ValidStateString(c.InitWorkState) {
		c.InitWorkState = metadata.WorkState_Working.String()
	}

	if c.UpgradeTenantBatchSize <= 0 {
		c.UpgradeTenantBatchSize = 16
	} else if c.UpgradeTenantBatchSize >= 32 {
		c.UpgradeTenantBatchSize = 32
	}

	c.Frontend.SetDefaultValues()
}

func (s *service) getLockServiceConfig() lockservice.Config {
	s.cfg.LockService.ServiceID = s.cfg.UUID
	s.cfg.LockService.RPC = s.cfg.RPC
	s.cfg.LockService.ListenAddress = s.lockServiceListenAddr()
	s.cfg.LockService.TxnIterFunc = func(f func([]byte) bool) {
		tc := s._txnClient
		if tc == nil {
			return
		}

		tc.IterTxns(func(to client.TxnOverview) bool {
			return f(to.Meta.ID)
		})
	}
	return s.cfg.LockService
}

func (s *service) getShardServiceConfig() shardservice.Config {
	s.cfg.ShardService.ServiceID = s.cfg.UUID
	s.cfg.ShardService.RPC = s.cfg.RPC
	s.cfg.ShardService.ListenAddress = s.shardServiceListenAddr()
	return s.cfg.ShardService
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
		queryClient qclient.QueryClient,
		hakeeper logservice.CNHAKeeperClient,
		udfService udf.Service,
		cli client.TxnClient,
		aicm *defines.AutoIncrCacheManager,
		messageAcquirer func() morpc.Message) error
	cancelMoServerFunc     context.CancelFunc
	mo                     *frontend.MOServer
	initHakeeperClientOnce sync.Once
	_hakeeperClient        logservice.CNHAKeeperClient
	hakeeperConnected      chan struct{}
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
	shardService           shardservice.ShardService
	sqlExecutor            executor.SQLExecutor
	sessionMgr             *queryservice.SessionManager
	// queryService is used to handle query request from other CN service.
	queryService queryservice.QueryService
	// queryClient is used to send query request to other CN services.
	queryClient qclient.QueryClient
	// udfService is used to handle non-sql udf
	udfService       udf.Service
	bootstrapService bootstrap.Service
	incrservice      incrservice.AutoIncrementService

	stopper *stopper.Stopper
	aicm    *defines.AutoIncrCacheManager

	task struct {
		sync.RWMutex
		holder         taskservice.TaskServiceHolder
		runner         taskservice.TaskRunner
		storageFactory taskservice.TaskStorageFactory
	}

	addressMgr address.AddressManager
	gossipNode *gossip.Node
	config     *util.ConfigData

	options struct {
		bootstrapOptions []bootstrap.Option
		traceDataPath    string
	}

	// pipelines record running pipelines in the service, used for monitoring.
	pipelines struct {
		// counter recording the total number of running pipelines,
		// details are not recorded for simplicity as suggested by @nnsgmsone
		counter atomic.Int64
	}
}

func dumpCnConfig(cfg Config) (map[string]*logservicepb.ConfigItem, error) {
	defCfg := Config{}
	defCfg.SetDefaultValue()
	return util.DumpConfig(cfg, defCfg)
}
