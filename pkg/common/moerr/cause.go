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

package moerr

import (
	"context"
	"errors"
)

var (
	//cmd/mo-service
	CauseSaveMallocProfileTimeout = NewInternalError(context.Background(), "saveMallocProfile")
	CauseWaitHAKeeperReader1      = NewInternalError(context.Background(), "waitHAKeeperReader 1")
	CauseWaitHAKeeperReader2      = NewInternalError(context.Background(), "waitHAKeeperReader 2")
	CauseWaitHAKeeperRunning      = NewInternalError(context.Background(), "waitHAKeeperRunning")
	CauseWaitAnyShardReady        = NewInternalError(context.Background(), "waitAnyShardReady")
	//pkg/bootstrap
	CauseUpgradeOneTenant       = NewInternalError(context.Background(), "upgradeOneTenant")
	CauseAsyncUpgradeTask       = NewInternalError(context.Background(), "asyncUpgradeTask")
	CauseAsyncUpgradeTenantTask = NewInternalError(context.Background(), "asyncUpgradeTenantTask")
	//pkg/cdc
	CauseFinishTxnOp = NewInternalError(context.Background(), "finishTxnOp")
	//pkg/clusterservice
	CauseDebugUpdateCNLabel     = NewInternalError(context.Background(), "debugUpdateCNLabel")
	CauseDebugUpdateCNWorkState = NewInternalError(context.Background(), "debugUpdateCNWorkState")
	CauseRefresh                = NewInternalError(context.Background(), "cluster refresh")
	//pkg/cnservice
	CauseCheckTenantUpgrade = NewInternalError(context.Background(), "checkTenantUpgrade")
	CauseUpgradeTenant      = NewInternalError(context.Background(), "upgradeTenant")
	CauseGetHAKeeperClient  = NewInternalError(context.Background(), "getHAKeeperClient")
	CauseBootstrap          = NewInternalError(context.Background(), "cn service bootstrap")
	CauseBootstrap2         = NewInternalError(context.Background(), "cn service bootstrap 2")
	CauseSaveProfile        = NewInternalError(context.Background(), "save profile")
	CauseHeartbeat          = NewInternalError(context.Background(), "cn service heartbeat")
	CauseCanClaimDaemonTask = NewInternalError(context.Background(), "canClaimDaemonTask")
	CauseMergeObject        = NewInternalError(context.Background(), "merge object")
	CauseRetention          = NewInternalError(context.Background(), "retention")
	CauseRetention2         = NewInternalError(context.Background(), "retention 2")
	//pkg/common/morpc
	CauseDeadlineContextCodec = NewInternalError(context.Background(), "morpc deadlineContextCodec")
	CausePingPongMain         = NewInternalError(context.Background(), "morpc ping pong main")
	CauseStreamMain           = NewInternalError(context.Background(), "morpc stream main")
	//pkg/datasync
	CauseLogClientGetLeaderID         = NewInternalError(context.Background(), "log client getLeaderID")
	CauseLogClientWrite               = NewInternalError(context.Background(), "log client write")
	CauseLogClientReadEntries         = NewInternalError(context.Background(), "log client readEntries")
	CauseLogClientTruncate            = NewInternalError(context.Background(), "log client truncate")
	CauseLogClientGetTruncatedLsn     = NewInternalError(context.Background(), "log client getTruncatedLsn")
	CauseLogClientGetLatestLsn        = NewInternalError(context.Background(), "log client getLatestLsn")
	CauseLogClientSetRequiredLsn      = NewInternalError(context.Background(), "log client setRequiredLsn")
	CauseLogClientGetRequiredLsn      = NewInternalError(context.Background(), "log client getRequiredLsn")
	CauseTxnClientGetLatestCheckpoint = NewInternalError(context.Background(), "txn client getLatestCheckpoint")
	//pkg/embed
	CauseWaitHAKeeperRunningLocked = NewInternalError(context.Background(), "waitHAKeeperRunningLocked")
	CauseWaitHAKeeperReadyLocked   = NewInternalError(context.Background(), "waitHAKeeperReadyLocked")
	CauseWaitHAKeeperReadyLocked2  = NewInternalError(context.Background(), "waitHAKeeperReadyLocked 2")
	CauseWaitAnyShardReadyLocked   = NewInternalError(context.Background(), "waitAnyShardReadyLocked")
	//pkg/fileservice
	CauseNewAwsSDKv2     = NewInternalError(context.Background(), "fileservice newAwsSDKv2")
	CauseReadCache       = NewInternalError(context.Background(), "fileservice read cache")
	CauseRemoteCacheRead = NewInternalError(context.Background(), "fileservice remote cache read")
	//pkg/frontend
	CauseRegisterCdc           = NewInternalError(context.Background(), "register cdc")
	CauseInternalExecutorExec  = NewInternalError(context.Background(), "internal executor exec")
	CauseInternalExecutorQuery = NewInternalError(context.Background(), "internal executor query")
	CauseHandleRequest         = NewInternalError(context.Background(), "handle request")
	CauseGetConnID             = NewInternalError(context.Background(), "getConnID")
	CauseHandshake             = NewInternalError(context.Background(), "handshake")
	CauseHandshake2            = NewInternalError(context.Background(), "handshake 2")
	CauseMigrate               = NewInternalError(context.Background(), "migrate")
	CauseCommitUnsafe          = NewInternalError(context.Background(), "commitUnsafe")
	CauseRollbackUnsafe        = NewInternalError(context.Background(), "rollbackUnsafe")
	CauseCreateTxnOpUnsafe     = NewInternalError(context.Background(), "createTxnOpUnsafe")
	CauseCreateUnsafe          = NewInternalError(context.Background(), "createUnsafe")
	//pkg/hakeeper/task
	CauseQueryTasks    = NewInternalError(context.Background(), "queryTasks")
	CauseAllocateTasks = NewInternalError(context.Background(), "allocateTask")
	CauseTruncateTasks = NewInternalError(context.Background(), "truncateTasks")
	//pkg/incrservice
	CauseDoAllocate    = NewInternalError(context.Background(), "doAllocate")
	CauseDoUpdate      = NewInternalError(context.Background(), "doUpdate")
	CauseDestroyTables = NewInternalError(context.Background(), "destroyTables")
	CauseAllocate      = NewInternalError(context.Background(), "allocate")
	//pkg/lockservice
	CauseCleanCommitState          = NewInternalError(context.Background(), "cleanCommitState")
	CauseValidateService           = NewInternalError(context.Background(), "validateService")
	CauseDoKeepRemoteLock          = NewInternalError(context.Background(), "doKeepRemoteLock")
	CauseDoKeepLockTableBind       = NewInternalError(context.Background(), "doKeepLockTableBind")
	CauseDoUnlock                  = NewInternalError(context.Background(), "doUnlock")
	CauseDoGetLock                 = NewInternalError(context.Background(), "doGetLock")
	CauseInitRemote1               = NewInternalError(context.Background(), "initRemote 1")
	CauseInitRemote2               = NewInternalError(context.Background(), "initRemote 2")
	CauseGetTxnWaitingListOnRemote = NewInternalError(context.Background(), "getTxnWaitingListOnRemote")
	CauseGetLockTableBind          = NewInternalError(context.Background(), "getLockTableBind")
	//pkg/logservice
	CauseNewStandbyClientWithRetry     = NewInternalError(context.Background(), "NewStandbyClientWithRetry")
	CauseNewLogHAKeeperClientWithRetry = NewInternalError(context.Background(), "NewLogHAKeeperClientWithRetry")
	CauseHandleAddLogShard             = NewInternalError(context.Background(), "handleAddLogShard")
	CauseLogServiceHeartbeat           = NewInternalError(context.Background(), "log service heartbeat")
	CauseGetShardInfo                  = NewInternalError(context.Background(), "GetShardInfo")
	CauseAddReplica                    = NewInternalError(context.Background(), "addReplica")
	CauseAddNonVotingReplica           = NewInternalError(context.Background(), "addNonVotingReplica")
	CauseRemoveReplica                 = NewInternalError(context.Background(), "removeReplica")
	CauseHakeeperTick                  = NewInternalError(context.Background(), "hakeeperTick")
	CauseSetInitialClusterInfo         = NewInternalError(context.Background(), "setInitialClusterInfo")
	CauseUpdateIDAlloc                 = NewInternalError(context.Background(), "updateIDAlloc")
	CauseHealthCheck                   = NewInternalError(context.Background(), "healthCheck")
	CauseLogServiceBootstrap           = NewInternalError(context.Background(), "logservice bootstrap")
	CauseSetBootstrapState             = NewInternalError(context.Background(), "setBootstrapState")
	CauseGetCheckerState               = NewInternalError(context.Background(), "getCheckerState")
	CauseSetTaskTableUser              = NewInternalError(context.Background(), "setTaskTableUser")
	CauseRunClientTest                 = NewInternalError(context.Background(), "RunClientTest")
	CauseProcessShardTruncateLog       = NewInternalError(context.Background(), "processShardTruncateLog")
	CauseProcessHAKeeperTruncation     = NewInternalError(context.Background(), "processHAKeeperTruncation")
	//pkg/proxy
	CauseProxyBootstrap  = NewInternalError(context.Background(), "proxy Bootstrap")
	CauseGenConnID       = NewInternalError(context.Background(), "genConnID")
	CauseResetSession    = NewInternalError(context.Background(), "resetSession")
	CauseMigrateConnFrom = NewInternalError(context.Background(), "migrateConnFrom")
	CauseMigrateConnTo   = NewInternalError(context.Background(), "migrateConnTo")
	CauseUpgradeToTLS    = NewInternalError(context.Background(), "upgrade to TLS")
	CauseDoHeartbeat     = NewInternalError(context.Background(), "doHeartbeat")
	CauseRequest         = NewInternalError(context.Background(), "request")
	CauseNewServer       = NewInternalError(context.Background(), "newServer")
	CauseHandleHandshake = NewInternalError(context.Background(), "HandleHandshake")
	CauseTransfer        = NewInternalError(context.Background(), "transfer")
	CauseTransferSync    = NewInternalError(context.Background(), "transferSync")
	//pkg/shardservice
	CauseSend        = NewInternalError(context.Background(), "send")
	CauseGet         = NewInternalError(context.Background(), "get")
	CauseGetChanged  = NewInternalError(context.Background(), "GetChanged")
	CauseUnsubscribe = NewInternalError(context.Background(), "Unsubscribe")
	//pkg/sql/colexec
	CauseBuildInsertIndexMetaBatch  = NewInternalError(context.Background(), "buildInsertIndexMetaBatch")
	CauseBuildInsertIndexMetaBatch2 = NewInternalError(context.Background(), "buildInsertIndexMetaBatch 2")
	//pkg/sql/colexec/dispatch
	CauseWaitRemoteRegsReady = NewInternalError(context.Background(), "waitRemoteRegsReady")
	//pkg/sql/compile
	CauseIsAvailable              = NewInternalError(context.Background(), "isAvailable")
	CauseNewMessageSenderOnClient = NewInternalError(context.Background(), "newMessageSenderOnClient")
	CauseWaitingTheStopResponse   = NewInternalError(context.Background(), "waitingTheStopResponse")
	CauseHandlePipelineMessage    = NewInternalError(context.Background(), "handlePipelineMessage")
	CauseGetProcByUuid            = NewInternalError(context.Background(), "GetProcByUuid")
	CauseGenInsertMOIndexesSql    = NewInternalError(context.Background(), "genInsertMOIndexesSql")
	CauseGenInsertMOIndexesSql2   = NewInternalError(context.Background(), "genInsertMOIndexesSql 2")
	//pkg/sql/plan/function/ctl
	CauseHandleCoreDump                 = NewInternalError(context.Background(), "handleCoreDump")
	CauseHandleSyncCommit               = NewInternalError(context.Background(), "handleSyncCommit")
	CauseHandleRemoveRemoteLockTable    = NewInternalError(context.Background(), "handleRemoveRemoteLockTable")
	CauseHandleGetLatestBind            = NewInternalError(context.Background(), "handleGetLatestBind")
	CauseHandleReloadAutoIncrementCache = NewInternalError(context.Background(), "handleReloadAutoIncrementCache")
	CauseHandleGetProtocolVersion       = NewInternalError(context.Background(), "handleGetProtocolVersion")
	CauseTransferToTN                   = NewInternalError(context.Background(), "transferToTN")
	CauseTransferToCN                   = NewInternalError(context.Background(), "transferToCN")
	CauseHandleTask                     = NewInternalError(context.Background(), "handleTask")
	CauseTransferTaskToCN               = NewInternalError(context.Background(), "transferTaskToCN")
	CauseTransferRequest2OtherCNs       = NewInternalError(context.Background(), "transferRequest2OtherCNs")
	CauseDoUnsubscribeTable             = NewInternalError(context.Background(), "doUnsubscribeTable")
	//pkg/stream/connector
	CauseKafkaSinkConnectorExecutor = NewInternalError(context.Background(), "kafkaSinkConnectorExecutor")
	//pkg/taskservice
	CauseResumeTaskHandle  = NewInternalError(context.Background(), "resume task handle")
	CauseRestartTaskHandle = NewInternalError(context.Background(), "restart task handle")
	CausePauseTaskHandle   = NewInternalError(context.Background(), "pause task handle")
	CauseCancelTaskHandle  = NewInternalError(context.Background(), "cancel task handle")
	CauseQueryDaemonTasks  = NewInternalError(context.Background(), "query daemon tasks")
	CauseStartTasks        = NewInternalError(context.Background(), "start tasks")
	CauseDoFetch           = NewInternalError(context.Background(), "do fetch")
	CauseFetchCronTasks    = NewInternalError(context.Background(), "fetch cron tasks")
	CauseDoRun             = NewInternalError(context.Background(), "do run")
	CauseCheckConcurrency  = NewInternalError(context.Background(), "check concurrency")
	//pkg/tests/service
	CauseBuildTNOptions   = NewInternalError(context.Background(), "buildTNOptions")
	CauseBuildTNOptions2  = NewInternalError(context.Background(), "buildTNOptions 2")
	CauseTestClusterStart = NewInternalError(context.Background(), "test cluster start")
	//pkg/tests/testutils
	CauseCreateTestDatabase        = NewInternalError(context.Background(), "createTestDatabase")
	CauseExecSQL                   = NewInternalError(context.Background(), "ExecSQL")
	CauseExecSQLWithMinCommittedTS = NewInternalError(context.Background(), "ExecSQLWithMinCommittedTS")
	CauseDBExists                  = NewInternalError(context.Background(), "DBExists")
	CauseTableExists               = NewInternalError(context.Background(), "TableExists")
	CauseWaitClusterAppliedTo      = NewInternalError(context.Background(), "WaitClusterAppliedTo")
	//pkg/tests/txn
	CauseClusterStart = NewInternalError(context.Background(), "ClusterStart")
	//pkg/tnservice
	CauseNewLogServiceClient = NewInternalError(context.Background(), "NewLogServiceClient")
	CauseInitHAKeeperClient  = NewInternalError(context.Background(), "InitHAKeeperClient")
	CauseTnServiceHeartbeat  = NewInternalError(context.Background(), "tn Service Heartbeat")
	//pkg/txn/client
	CauseSyncLatestCommitT = NewInternalError(context.Background(), "SyncLatestCommitTS")
	//pkg/txn/service
	CauseStartAsyncCommitTask = NewInternalError(context.Background(), "StartAsyncCommitTask")
	CauseTestSenderSend       = NewInternalError(context.Background(), "TestSenderSend")
	//pkg/txn/storage/mem
	CauseSaveLog           = NewInternalError(context.Background(), "saveLog")
	CauseNewCatalogHandler = NewInternalError(context.Background(), "NewCatalogHandler")
	//pkg/txn/trace
	CauseWatch                   = NewInternalError(context.Background(), "txn trace Watch")
	CauseUpdateState             = NewInternalError(context.Background(), "txn trace UpdateState")
	CauseAddTableFilter          = NewInternalError(context.Background(), "txn trace AddTableFilter")
	CauseClearTableFilters       = NewInternalError(context.Background(), "txn trace ClearTableFilters")
	CauseRefreshTableFilters     = NewInternalError(context.Background(), "txn trace RefreshTableFilters")
	CauseWriteToMO               = NewInternalError(context.Background(), "txn trace WriteToMO")
	CauseWriteToS3               = NewInternalError(context.Background(), "txn trace WriteToS3")
	CauseAddStatementFilter      = NewInternalError(context.Background(), "AddStatementFilter")
	CauseClearStatementFilters   = NewInternalError(context.Background(), "ClearStatementFilters")
	CauseRefreshStatementFilters = NewInternalError(context.Background(), "RefreshStatementFilters")
	CauseAddTxnFilter            = NewInternalError(context.Background(), "AddTxnFilter")
	CauseClearTxnFilters         = NewInternalError(context.Background(), "ClearTxnFilters")
	CauseRefreshTxnFilters       = NewInternalError(context.Background(), "RefreshTxnFilters")
	CauseDoAddTxnError           = NewInternalError(context.Background(), "DoAddTxnError")
	//pkg/util
	CauseAddressFunc = NewInternalError(context.Background(), "AddressFunc")
	//pkg/util/export/etl/db
	CauseWriteRowRecords = NewInternalError(context.Background(), "WriteRowRecords")
	//pkg/util/file
	CauseReadFile  = NewInternalError(context.Background(), "ReadFile")
	CauseWriteFile = NewInternalError(context.Background(), "WriteFile")
	//pkg/util/status
	CauseHakeeperStatsFill = NewInternalError(context.Background(), "HakeeperStatsFill")
	//pkg/util/trace/impl/motrace
	CauseNewMOHungSpan = NewInternalError(context.Background(), "NewMOHungSpan")
	CauseShutdown      = NewInternalError(context.Background(), "Shutdown")
	//pkg/vm/engine/disttae
	CauseReceiveOneLogtail             = NewInternalError(context.Background(), "ReceiveOneLogtail")
	CauseReplayCatalogCache            = NewInternalError(context.Background(), "ReplayCatalogCache")
	CauseSubscribeTable                = NewInternalError(context.Background(), "SubscribeTable")
	CauseUnSubscribeTable              = NewInternalError(context.Background(), "unSubscribeTable")
	CauseAllocateID                    = NewInternalError(context.Background(), "AllocateID")
	CauseShardingLocalReader           = NewInternalError(context.Background(), "ShardingLocalReader Close")
	CauseHakeeperIDGeneratorNew        = NewInternalError(context.Background(), "HakeeperIDGenerator New")
	CauseHakeeperIDGeneratorNewIDByKey = NewInternalError(context.Background(), "HakeeperIDGenerator NewIDByKey")
	//pkg/vm/engine/memoryengine
	CauseDoTxnRequest = NewInternalError(context.Background(), "DoTxnRequest")
	//pkg/vm/engine/tae/common
	CauseRetryWithIntervalAndTimeout = NewInternalError(context.Background(), "RetryWithIntervalAndTimeout")
	//pkg/vm/engine/tae/db/merge
	CauseCleanUpUselessFiles = NewInternalError(context.Background(), "CleanUpUselessFiles")
	CauseOnObject            = NewInternalError(context.Background(), "OnObject")
	//pkg/vm/engine/tae/logstore/driver/logservicedriver
	CauseDriverAppender1        = NewInternalError(context.Background(), "DriverAppender append 1")
	CauseDriverAppender2        = NewInternalError(context.Background(), "DriverAppender append 2")
	CauseRetryWithTimeout       = NewInternalError(context.Background(), "RetryWithTimeout")
	CauseNewTestConfig          = NewInternalError(context.Background(), "NewTestConfig")
	CauseReadFromLogService     = NewInternalError(context.Background(), "ReadFromLogService")
	CauseReadFromLogService2    = NewInternalError(context.Background(), "ReadFromLogService 2")
	CauseAppendSkipCmd          = NewInternalError(context.Background(), "AppendSkipCmd")
	CauseAppendSkipCmd2         = NewInternalError(context.Background(), "AppendSkipCmd 2")
	CauseTruncateLogservice     = NewInternalError(context.Background(), "TruncateLogservice")
	CauseTruncateLogservice2    = NewInternalError(context.Background(), "TruncateLogservice 2")
	CauseGetLogserviceTruncate  = NewInternalError(context.Background(), "GetLogserviceTruncate")
	CauseGetLogserviceTruncate2 = NewInternalError(context.Background(), "GetLogserviceTruncate 2")
	//pkg/vm/engine/tae/logtail/service
	CauseGetSubLogtailPhase = NewInternalError(context.Background(), "GetSubLogtailPhase")
	CauseSendSubscription   = NewInternalError(context.Background(), "SendSubscription")
	CauseNewSession         = NewInternalError(context.Background(), "NewSession")
	CausePublish            = NewInternalError(context.Background(), "Publish")
	//pkg/vm/engine/tae/model
	CauseLoadTable         = NewInternalError(context.Background(), "LoadTable")
	CauseClearPersistTable = NewInternalError(context.Background(), "ClearPersistTable")
	CauseWriteTransferPage = NewInternalError(context.Background(), "WriteTransferPage")
	CauseNewTransferTable  = NewInternalError(context.Background(), "NewTransferTable")
	//pkg/vm/engine/tae/rpc
	CauseHandleWrite         = NewInternalError(context.Background(), "HandleWrite")
	CauseStorageUsageDetails = NewInternalError(context.Background(), "storageUsageDetails")
	CauseObjGetArgGetData    = NewInternalError(context.Background(), "ObjGetArg GetData")
	CauseObjGetArgGetData2   = NewInternalError(context.Background(), "ObjGetArg GetData 2")
	//pkg/vm/engine/tae/tables/jobs
	CauseWaitFlushAObjForSnapshot = NewInternalError(context.Background(), "WaitFlushAObjForSnapshot")
	CauseReleaseFlushObjTasks     = NewInternalError(context.Background(), "ReleaseFlushObjTasks")
	//pkg/vm/engine/tae/tables/txnentries
	CausePrepareRollback  = NewInternalError(context.Background(), "PrepareRollback")
	CausePrepareRollback2 = NewInternalError(context.Background(), "PrepareRollback 2")
	//pkg/vm/engine/test/testutil
	CauseWaitLogtail    = NewInternalError(context.Background(), "WaitLogtail")
	CauseInitEnginePack = NewInternalError(context.Background(), "InitEnginePack")
	//pkg/vm/message
	CauseReceiveMessage = NewInternalError(context.Background(), "ReceiveMessage")
)

func AttachCause(ctx context.Context, err error) error {
	if err == nil {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		err = errors.Join(err, context.Cause(ctx))
	}
	return err
}
