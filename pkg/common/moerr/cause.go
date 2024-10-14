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

var (
	//cmd/mo-service
	CauseSaveMallocProfileTimeout = NewInternalErrorNoCtx("saveMallocProfile")
	CauseWaitHAKeeperReader1      = NewInternalErrorNoCtx("waitHAKeeperReader 1")
	CauseWaitHAKeeperReader2      = NewInternalErrorNoCtx("waitHAKeeperReader 2")
	CauseWaitHAKeeperRunning      = NewInternalErrorNoCtx("waitHAKeeperRunning")
	CauseWaitAnyShardReady        = NewInternalErrorNoCtx("waitAnyShardReady")
	//pkg/bootstrap
	CauseUpgradeOneTenant       = NewInternalErrorNoCtx("upgradeOneTenant")
	CauseAsyncUpgradeTask       = NewInternalErrorNoCtx("asyncUpgradeTask")
	CauseAsyncUpgradeTenantTask = NewInternalErrorNoCtx("asyncUpgradeTenantTask")
	//pkg/cdc
	CauseFinishTxnOp = NewInternalErrorNoCtx("finishTxnOp")
	//pkg/clusterservice
	CauseDebugUpdateCNLabel     = NewInternalErrorNoCtx("debugUpdateCNLabel")
	CauseDebugUpdateCNWorkState = NewInternalErrorNoCtx("debugUpdateCNWorkState")
	CauseRefresh                = NewInternalErrorNoCtx("cluster refresh")
	//pkg/cnservice
	CauseCheckTenantUpgrade = NewInternalErrorNoCtx("checkTenantUpgrade")
	CauseUpgradeTenant      = NewInternalErrorNoCtx("upgradeTenant")
	CauseGetHAKeeperClient  = NewInternalErrorNoCtx("getHAKeeperClient")
	CauseBootstrap          = NewInternalErrorNoCtx("cn service bootstrap")
	CauseBootstrap2         = NewInternalErrorNoCtx("cn service bootstrap 2")
	CauseSaveProfile        = NewInternalErrorNoCtx("save profile")
	CauseHeartbeat          = NewInternalErrorNoCtx("cn service heartbeat")
	CauseCanClaimDaemonTask = NewInternalErrorNoCtx("canClaimDaemonTask")
	CauseMergeObject        = NewInternalErrorNoCtx("merge object")
	CauseRetention          = NewInternalErrorNoCtx("retention")
	CauseRetention2         = NewInternalErrorNoCtx("retention 2")
	//pkg/common/morpc
	CauseDeadlineContextCodec = NewInternalErrorNoCtx("morpc deadlineContextCodec")
	CausePingPongMain         = NewInternalErrorNoCtx("morpc ping pong main")
	CauseStreamMain           = NewInternalErrorNoCtx("morpc stream main")
	//pkg/datasync
	CauseLogClientGetLeaderID         = NewInternalErrorNoCtx("log client getLeaderID")
	CauseLogClientWrite               = NewInternalErrorNoCtx("log client write")
	CauseLogClientReadEntries         = NewInternalErrorNoCtx("log client readEntries")
	CauseLogClientTruncate            = NewInternalErrorNoCtx("log client truncate")
	CauseLogClientGetTruncatedLsn     = NewInternalErrorNoCtx("log client getTruncatedLsn")
	CauseLogClientGetLatestLsn        = NewInternalErrorNoCtx("log client getLatestLsn")
	CauseLogClientSetRequiredLsn      = NewInternalErrorNoCtx("log client setRequiredLsn")
	CauseLogClientGetRequiredLsn      = NewInternalErrorNoCtx("log client getRequiredLsn")
	CauseTxnClientGetLatestCheckpoint = NewInternalErrorNoCtx("txn client getLatestCheckpoint")
	//pkg/embed
	CauseWaitHAKeeperRunningLocked = NewInternalErrorNoCtx("waitHAKeeperRunningLocked")
	CauseWaitHAKeeperReadyLocked   = NewInternalErrorNoCtx("waitHAKeeperReadyLocked")
	CauseWaitHAKeeperReadyLocked2  = NewInternalErrorNoCtx("waitHAKeeperReadyLocked 2")
	CauseWaitAnyShardReadyLocked   = NewInternalErrorNoCtx("waitAnyShardReadyLocked")
	//pkg/fileservice
	CauseNewAwsSDKv2     = NewInternalErrorNoCtx("fileservice newAwsSDKv2")
	CauseReadCache       = NewInternalErrorNoCtx("fileservice read cache")
	CauseRemoteCacheRead = NewInternalErrorNoCtx("fileservice remote cache read")
	//pkg/frontend
	CauseRegisterCdc           = NewInternalErrorNoCtx("register cdc")
	CauseInternalExecutorExec  = NewInternalErrorNoCtx("internal executor exec")
	CauseInternalExecutorQuery = NewInternalErrorNoCtx("internal executor query")
	CauseHandleRequest         = NewInternalErrorNoCtx("handle request")
	CauseGetConnID             = NewInternalErrorNoCtx("getConnID")
	CauseHandshake             = NewInternalErrorNoCtx("handshake")
	CauseHandshake2            = NewInternalErrorNoCtx("handshake 2")
	CauseMigrate               = NewInternalErrorNoCtx("migrate")
	CauseCommitUnsafe          = NewInternalErrorNoCtx("commitUnsafe")
	CauseRollbackUnsafe        = NewInternalErrorNoCtx("rollbackUnsafe")
	//pkg/hakeeper/task
	CauseQueryTasks    = NewInternalErrorNoCtx("queryTasks")
	CauseAllocateTasks = NewInternalErrorNoCtx("allocateTask")
	CauseTruncateTasks = NewInternalErrorNoCtx("truncateTasks")
	//pkg/incrservice
	CauseDoAllocate    = NewInternalErrorNoCtx("doAllocate")
	CauseDoUpdate      = NewInternalErrorNoCtx("doUpdate")
	CauseDestroyTables = NewInternalErrorNoCtx("destroyTables")
	CauseAllocate      = NewInternalErrorNoCtx("allocate")
	//pkg/lockservice
	CauseCleanCommitState          = NewInternalErrorNoCtx("cleanCommitState")
	CauseValidateService           = NewInternalErrorNoCtx("validateService")
	CauseDoKeepRemoteLock          = NewInternalErrorNoCtx("doKeepRemoteLock")
	CauseDoKeepLockTableBind       = NewInternalErrorNoCtx("doKeepLockTableBind")
	CauseDoUnlock                  = NewInternalErrorNoCtx("doUnlock")
	CauseDoGetLock                 = NewInternalErrorNoCtx("doGetLock")
	CauseInitRemote1               = NewInternalErrorNoCtx("initRemote 1")
	CauseInitRemote2               = NewInternalErrorNoCtx("initRemote 2")
	CauseGetTxnWaitingListOnRemote = NewInternalErrorNoCtx("getTxnWaitingListOnRemote")
	CauseGetLockTableBind          = NewInternalErrorNoCtx("getLockTableBind")
	//pkg/logservice
	CauseNewStandbyClientWithRetry     = NewInternalErrorNoCtx("NewStandbyClientWithRetry")
	CauseNewLogHAKeeperClientWithRetry = NewInternalErrorNoCtx("NewLogHAKeeperClientWithRetry")
	CauseHandleAddLogShard             = NewInternalErrorNoCtx("handleAddLogShard")
	CauseLogServiceHeartbeat           = NewInternalErrorNoCtx("log service heartbeat")
	CauseGetShardInfo                  = NewInternalErrorNoCtx("GetShardInfo")
	CauseAddReplica                    = NewInternalErrorNoCtx("addReplica")
	CauseAddNonVotingReplica           = NewInternalErrorNoCtx("addNonVotingReplica")
	CauseRemoveReplica                 = NewInternalErrorNoCtx("removeReplica")
	CauseHakeeperTick                  = NewInternalErrorNoCtx("hakeeperTick")
	CauseSetInitialClusterInfo         = NewInternalErrorNoCtx("setInitialClusterInfo")
	CauseUpdateIDAlloc                 = NewInternalErrorNoCtx("updateIDAlloc")
	CauseHealthCheck                   = NewInternalErrorNoCtx("healthCheck")
	CauseLogServiceBootstrap           = NewInternalErrorNoCtx("logservice bootstrap")
	CauseSetBootstrapState             = NewInternalErrorNoCtx("setBootstrapState")
	CauseGetCheckerState               = NewInternalErrorNoCtx("getCheckerState")
	CauseSetTaskTableUser              = NewInternalErrorNoCtx("setTaskTableUser")
	CauseRunClientTest                 = NewInternalErrorNoCtx("RunClientTest")
	CauseProcessShardTruncateLog       = NewInternalErrorNoCtx("processShardTruncateLog")
	CauseProcessHAKeeperTruncation     = NewInternalErrorNoCtx("processHAKeeperTruncation")
	//pkg/proxy
	CauseProxyBootstrap  = NewInternalErrorNoCtx("proxy Bootstrap")
	CauseGenConnID       = NewInternalErrorNoCtx("genConnID")
	CauseResetSession    = NewInternalErrorNoCtx("resetSession")
	CauseMigrateConnFrom = NewInternalErrorNoCtx("migrateConnFrom")
	CauseMigrateConnTo   = NewInternalErrorNoCtx("migrateConnTo")
	CauseUpgradeToTLS    = NewInternalErrorNoCtx("upgrade to TLS")
	CauseDoHeartbeat     = NewInternalErrorNoCtx("doHeartbeat")
	CauseRequest         = NewInternalErrorNoCtx("request")
	CauseNewServer       = NewInternalErrorNoCtx("newServer")
	CauseHandleHandshake = NewInternalErrorNoCtx("HandleHandshake")
	CauseTransfer        = NewInternalErrorNoCtx("transfer")
	CauseTransferSync    = NewInternalErrorNoCtx("transferSync")
	//pkg/shardservice
	CauseSend        = NewInternalErrorNoCtx("send")
	CauseGet         = NewInternalErrorNoCtx("get")
	CauseGetChanged  = NewInternalErrorNoCtx("GetChanged")
	CauseUnsubscribe = NewInternalErrorNoCtx("Unsubscribe")
	//pkg/sql/colexec
	CauseBuildInsertIndexMetaBatch  = NewInternalErrorNoCtx("buildInsertIndexMetaBatch")
	CauseBuildInsertIndexMetaBatch2 = NewInternalErrorNoCtx("buildInsertIndexMetaBatch 2")
	//pkg/sql/colexec/dispatch
	CauseWaitRemoteRegsReady = NewInternalErrorNoCtx("waitRemoteRegsReady")
	//pkg/sql/compile
	CauseIsAvailable              = NewInternalErrorNoCtx("isAvailable")
	CauseNewMessageSenderOnClient = NewInternalErrorNoCtx("newMessageSenderOnClient")
	CauseWaitingTheStopResponse   = NewInternalErrorNoCtx("waitingTheStopResponse")
	CauseHandlePipelineMessage    = NewInternalErrorNoCtx("handlePipelineMessage")
	CauseGetProcByUuid            = NewInternalErrorNoCtx("GetProcByUuid")
	CauseGenInsertMOIndexesSql    = NewInternalErrorNoCtx("genInsertMOIndexesSql")
	CauseGenInsertMOIndexesSql2   = NewInternalErrorNoCtx("genInsertMOIndexesSql 2")
	//pkg/sql/plan/function/ctl
	CauseHandleCoreDump                 = NewInternalErrorNoCtx("handleCoreDump")
	CauseHandleSyncCommit               = NewInternalErrorNoCtx("handleSyncCommit")
	CauseHandleRemoveRemoteLockTable    = NewInternalErrorNoCtx("handleRemoveRemoteLockTable")
	CauseHandleGetLatestBind            = NewInternalErrorNoCtx("handleGetLatestBind")
	CauseHandleReloadAutoIncrementCache = NewInternalErrorNoCtx("handleReloadAutoIncrementCache")
	CauseHandleGetProtocolVersion       = NewInternalErrorNoCtx("handleGetProtocolVersion")
	CauseTransferToTN                   = NewInternalErrorNoCtx("transferToTN")
	CauseTransferToCN                   = NewInternalErrorNoCtx("transferToCN")
	CauseHandleTask                     = NewInternalErrorNoCtx("handleTask")
	CauseTransferTaskToCN               = NewInternalErrorNoCtx("transferTaskToCN")
	CauseTransferRequest2OtherCNs       = NewInternalErrorNoCtx("transferRequest2OtherCNs")
	CauseDoUnsubscribeTable             = NewInternalErrorNoCtx("doUnsubscribeTable")
	//pkg/stream/connector
	CauseKafkaSinkConnectorExecutor = NewInternalErrorNoCtx("kafkaSinkConnectorExecutor")
	//pkg/taskservice
	CauseResumeTaskHandle  = NewInternalErrorNoCtx("resume task handle")
	CauseRestartTaskHandle = NewInternalErrorNoCtx("restart task handle")
	CausePauseTaskHandle   = NewInternalErrorNoCtx("pause task handle")
	CauseCancelTaskHandle  = NewInternalErrorNoCtx("cancel task handle")
	CauseQueryDaemonTasks  = NewInternalErrorNoCtx("query daemon tasks")
	CauseStartTasks        = NewInternalErrorNoCtx("start tasks")
	CauseDoFetch           = NewInternalErrorNoCtx("do fetch")
	CauseFetchCronTasks    = NewInternalErrorNoCtx("fetch cron tasks")
	CauseDoRun             = NewInternalErrorNoCtx("do run")
	CauseCheckConcurrency  = NewInternalErrorNoCtx("check concurrency")
	//pkg/tests/service
	CauseBuildTNOptions   = NewInternalErrorNoCtx("buildTNOptions")
	CauseBuildTNOptions2  = NewInternalErrorNoCtx("buildTNOptions 2")
	CauseTestClusterStart = NewInternalErrorNoCtx("test cluster start")
	//pkg/tests/testutils
	CauseCreateTestDatabase        = NewInternalErrorNoCtx("createTestDatabase")
	CauseExecSQL                   = NewInternalErrorNoCtx("ExecSQL")
	CauseExecSQLWithMinCommittedTS = NewInternalErrorNoCtx("ExecSQLWithMinCommittedTS")
	CauseDBExists                  = NewInternalErrorNoCtx("DBExists")
	CauseTableExists               = NewInternalErrorNoCtx("TableExists")
	CauseWaitClusterAppliedTo      = NewInternalErrorNoCtx("WaitClusterAppliedTo")
	//pkg/tests/txn
	CauseClusterStart = NewInternalErrorNoCtx("ClusterStart")
	//pkg/tnservice
	CauseNewLogServiceClient = NewInternalErrorNoCtx("NewLogServiceClient")
	CauseInitHAKeeperClient  = NewInternalErrorNoCtx("InitHAKeeperClient")
	CauseTnServiceHeartbeat  = NewInternalErrorNoCtx("tn Service Heartbeat")
	//pkg/txn/client
	CauseSyncLatestCommitT = NewInternalErrorNoCtx("SyncLatestCommitTS")
	//pkg/txn/service
	CauseStartAsyncCommitTask = NewInternalErrorNoCtx("StartAsyncCommitTask")
	CauseTestSenderSend       = NewInternalErrorNoCtx("TestSenderSend")
	//pkg/txn/storage/mem
	CauseSaveLog           = NewInternalErrorNoCtx("saveLog")
	CauseNewCatalogHandler = NewInternalErrorNoCtx("NewCatalogHandler")
	//pkg/txn/trace
	CauseWatch                   = NewInternalErrorNoCtx("txn trace Watch")
	CauseUpdateState             = NewInternalErrorNoCtx("txn trace UpdateState")
	CauseAddTableFilter          = NewInternalErrorNoCtx("txn trace AddTableFilter")
	CauseClearTableFilters       = NewInternalErrorNoCtx("txn trace ClearTableFilters")
	CauseRefreshTableFilters     = NewInternalErrorNoCtx("txn trace RefreshTableFilters")
	CauseWriteToMO               = NewInternalErrorNoCtx("txn trace WriteToMO")
	CauseWriteToS3               = NewInternalErrorNoCtx("txn trace WriteToS3")
	CauseAddStatementFilter      = NewInternalErrorNoCtx("AddStatementFilter")
	CauseClearStatementFilters   = NewInternalErrorNoCtx("ClearStatementFilters")
	CauseRefreshStatementFilters = NewInternalErrorNoCtx("RefreshStatementFilters")
	CauseAddTxnFilter            = NewInternalErrorNoCtx("AddTxnFilter")
	CauseClearTxnFilters         = NewInternalErrorNoCtx("ClearTxnFilters")
	CauseRefreshTxnFilters       = NewInternalErrorNoCtx("RefreshTxnFilters")
	CauseDoAddTxnError           = NewInternalErrorNoCtx("DoAddTxnError")
	//pkg/util
	CauseAddressFunc = NewInternalErrorNoCtx("AddressFunc")
	//pkg/util/export/etl/db
	CauseWriteRowRecords = NewInternalErrorNoCtx("WriteRowRecords")
	//pkg/util/file
	CauseReadFile  = NewInternalErrorNoCtx("ReadFile")
	CauseWriteFile = NewInternalErrorNoCtx("WriteFile")
	//pkg/util/status
	CauseHakeeperStatsFill = NewInternalErrorNoCtx("HakeeperStatsFill")
	//pkg/util/trace/impl/motrace
	CauseNewMOHungSpan = NewInternalErrorNoCtx("NewMOHungSpan")
	CauseShutdown      = NewInternalErrorNoCtx("Shutdown")
	//pkg/vm/engine/disttae
	CauseReceiveOneLogtail             = NewInternalErrorNoCtx("ReceiveOneLogtail")
	CauseReplayCatalogCache            = NewInternalErrorNoCtx("ReplayCatalogCache")
	CauseSubscribeTable                = NewInternalErrorNoCtx("SubscribeTable")
	CauseUnSubscribeTable              = NewInternalErrorNoCtx("unSubscribeTable")
	CauseAllocateID                    = NewInternalErrorNoCtx("AllocateID")
	CauseShardingLocalReader           = NewInternalErrorNoCtx("ShardingLocalReader Close")
	CauseHakeeperIDGeneratorNew        = NewInternalErrorNoCtx("HakeeperIDGenerator New")
	CauseHakeeperIDGeneratorNewIDByKey = NewInternalErrorNoCtx("HakeeperIDGenerator NewIDByKey")
	//pkg/vm/engine/memoryengine
	CauseDoTxnRequest = NewInternalErrorNoCtx("DoTxnRequest")
	//pkg/vm/engine/tae/common
	CauseRetryWithIntervalAndTimeout = NewInternalErrorNoCtx("RetryWithIntervalAndTimeout")
	//pkg/vm/engine/tae/db/merge
	CauseCleanUpUselessFiles = NewInternalErrorNoCtx("CleanUpUselessFiles")
	CauseOnObject            = NewInternalErrorNoCtx("OnObject")
	//pkg/vm/engine/tae/logstore/driver/logservicedriver
	CauseDriverAppender1        = NewInternalErrorNoCtx("DriverAppender append 1")
	CauseDriverAppender2        = NewInternalErrorNoCtx("DriverAppender append 2")
	CauseRetryWithTimeout       = NewInternalErrorNoCtx("RetryWithTimeout")
	CauseNewTestConfig          = NewInternalErrorNoCtx("NewTestConfig")
	CauseReadFromLogService     = NewInternalErrorNoCtx("ReadFromLogService")
	CauseReadFromLogService2    = NewInternalErrorNoCtx("ReadFromLogService 2")
	CauseAppendSkipCmd          = NewInternalErrorNoCtx("AppendSkipCmd")
	CauseAppendSkipCmd2         = NewInternalErrorNoCtx("AppendSkipCmd 2")
	CauseTruncateLogservice     = NewInternalErrorNoCtx("TruncateLogservice")
	CauseTruncateLogservice2    = NewInternalErrorNoCtx("TruncateLogservice 2")
	CauseGetLogserviceTruncate  = NewInternalErrorNoCtx("GetLogserviceTruncate")
	CauseGetLogserviceTruncate2 = NewInternalErrorNoCtx("GetLogserviceTruncate 2")
	//pkg/vm/engine/tae/logtail/service
	CauseGetSubLogtailPhase = NewInternalErrorNoCtx("GetSubLogtailPhase")
	CauseSendSubscription   = NewInternalErrorNoCtx("SendSubscription")
	CauseNewSession         = NewInternalErrorNoCtx("NewSession")
	CausePublish            = NewInternalErrorNoCtx("Publish")
	//pkg/vm/engine/tae/model
	CauseLoadTable         = NewInternalErrorNoCtx("LoadTable")
	CauseClearPersistTable = NewInternalErrorNoCtx("ClearPersistTable")
	CauseWriteTransferPage = NewInternalErrorNoCtx("WriteTransferPage")
	CauseNewTransferTable  = NewInternalErrorNoCtx("NewTransferTable")
	//pkg/vm/engine/tae/rpc
	CauseHandleWrite         = NewInternalErrorNoCtx("HandleWrite")
	CauseStorageUsageDetails = NewInternalErrorNoCtx("storageUsageDetails")
	CauseObjGetArgGetData    = NewInternalErrorNoCtx("ObjGetArg GetData")
	CauseObjGetArgGetData2   = NewInternalErrorNoCtx("ObjGetArg GetData 2")
	//pkg/vm/engine/tae/tables/jobs
	CauseWaitFlushAObjForSnapshot = NewInternalErrorNoCtx("WaitFlushAObjForSnapshot")
	CauseReleaseFlushObjTasks     = NewInternalErrorNoCtx("ReleaseFlushObjTasks")
	//pkg/vm/engine/tae/tables/txnentries
	CausePrepareRollback  = NewInternalErrorNoCtx("PrepareRollback")
	CausePrepareRollback2 = NewInternalErrorNoCtx("PrepareRollback 2")
	//pkg/vm/engine/test/testutil
	CauseWaitLogtail    = NewInternalErrorNoCtx("WaitLogtail")
	CauseInitEnginePack = NewInternalErrorNoCtx("InitEnginePack")
	//pkg/vm/message
	CauseReceiveMessage = NewInternalErrorNoCtx("ReceiveMessage")
)
