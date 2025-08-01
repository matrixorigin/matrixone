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
	"fmt"
	"testing"
)

var causeArray = []error{
	CauseSaveMallocProfileTimeout,
	CauseWaitHAKeeperReader1,
	CauseWaitHAKeeperReader2,
	CauseWaitHAKeeperRunning,
	CauseWaitAnyShardReady,

	CauseUpgradeOneTenant,
	CauseAsyncUpgradeTask,
	CauseAsyncUpgradeTenantTask,

	CauseFinishTxnOp,

	CauseDebugUpdateCNLabel,
	CauseDebugUpdateCNWorkState,
	CauseRefresh,

	CauseCheckTenantUpgrade,
	CauseUpgradeTenant,
	CauseGetHAKeeperClient,
	CauseBootstrap,
	CauseBootstrap2,
	CauseSaveProfile,
	CauseHeartbeat,
	CauseCanClaimDaemonTask,
	CauseMergeObject,

	CauseDeadlineContextCodec,
	CausePingPongMain,
	CauseStreamMain,

	CauseLogClientGetLeaderID,
	CauseLogClientWrite,
	CauseLogClientReadEntries,
	CauseLogClientTruncate,
	CauseLogClientGetTruncatedLsn,
	CauseLogClientGetLatestLsn,
	CauseLogClientSetRequiredLsn,
	CauseLogClientGetRequiredLsn,
	CauseTxnClientGetLatestCheckpoint,

	CauseWaitHAKeeperRunningLocked,
	CauseWaitHAKeeperReadyLocked,
	CauseWaitHAKeeperReadyLocked2,
	CauseWaitAnyShardReadyLocked,

	CauseNewAwsSDKv2,
	CauseReadCache,
	CauseRemoteCacheRead,

	CauseRegisterCdc,
	CauseInternalExecutorExec,
	CauseInternalExecutorQuery,
	CauseHandleRequest,
	CauseGetConnID,
	CauseHandshake,
	CauseHandshake2,
	CauseMigrate,
	CauseCommitUnsafe,
	CauseRollbackUnsafe,

	CauseQueryTasks,
	CauseAllocateTasks,
	CauseTruncateTasks,

	CauseDoAllocate,
	CauseDoUpdate,
	CauseDestroyTables,
	CauseAllocate,

	CauseCleanCommitState,
	CauseValidateService,
	CauseDoKeepRemoteLock,
	CauseDoKeepLockTableBind,
	CauseDoUnlock,
	CauseDoGetLock,
	CauseInitRemote1,
	CauseInitRemote2,
	CauseGetTxnWaitingListOnRemote,
	CauseGetLockTableBind,

	CauseNewStandbyClientWithRetry,
	CauseNewLogHAKeeperClientWithRetry,
	CauseHandleAddLogShard,
	CauseLogServiceHeartbeat,
	CauseGetShardInfo,
	CauseAddReplica,
	CauseAddNonVotingReplica,
	CauseRemoveReplica,
	CauseHakeeperTick,
	CauseSetInitialClusterInfo,
	CauseUpdateIDAlloc,
	CauseHealthCheck,
	CauseLogServiceBootstrap,
	CauseSetBootstrapState,
	CauseGetCheckerState,
	CauseSetTaskTableUser,
	CauseRunClientTest,
	CauseProcessShardTruncateLog,
	CauseProcessHAKeeperTruncation,

	CauseProxyBootstrap,
	CauseGenConnID,
	CauseResetSession,
	CauseMigrateConnFrom,
	CauseMigrateConnTo,
	CauseUpgradeToTLS,
	CauseDoHeartbeat,
	CauseRequest,
	CauseNewServer,
	CauseHandleHandshake,
	CauseTransfer,
	CauseTransferSync,

	CauseSend,
	CauseGet,
	CauseGetChanged,
	CauseUnsubscribe,

	CauseBuildInsertIndexMetaBatch,
	CauseBuildInsertIndexMetaBatch2,

	CauseWaitRemoteRegsReady,

	CauseIsAvailable,
	CauseNewMessageSenderOnClient,
	CauseWaitingTheStopResponse,
	CauseHandlePipelineMessage,
	CauseGetProcByUuid,
	CauseGenInsertMOIndexesSql,
	CauseGenInsertMOIndexesSql2,

	CauseHandleCoreDump,
	CauseHandleSyncCommit,
	CauseHandleRemoveRemoteLockTable,
	CauseHandleGetLatestBind,
	CauseHandleReloadAutoIncrementCache,
	CauseHandleGetProtocolVersion,
	CauseTransferToTN,
	CauseTransferToCN,
	CauseHandleTask,
	CauseTransferTaskToCN,
	CauseTransferRequest2OtherCNs,
	CauseDoUnsubscribeTable,

	CauseKafkaSinkConnectorExecutor,

	CauseResumeTaskHandle,
	CauseRestartTaskHandle,
	CausePauseTaskHandle,
	CauseCancelTaskHandle,
	CauseQueryDaemonTasks,
	CauseStartTasks,
	CauseDoFetch,
	CauseFetchCronTasks,
	CauseDoRun,
	CauseCheckConcurrency,

	CauseBuildTNOptions,
	CauseBuildTNOptions2,
	CauseTestClusterStart,

	CauseCreateTestDatabase,
	CauseExecSQL,
	CauseExecSQLWithMinCommittedTS,
	CauseDBExists,
	CauseTableExists,
	CauseWaitClusterAppliedTo,

	CauseClusterStart,

	CauseNewLogServiceClient,
	CauseInitHAKeeperClient,
	CauseTnServiceHeartbeat,

	CauseSyncLatestCommitT,

	CauseStartAsyncCommitTask,
	CauseTestSenderSend,

	CauseSaveLog,
	CauseNewCatalogHandler,

	CauseWatch,
	CauseUpdateState,
	CauseAddTableFilter,
	CauseClearTableFilters,
	CauseRefreshTableFilters,
	CauseWriteToMO,
	CauseWriteToS3,
	CauseAddStatementFilter,
	CauseClearStatementFilters,
	CauseRefreshStatementFilters,
	CauseAddTxnFilter,
	CauseClearTxnFilters,
	CauseRefreshTxnFilters,
	CauseDoAddTxnError,

	CauseAddressFunc,

	CauseWriteRowRecords,

	CauseReadFile,
	CauseWriteFile,

	CauseHakeeperStatsFill,

	CauseNewMOHungSpan,
	CauseShutdown,

	CauseReceiveOneLogtail,
	CauseReplayCatalogCache,
	CauseSubscribeTable,
	CauseUnSubscribeTable,
	CauseAllocateID,
	CauseShardingLocalReader,
	CauseHakeeperIDGeneratorNew,
	CauseHakeeperIDGeneratorNewIDByKey,

	CauseDoTxnRequest,

	CauseRetryWithIntervalAndTimeout,

	CauseCleanUpUselessFiles,
	CauseOnObject,

	CauseDriverAppender1,
	CauseDriverAppender2,
	CauseRetryWithTimeout,
	CauseNewTestConfig,
	CauseReadFromLogService,
	CauseReadFromLogService2,
	CauseAppendSkipCmd,
	CauseAppendSkipCmd2,
	CauseTruncateLogservice,
	CauseTruncateLogservice2,
	CauseGetLogserviceTruncate,
	CauseGetLogserviceTruncate2,

	CauseGetSubLogtailPhase,
	CauseSendSubscription,
	CauseNewSession,
	CausePublish,

	CauseLoadTable,
	CauseClearPersistTable,
	CauseWriteTransferPage,
	CauseNewTransferTable,

	CauseHandleWrite,
	CauseStorageUsageDetails,
	CauseObjGetArgGetData,
	CauseObjGetArgGetData2,

	CauseWaitFlushAObjForSnapshot,
	CauseReleaseFlushObjTasks,

	CausePrepareRollback,
	CausePrepareRollback2,

	CauseWaitLogtail,
	CauseInitEnginePack,

	CauseReceiveMessage,
}

func Test_Cause(t *testing.T) {
	SetContextFunc(func() context.Context { return context.Background() })
	for _, err := range causeArray {
		fmt.Println(err.Error())
	}
}
