package compile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestBloomFilterRemotePropagation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedBF := []byte("test_bloom_filter_distributed_content")

	// 1. Sender Side: Create a process with BF in context
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	ctx = context.WithValue(ctx, defines.IvfBloomFilter{}, expectedBF)

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("test-txn")}).AnyTimes()

	proc := process.NewTopProcess(ctx, nil, nil, txnOperator, nil, nil, nil, nil, nil, nil, nil)
	proc.Base.Id = "test-proc-id"
	proc.Base.UnixTime = time.Now().Unix()
	proc.Base.SessionInfo = process.SessionInfo{
		TimeZone: time.UTC,
	}

	// 2. Serialize: This simulates sending the process info to remote CN
	data, err := encodeProcessInfo(proc, "select 1")
	require.NoError(t, err)

	// Verify the serialized PB contains the BF
	var procInfo pipeline.ProcessInfo
	err = procInfo.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, expectedBF, procInfo.IvfBloomFilter, "Serialized PB should carry the Bloom Filter")

	// 3. Receiver Side: Reconstruct everything on remote CN
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	remoteTxnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	remoteTxnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	remoteTxnID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	remoteTxnOperator.EXPECT().Txn().Return(txn.TxnMeta{ID: remoteTxnID[:]}).AnyTimes()
	remoteTxnOperator.EXPECT().GetWorkspace().Return(nil).AnyTimes()

	txnClient.EXPECT().NewWithSnapshot(gomock.Any()).Return(remoteTxnOperator, nil).AnyTimes()

	// Generate processHelper
	helper, err := generateProcessHelper(data, txnClient)
	require.NoError(t, err)
	require.Equal(t, expectedBF, helper.bloomFilter, "processHelper should have reconstructed the Bloom Filter")

	// 4. Create new Compile: Verify it injects BF into process context
	mockEngine := mock_frontend.NewMockEngine(ctrl)
	messageCenter := &message.MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	mockEngine.EXPECT().GetMessageCenter().Return(messageCenter).AnyTimes()

	receiver := &messageReceiverOnServer{
		messageCtx:    ctx,
		connectionCtx: ctx,
		cnInformation: cnInformation{
			cnAddr:      "remote-cn-addr",
			storeEngine: mockEngine,
		},
		procBuildHelper: helper,
	}

	c, err := receiver.newCompile()
	require.NoError(t, err)
	require.NotNil(t, c.proc)

	// FINAL ASSERTION: Check if the remote process context has the BF
	actualBFVal := c.proc.Ctx.Value(defines.IvfBloomFilter{})
	require.NotNil(t, actualBFVal, "Remote process context should have the Bloom Filter")
	actualBF, ok := actualBFVal.([]byte)
	require.True(t, ok)
	require.Equal(t, expectedBF, actualBF, "Distributed Bloom Filter data should match exactly")
}
