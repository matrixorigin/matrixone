package client

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// WithTxnLogger setup txn logger
func WithTxnLogger(logger *zap.Logger) TxnOption {
	return func(tc *txnCoordinator) {
		tc.logger = logger
	}
}

// WithDelayWrites Set delayed write, after each Write call, the request will not be sent to the
// DN node immediately, but stored in the Coordinator's memory, and the Coordinator will choose
// the right time to send the cached requests. The following scenarios trigger the sending of
// requests to DN:
// 1. Before read, because the Coordinator is not aware of the format and content of the written data,
//    it is necessary to send the cached write requests to the corresponding DN node each time Read is
//    called, used to implement "read your write".
// 2. Before commit, obviously, the cache write requests needs to be sent to the corresponding DN node
//    before commit.
func WithDelayWrites() TxnOption {
	return func(tc *txnCoordinator) {
		tc.delayWrites = true
		tc.mu.cachedWrites = make(map[uint64][]txn.DNOpRequest)
	}
}

type txnCoordinator struct {
	logger *zap.Logger
	sender TxnSender

	readyOnly   bool
	delayWrites bool

	mu struct {
		sync.RWMutex
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.DNOpRequest
	}
}

func newTxnCoordinator(sender TxnSender, txnMeta txn.TxnMeta, options ...TxnOption) *txnCoordinator {
	tc := &txnCoordinator{sender: sender}
	tc.mu.txn = txnMeta

	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()
	return tc
}

func (tc *txnCoordinator) adjust() {
	tc.logger = logutil.Adjust(tc.logger)
	if tc.sender == nil {
		tc.logger.Fatal("missing txn sender")
	}
	if len(tc.mu.txn.ID) == 0 {
		tc.logger.Fatal("missing txn id")
	}
	if tc.mu.txn.SnapshotTS.IsEmpty() {
		tc.logger.Fatal("missing txn snapshot timestamp")
	}
	if tc.readyOnly && tc.delayWrites {
		tc.logger.Fatal("readyOnly and delayWrites cannot both be set")
	}

	if tc.logger.Core().Enabled(zapcore.DebugLevel) {
		tc.logger = tc.logger.With(util.TxnIDField(tc.mu.txn))
	}
}

func (tc *txnCoordinator) Read(ctx context.Context, ops []txn.DNOpRequest) ([]txn.DNOpResponse, error) {
	return nil, nil
}

func (tc *txnCoordinator) Write(ctx context.Context, ops []txn.DNOpRequest) ([]txn.DNOpResponse, error) {
	return nil, nil
}

func (tc *txnCoordinator) WriteAndCommit(ctx context.Context, ops []txn.DNOpRequest) ([]txn.DNOpResponse, error) {
	return nil, nil
}

func (tc *txnCoordinator) Commit(ctx context.Context) error {
	return nil
}

func (tc *txnCoordinator) Rollback(ctx context.Context) error {
	return nil
}
