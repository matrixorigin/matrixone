// Copyright 2023 Matrix Origin
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

package executor

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// WithDisableIncrStatement disable incr statement
func (opts Options) WithDisableIncrStatement() Options {
	opts.disableIncrStatement = true
	return opts
}

// WithDisableIncrStatement disable incr statement
func (opts StatementOption) WithIgnoreForeignKey() StatementOption {
	opts.ignoreForeignKey = true
	return opts
}

// WithTxn exec sql in a exists txn
func (opts Options) WithTxn(txnOp client.TxnOperator) Options {
	opts.txnOp = txnOp
	return opts
}

// WithDatabase exec sql in database
func (opts Options) WithDatabase(database string) Options {
	opts.database = database
	return opts
}

// WithAccountID execute sql in account
func (opts Options) WithAccountID(accountID uint32) Options {
	opts.accountID = accountID
	opts.hasAccountID = true
	return opts
}

func (opts Options) WithTimeZone(timeZone *time.Location) Options {
	opts.timeZone = timeZone
	return opts
}

// WithMinCommittedTS use minCommittedTS to exec sql. It will set txn's snapshot to
// minCommittedTS+1, so the txn can see the data which committed at minCommittedTS.
// It's not work if txn operator is set.
func (opts Options) WithMinCommittedTS(ts timestamp.Timestamp) Options {
	opts.minCommittedTS = ts
	return opts
}

// WithWaitCommittedLogApplied if set, the executor will wait all committed log applied
// for the txn.
func (opts Options) WithWaitCommittedLogApplied() Options {
	opts.waitCommittedLogApplied = true
	return opts
}

// Database returns default database
func (opts Options) Database() string {
	return opts.database
}

// AccountID returns account id
func (opts Options) AccountID() uint32 {
	return opts.accountID
}

// HasAccountID returns true if account is set
func (opts Options) HasAccountID() bool {
	return opts.hasAccountID
}

// MinCommittedTS returns min committed ts
func (opts Options) MinCommittedTS() timestamp.Timestamp {
	return opts.minCommittedTS
}

// WaitCommittedLogApplied return true means need wait committed log applied in current cn.
func (opts Options) WaitCommittedLogApplied() bool {
	return opts.waitCommittedLogApplied
}

// HasExistsTxn return true if a exists txn is set
func (opts Options) HasExistsTxn() bool {
	return opts.txnOp != nil
}

// ExistsTxn return true if the txn is a exists txn which is not create by executor
func (opts Options) ExistsTxn() bool {
	return !opts.innerTxn
}

// SetupNewTxn setup new txn
func (opts Options) SetupNewTxn(txnOp client.TxnOperator) Options {
	opts.txnOp = txnOp
	opts.innerTxn = true
	return opts
}

// Txn returns the txn operator
func (opts Options) Txn() client.TxnOperator {
	return opts.txnOp
}

// DisableIncrStatement returns the txn operator need incr a new input statement
func (opts Options) DisableIncrStatement() bool {
	return opts.disableIncrStatement
}

// GetTimeZone return the time zone of original session
func (opts Options) GetTimeZone() *time.Location {
	l := opts.timeZone
	if l == nil {
		return time.Local
	}
	return l
}

// WithStatementOption set statement option
func (opts Options) WithStatementOption(statementOption StatementOption) Options {
	opts.statementOptions = statementOption
	return opts
}

// StatementOption returns statement options
func (opts Options) StatementOption() StatementOption {
	return opts.statementOptions
}

// WithWaitPolicy set wait policy for current statement
func (opts StatementOption) WithWaitPolicy(waitPolicy lock.WaitPolicy) StatementOption {
	opts.waitPolicy = waitPolicy
	return opts
}

// WaitPolicy returns the wait policy for current statement
func (opts StatementOption) WaitPolicy() lock.WaitPolicy {
	return opts.waitPolicy
}

// WithAccountID execute sql in account
func (opts StatementOption) WithAccountID(accountID uint32) StatementOption {
	opts.accountId = accountID
	opts.hasAccountID = true
	return opts
}

func (opts StatementOption) WithAlterCopyOpt(opt *plan.AlterCopyOpt) StatementOption {
	opts.alterCopyOpt = opt
	return opts
}

func (opts StatementOption) AlterCopyDedupOpt() *plan.AlterCopyOpt {
	return opts.alterCopyOpt
}

func (opts StatementOption) AccountID() uint32 {
	return opts.accountId
}

func (opts StatementOption) HasAccountID() bool {
	return opts.hasAccountID
}

func (opts StatementOption) WithRoleID(roleID uint32) StatementOption {
	opts.roleId = roleID
	return opts
}

func (opts StatementOption) RoleID() uint32 {
	return opts.roleId
}

func (opts StatementOption) HasRoleID() bool {
	return opts.roleId > 0
}

func (opts StatementOption) WithUserID(userID uint32) StatementOption {
	opts.userId = userID
	return opts
}

func (opts StatementOption) UserID() uint32 {
	return opts.userId
}

func (opts StatementOption) HasUserID() bool {
	return opts.userId > 0
}

func (opts StatementOption) WithDisableLog() StatementOption {
	opts.disableLog = true
	return opts
}

func (opts StatementOption) DisableLog() bool {
	return opts.disableLog
}

func (opts StatementOption) IgnoreForeignKey() bool {
	return opts.ignoreForeignKey
}

func (opts Options) WithDisableTrace() Options {
	opts.txnOpts = append(opts.txnOpts, client.WithDisableTrace(true))
	return opts
}

func (opts Options) WithDisableWaitPaused() Options {
	opts.txnOpts = append(opts.txnOpts, client.WithDisableWaitPaused())
	return opts
}

func (opts Options) WithUserTxn() Options {
	opts.txnOpts = append(opts.txnOpts, client.WithUserTxn())
	return opts
}

func (opts Options) ExtraTxnOptions() []client.TxnOption {
	return opts.txnOpts
}

func (opts Options) WithEnableTrace() Options {
	opts.enableTrace = true
	return opts
}

func (opts Options) EnableTrace() bool {
	return opts.enableTrace
}

func (opts Options) WithLowerCaseTableNames(lower *int64) Options {
	opts.lower = lower
	return opts
}

func (opts Options) WithSQL(sql string) Options {
	opts.sql = sql
	return opts
}

func (opts Options) WithKeepTxnAlive() Options {
	opts.keepTxnAlive = true
	return opts
}

func (opts Options) KeepTxnAlive() bool {
	return opts.keepTxnAlive
}

func (opts Options) SQL() string {
	return opts.sql
}

func (opts Options) LowerCaseTableNames() int64 {
	if opts.lower != nil {
		return *opts.lower
	}
	return 1
}

func (opts Options) WithStreaming(stream_chan chan Result, error_chan chan error) Options {
	opts.stream_chan = stream_chan
	opts.error_chan = error_chan
	opts.streaming = true
	return opts
}

func (opts Options) Streaming() (chan Result, chan error, bool) {
	return opts.stream_chan, opts.error_chan, opts.streaming
}

func (opts Options) WithResolveVariableFunc(fn func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error)) Options {
	opts.resolveVariableFunc = fn
	return opts
}

func (opts Options) ResolveVariableFunc() func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return opts.resolveVariableFunc
}

func (opts StatementOption) HasParams() bool {
	return len(opts.params) > 0
}

func (opts StatementOption) Params(
	mp *mpool.MPool,
) *vector.Vector {
	vec := vector.NewVec(types.T_varchar.ToType())
	vector.AppendStringList(
		vec,
		opts.params,
		make([]bool, len(opts.params)),
		mp,
	)
	return vec
}

func (opts StatementOption) WithParams(
	values []string,
) StatementOption {
	opts.params = values
	return opts
}

func (opts Options) WithForceRebuildPlan() Options {
	opts.forceRebuildPlan = true
	return opts
}

func (opts Options) ForceRebuildPlan() bool {
	return opts.forceRebuildPlan
}

func (opts Options) WithAdjustTableExtraFunc(
	fn func(*api.SchemaExtra) error,
) Options {
	opts.adjustTableExtraFunc = fn
	return opts
}

func (opts Options) AdjustTableExtraFunc() func(*api.SchemaExtra) error {
	if opts.adjustTableExtraFunc == nil {
		return func(*api.SchemaExtra) error { return nil }
	}
	return opts.adjustTableExtraFunc
}

func (opts StatementOption) DisableDropIncrStatement() bool {
	return opts.disableDropAutoIncrement
}

func (opts StatementOption) WithDisableDropIncrStatement() StatementOption {
	opts.disableDropAutoIncrement = true
	return opts
}

func (opts StatementOption) KeepAutoIncrement() uint64 {
	return opts.keepAutoIncrement
}

func (opts StatementOption) WithKeepAutoIncrement(keep uint64) StatementOption {
	opts.keepAutoIncrement = keep
	return opts
}

func (opts StatementOption) KeepLogicalId() uint64 {
	return opts.keepLogicalId
}

func (opts StatementOption) WithKeepLogicalId(keep uint64) StatementOption {
	opts.keepLogicalId = keep
	return opts
}

func (opts StatementOption) WithIgnorePublish() StatementOption {
	opts.ignorePublish = true
	return opts
}

func (opts StatementOption) IgnorePublish() bool {
	return opts.ignorePublish
}

func (opts StatementOption) WithIgnoreCheckExperimental() StatementOption {
	opts.ignoreCheckExperimental = true
	return opts
}

func (opts StatementOption) IgnoreCheckExperimental() bool {
	return opts.ignoreCheckExperimental
}

func (opts StatementOption) WithDisableLock() StatementOption {
	opts.disableLock = true
	return opts
}

func (opts StatementOption) DisableLock() bool {
	return opts.disableLock
}
