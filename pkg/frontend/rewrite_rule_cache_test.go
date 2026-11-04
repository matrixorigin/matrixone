// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestPreparedRefreshFailureReleasesProtocolResources(t *testing.T) {
	for _, command := range []CommandType{COM_STMT_EXECUTE, COM_STMT_FETCH} {
		for _, failure := range []string{"rule read", "canceled", "deadline"} {
			t.Run(fmt.Sprintf("command_%d/%s", command, failure), func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ses := newTestSession(t, ctrl)
				ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
				seedRewriteRuleCacheProofForTest(ses)
				ses.ruleCache = map[string]string{}
				bh := &backgroundExecTest{}
				bh.init()
				bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId(nil)
				want := errors.New("rule read failed")
				bh.sql2err[getSqlForRoleRulesOfRoleIDs([]int64{10})] = want
				proofErr := errors.New("proof unavailable")
				if failure == "canceled" {
					proofErr, want = context.Canceled, context.Canceled
				} else if failure == "deadline" {
					proofErr, want = context.DeadlineExceeded, context.DeadlineExceeded
				}
				ses.ruleCacheVersionProvider = func(context.Context, *Session) (any, error) { return nil, proofErr }
				stub := gostub.StubFunc(&NewBackgroundExec, bh)
				defer stub.Reset()
				stmt := &PrepareStmt{Name: getPrepareStmtName(321), proc: ses.GetProc(), rewritePolicyCaptured: true}
				defer stmt.Close()
				ses.prepareStmts[stmt.Name] = stmt
				const retained = 16
				require.True(t, ses.tryReservePreparedCursorBytes(retained, 1024))
				stmt.cursor = &preparedStmtCursor{owner: ses, bytes: retained, result: &MysqlResultSet{Data: [][]interface{}{{1}}}}
				if command == COM_STMT_EXECUTE {
					stmt.params = vector.NewVec(types.T_text.ToType())
					require.NoError(t, vector.AppendBytes(stmt.params, []byte("pending"), false, ses.GetProc().Mp()))
					stmt.getFromSendLongData = map[int]struct{}{0: {}}
				}
				data := make([]byte, 8)
				binary.LittleEndian.PutUint32(data, 321)
				binary.LittleEndian.PutUint32(data[4:], 1)
				ctx := context.Background()
				execCtx := newTestExecCtx(ctx, ctrl)
				resp, err := ExecRequest(ses, execCtx, &Request{cmd: command, data: data})
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, ErrorResponse, resp.category)
				require.ErrorIs(t, resp.GetData().(error), want)
				require.Nil(t, stmt.cursor)
				require.Zero(t, ses.preparedCursorBytes.Load())
				require.Nil(t, stmt.params)
				require.Empty(t, stmt.getFromSendLongData)
				require.Equal(t, ctx, execCtx.reqCtx, "request marker must not survive ExecCtx reuse")
				before := len(bh.executedSQLs)
				got, err := ses.getPrepareStmtAllowInvalidated(ctx, stmt.Name)
				require.NoError(t, err)
				require.Same(t, stmt, got)
				require.Len(t, bh.executedSQLs, before, "cleanup-only lookup performs no I/O")
			})
		}
	}
}

func TestPreparedFreshnessAdmissionRequestScope(t *testing.T) {
	for _, change := range []string{"stable", "text capture", "next request", "different session", "principal", "generation", "revision", "canceled", "failure retry", "ended request"} {
		t.Run(change, func(t *testing.T) {
			ses := newSes(&privilege{}, gomock.NewController(t))
			ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
			ses.ruleCache = map[string]string{}
			seedRewriteRuleCacheProofForTest(ses)
			calls := 0
			var proofErr error
			ses.ruleCacheVersionProvider = func(context.Context, *Session) (any, error) {
				unlocked := ses.mu.TryLock()
				if unlocked {
					ses.mu.Unlock()
				}
				require.True(t, unlocked, "a real prepared refresh must not hold the session lock")
				calls++
				return 1, proofErr
			}
			stmt := &PrepareStmt{Name: "saved", rewritePolicyCaptured: true}
			ses.prepareStmts[stmt.Name] = stmt
			ctx := context.WithValue(context.Background(), rewritePolicyRequestKey{}, &rewritePolicyRequest{owner: ses})
			lookup := func(ctx context.Context) {
				t.Helper()
				got, err := ses.GetPrepareStmt(ctx, stmt.Name)
				require.NoError(t, err)
				require.Same(t, stmt, got)
			}
			if change == "text capture" {
				_, err := captureRewritePolicy(ctx, ses)
				require.NoError(t, err)
			}
			lookup(ctx)
			lookup(ctx)
			lookup(ctx)
			require.Equal(t, 1, calls, "parse, wrapper and parameter checks share one role-proof admission")
			switch change {
			case "stable", "text capture":
				return
			case "next request":
				ctx = context.WithValue(ctx, rewritePolicyRequestKey{}, &rewritePolicyRequest{owner: ses})
			case "different session":
				ctx = context.WithValue(ctx, rewritePolicyRequestKey{}, &rewritePolicyRequest{owner: &Session{}})
			case "principal":
				// An identity change invalidates the marker even when another
				// owner has already installed an equal cache for that identity.
				ses.tenant.UserID++
				ses.ruleCacheIdentity = rewriteRuleSessionIdentity(ses)
			case "generation":
				ses.bumpRewritePolicyGeneration()
			case "revision":
				ses.ruleCacheMu.Lock()
				ses.ruleCacheRevision++
				ses.ruleCacheMu.Unlock()
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				got, err := ses.GetPrepareStmt(canceled, stmt.Name)
				require.ErrorIs(t, err, context.Canceled)
				require.Same(t, stmt, got)
				require.False(t, preparedPolicyAlreadyRefreshed(ctx, ses))
				return
			case "failure retry":
				proofErr = context.DeadlineExceeded
				_, err := captureRewritePolicy(ctx, ses)
				require.ErrorIs(t, err, proofErr)
				require.False(t, preparedPolicyAlreadyRefreshed(ctx, ses))
				proofErr = nil
				_, err = captureRewritePolicy(ctx, ses)
				require.NoError(t, err)
				require.Equal(t, 3, calls)
				return
			case "ended request":
				request := rewritePolicyRequestFor(ctx, ses)
				request.mu.Lock()
				request.valid, request.ended = false, true
				request.mu.Unlock()
				_, err := captureRewritePolicy(ctx, ses)
				require.NoError(t, err)
				require.False(t, preparedPolicyAlreadyRefreshed(ctx, ses), "a retained context cannot reactivate the request")
				return
			}
			lookup(ctx)
			require.Equal(t, 2, calls)
		})
	}
}

func TestPreparedFreshnessRequestEndsDuringRefresh(t *testing.T) {
	ses := newSes(&privilege{}, gomock.NewController(t))
	ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
	ses.ruleCache = map[string]string{}
	seedRewriteRuleCacheProofForTest(ses)
	request := &rewritePolicyRequest{owner: ses}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, rewritePolicyRequestKey{}, request)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	ses.ruleCacheVersionProvider = func(ctx context.Context, _ *Session) (any, error) {
		close(entered)
		select {
		case <-resume:
			return 1, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() {
		defer close(done)
		_, err := captureRewritePolicy(ctx, ses)
		result <- err
	}()
	t.Cleanup(func() { release(); <-done })
	select {
	case <-entered:
	case <-done:
		t.Fatal("refresh did not reach snapshot admission")
	}
	// Ending a request never waits for its outstanding catalog admission.
	request.mu.Lock()
	request.valid, request.ended = false, true
	request.mu.Unlock()
	release()
	<-done
	require.NoError(t, <-result)
	require.False(t, preparedPolicyAlreadyRefreshed(ctx, ses), "late success must not reactivate the ended request")
}

// SQL-recording session fixtures have no versioned catalog. Give only these
// synthetic caches a stable private proof; freshness cases replace the provider
// and the authenticated public regression uses the real engine proof.
func seedRewriteRuleCacheProofForTest(ses *Session) {
	ses.ruleCacheVersion = 1
	ses.ruleCacheIdentity = rewriteRuleSessionIdentity(ses)
	ses.ruleCacheVersionProvider = func(context.Context, *Session) (any, error) { return 1, nil }
}

func TestPreparedRefreshDoesNotMaterializeOrAliasPolicy(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ctx := t.Context()
	ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
	ses.ruleCache = map[string]string{"db.t": "select * from db.t where role_keep = 1"}
	seedRewriteRuleCacheProofForTest(ses)
	ses.rewriteEnabled.Store(true)
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites", `{"rewrites":{"db.t":"select * from db.t where session_keep = 1"}}`))
	policy, err := captureRewritePolicy(ctx, ses)
	require.NoError(t, err)
	metadata, err := refreshRewritePolicy(ctx, ses, ses.ruleCacheVersionProvider, false)
	require.NoError(t, err)
	require.True(t, metadata.enabled)
	require.False(t, metadata.captured, "refresh metadata cannot masquerade as a captured plan policy")
	require.Nil(t, metadata.roleRules)
	require.Nil(t, metadata.sessionRules)
	require.Nil(t, metadata.sessionRemapDb)
	ses.ruleCacheMu.Lock()
	ses.ruleCache["db.t"] = "select * from db.t where role_keep = 2"
	ses.ruleCacheRevision++
	ses.ruleCacheMu.Unlock()
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites", `{"rewrites":{"db.t":"select * from db.t where session_keep = 2"}}`))
	require.Equal(t, "select * from db.t where role_keep = 1", policy.roleRules["db.t"])
	require.Equal(t, "select * from db.t where session_keep = 1", policy.sessionRules["db.t"])
	ses.rewriteEnabled.Store(false)
	require.NoError(t, refreshPreparedRewritePolicy(ctx, ses), "disabling optional rewrites still refreshes mandatory rules")
}

func TestRewriteRuleCacheFreshness(t *testing.T) {
	for _, materialize := range []bool{true, false} {
		t.Run(fmt.Sprintf("materialize_%v", materialize), func(t *testing.T) {
			for _, scenario := range []string{"stable", "unrelated catalog change", "changed rules", "unknown proof", "proof error", "proof and read error", "proof canceled", "proof deadline", "pre-read stamp", "account change", "role change", "secondary roles"} {
				t.Run(scenario, func(t *testing.T) {
					ctx := context.Background()
					ses := newSes(&privilege{}, gomock.NewController(t))
					ses.SetTenantInfo(&TenantInfo{TenantID: 7, UserID: 42, DefaultRoleID: 10})
					bh := &backgroundExecTest{}
					bh.init()
					bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId(nil)
					query := getSqlForRoleRulesOfRoleIDs([]int64{10})
					bh.sql2result[query] = newMrsForRewriteRules(nil)
					stub := gostub.StubFunc(&NewBackgroundExec, bh)
					defer stub.Reset()
					var proof any = 1
					var proofErr error
					var expectedErr error
					versionOf := func(context.Context, *Session) (any, error) { return proof, proofErr }
					capture := func() *rewritePolicySnapshot {
						policy, err := refreshRewritePolicy(ctx, ses, versionOf, materialize)
						require.NoError(t, err)
						return &policy
					}
					initial := capture()
					old := &PrepareStmt{Name: "old", rewritePolicyCaptured: true, rewritePolicyGeneration: initial.generation}
					ses.prepareStmts["old"] = old
					switch scenario {
					case "account change":
						ses.GetTenantInfo().TenantID = 8
					case "role change":
						ses.GetTenantInfo().DefaultRoleID = 11
						bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(11)] = newMrsForInheritedRoleIdOfRoleId(nil)
						bh.sql2result[getSqlForRoleRulesOfRoleIDs([]int64{11})] = newMrsForRewriteRules(nil)
					case "secondary roles":
						ses.GetTenantInfo().SetUseSecondaryRole(true)
						bh.sql2result[getSqlForRoleIDsOfUserForRuleCache(42)] = newMrsForRoleIdOfUserId(nil)
					case "unrelated catalog change":
						proof = 2
					case "changed rules":
						proof = 2
						bh.sql2result[query] = newMrsForRewriteRules([][]interface{}{{10, "db.t", "select * from db.t where tenant=1"}})
					case "unknown proof":
						proof = nil
						bh.sql2result[query] = newMrsForRewriteRules([][]interface{}{{10, "db.t", "select * from db.t where tenant=1"}})
					case "proof error":
						proofErr = errors.New("proof unavailable")
						bh.sql2result[query] = newMrsForRewriteRules([][]interface{}{{10, "db.t", "select * from db.t where tenant=1"}})
					case "proof and read error":
						proofErr = errors.New("proof unavailable")
						expectedErr = errors.New("rule read unavailable")
						bh.sql2err[query] = expectedErr
					case "proof canceled":
						proofErr, expectedErr = context.Canceled, context.Canceled
					case "proof deadline":
						proofErr, expectedErr = context.DeadlineExceeded, context.DeadlineExceeded
					case "pre-read stamp":
						proof = 2
						bh.beforeExec = func(sql string) {
							if sql == query {
								proof = 3
							}
						}
					}
					if expectedErr != nil {
						ses.ruleCacheVersionProvider = versionOf
						before := len(bh.executedSQLs)
						_, err := ses.GetPrepareStmt(ctx, "old")
						require.ErrorIs(t, err, expectedErr)
						require.True(t, old.rewritePolicyInvalidated.Load())
						if expectedErr == proofErr {
							require.Len(t, bh.executedSQLs, before, "cancellation must not start a fallback read")
						}
						// A failed refresh must leave session locks usable, including cleanup.
						_, err = ses.getPrepareStmtAllowInvalidated(ctx, "old")
						require.NoError(t, err)
						return
					}
					refreshed := capture()
					changed := scenario == "changed rules" || scenario == "unknown proof" || scenario == "proof error"
					require.Equal(t, changed, old.rewritePolicyInvalidated.Load())
					if changed {
						require.NotEmpty(t, ses.ruleCache)
						if materialize {
							require.NotEmpty(t, refreshed.roleRules)
						} else {
							require.Nil(t, refreshed.roleRules)
							require.False(t, refreshed.captured)
						}
						require.Greater(t, refreshed.generation, initial.generation)
					} else {
						require.Equal(t, initial.generation, refreshed.generation)
					}
					if scenario == "pre-read stamp" {
						require.Equal(t, 2, ses.ruleCacheVersion)
					}
					if scenario == "proof error" {
						require.Nil(t, ses.ruleCacheVersion, "a failed proof must not stamp the new rules")
					}
					capture()
					loads := 0
					for _, sql := range bh.executedSQLs {
						if strings.Contains(sql, "select role_id, rule_name") {
							loads++
						}
					}
					expected := 2
					if scenario == "stable" {
						expected = 1
					}
					if scenario == "unknown proof" || scenario == "pre-read stamp" || scenario == "proof error" {
						expected = 3
					}
					require.Equal(t, expected, loads)
				})
			}
		})
	}
}

func TestLoadRuleCacheTransactionFailures(t *testing.T) {
	for _, failing := range []string{"begin;", getSqlForInheritedRoleIDsForRuleCache(10), getSqlForRoleRulesOfRoleIDs([]int64{10}), "commit;"} {
		t.Run(failing, func(t *testing.T) {
			ses := newSes(&privilege{}, gomock.NewController(t))
			ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[getSqlForInheritedRoleIDsForRuleCache(10)] = newMrsForInheritedRoleIdOfRoleId(nil)
			bh.sql2result[getSqlForRoleRulesOfRoleIDs([]int64{10})] = newMrsForRewriteRules(nil)
			failure := errors.New("catalog transaction failed")
			bh.sql2err[failing] = failure
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer stub.Reset()
			_, err := loadRuleCache(context.Background(), ses)
			require.ErrorIs(t, err, failure)
			require.Equal(t, "begin;", bh.executedSQLs[0])
			if failing != "begin;" {
				require.Equal(t, "rollback;", bh.executedSQLs[len(bh.executedSQLs)-1])
			}
		})
	}
}
