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
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// SQL-recording session fixtures have no versioned catalog. Give only these
// synthetic caches a stable private proof; freshness cases replace the provider
// and the authenticated public regression uses the real engine proof.
func seedRewriteRuleCacheProofForTest(ses *Session) {
	ses.ruleCacheVersion = 1
	ses.ruleCacheIdentity = rewriteRuleSessionIdentity(ses)
	ses.ruleCacheVersionProvider = func(context.Context, *Session) (any, error) { return 1, nil }
}

func TestRewriteRuleCacheFreshness(t *testing.T) {
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
				policy, err := captureRewritePolicyWithVersion(ctx, ses, versionOf)
				require.NoError(t, err)
				return policy
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
				require.NotEmpty(t, refreshed.roleRules)
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
