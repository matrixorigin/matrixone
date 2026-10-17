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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
)

func BenchmarkAuthorizationStagingSingleStatement(b *testing.B) {
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(b.Context(), "", &testMysqlWriter{}, nil)
	b.Cleanup(ses.Close)
	for _, query := range []string{
		"select id from app.t order by id;",
		"select ';' as x",
		"select id from app.t /* ; */ order by id;",
	} {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _, staged, err := prepareAuthorizationStagedExecution(b.Context(), ses, ses.GetMySQLParser(), query)
				if err != nil || staged {
					b.Fatalf("single statement: staged=%v err=%v", staged, err)
				}
			}
		})
	}
}

func BenchmarkAuthorizationEpochScope(b *testing.B) {
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(b.Context(), "", &testMysqlWriter{}, nil)
	b.Cleanup(ses.Close)
	parent, cancel := context.WithCancel(b.Context())
	defer cancel()
	exec := &ExecCtx{}
	b.ReportAllocs()
	for b.Loop() {
		_, epoch := beginAuthorizationEpoch(parent, ses, exec)
		epoch.end()
	}
}

func BenchmarkPreparedRewritePolicyRefresh(b *testing.B) {
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(b.Context(), "", &testMysqlWriter{}, nil)
	b.Cleanup(ses.Close)
	ses.SetTenantInfo(&TenantInfo{DefaultRoleID: 10})
	ses.ruleCache = map[string]string{"db.t": "select * from db.t where id = 1"}
	seedRewriteRuleCacheProofForTest(ses)
	for _, materialize := range []bool{true, false} {
		name := "refresh"
		if materialize {
			name = "capture"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := refreshRewritePolicy(b.Context(), ses, ses.ruleCacheVersionProvider, materialize)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
