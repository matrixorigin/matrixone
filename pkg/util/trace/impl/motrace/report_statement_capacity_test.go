// Copyright 2026 Matrix Origin
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

package motrace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type capacityExecPlan struct {
	dummySerializableExecPlan
	payload         []byte
	marshals, frees int
}

func (p *capacityExecPlan) Marshal(context.Context) []byte { p.marshals++; return p.payload }
func (p *capacityExecPlan) Free()                          { p.frees++ }

func statementCapacityValues(rowValues []string) map[string]string {
	result := make(map[string]string)
	for i, c := range SingleStatementTable.Columns {
		result[c.Name] = rowValues[i]
	}
	return result
}

func TestStatementInfoCapacityAndPlanOwnership(t *testing.T) {
	provider := GetTracerProvider()
	old := provider.disableSqlWriter
	t.Cleanup(func() { provider.disableSqlWriter = old })
	for _, disable := range []bool{false, true} {
		provider.disableSqlWriter = disable
		originalError := errors.New(strings.Repeat("e", db_holder.StatementInfoTextLimit) + "tail")
		plan := &capacityExecPlan{payload: []byte(`{"plan":"` + strings.Repeat("p", db_holder.StatementInfoTextLimit) + `"}`)}
		s := &StatementInfo{Statement: []byte(strings.Repeat("🙂", 20000)), Error: originalError, ExecPlan: plan, Status: StatementStatusFailed, end: true}
		s.statsArray.Init()
		key := s.Key(time.Second)
		row := SingleStatementTable.GetRow(context.Background())
		s.FillRow(context.Background(), row)
		first := statementCapacityValues(row.ToStrings())
		for _, name := range []string{"statement", "error"} {
			require.LessOrEqual(t, len(first[name]), db_holder.StatementInfoTextLimit)
			require.True(t, utf8.ValidString(first[name]))
			require.True(t, strings.HasSuffix(first[name], db_holder.StatementInfoTruncationMarker))
		}
		var summary map[string]any
		require.NoError(t, json.Unmarshal([]byte(first["exec_plan"]), &summary))
		require.Equal(t, float64(len(plan.payload)), summary["original_bytes"])
		s.FillRow(context.Background(), row)
		require.Equal(t, first, statementCapacityValues(row.ToStrings()))
		require.Equal(t, 1, plan.marshals)
		require.Same(t, originalError, s.Error)
		require.Equal(t, key, s.Key(time.Second))
		other := &StatementInfo{Error: errors.New(strings.Repeat("e", db_holder.StatementInfoTextLimit) + "different"), Status: StatementStatusFailed}
		require.NotEqual(t, s.Key(time.Second), other.Key(time.Second))
		s.Free()
		require.Equal(t, 1, plan.frees)
		require.Nil(t, s.jsonByte)
		row.Free()
	}
}

func TestStatementInfoAggregationCapacity(t *testing.T) {
	provider := GetTracerProvider()
	old := provider.enableStmtMerge
	t.Cleanup(func() { provider.enableStmtMerge = old })
	ctx := context.WithValue(context.Background(), DurationKey, time.Second)
	for _, merge := range []bool{false, true} {
		provider.enableStmtMerge = merge
		for _, largeFirst := range []bool{false, true} {
			text := "select 1"
			if largeFirst {
				text = strings.Repeat("你", 30000)
			}
			source := &StatementInfo{Statement: []byte(text), Status: StatementStatusSuccess, Duration: time.Millisecond}
			source.statsArray.Init()
			aggregate := StatementInfoNew(source, ctx).(*StatementInfo)
			require.LessOrEqual(t, aggregate.StmtBuilder.Len(), db_holder.StatementInfoTextLimit)
			for range 12 {
				next := &StatementInfo{Statement: []byte(strings.Repeat("你", 2000)), Duration: time.Millisecond, ResultCount: 1}
				next.statsArray.Init()
				StatementInfoUpdate(ctx, aggregate, next)
			}
			require.Equal(t, int64(13), aggregate.AggrCount)
			require.Equal(t, int64(12), aggregate.ResultCount)
			require.Equal(t, 13*time.Millisecond, aggregate.Duration)
			require.LessOrEqual(t, aggregate.StmtBuilder.Len(), db_holder.StatementInfoTextLimit)
			row := SingleStatementTable.GetRow(ctx)
			aggregate.FillRow(ctx, row)
			first := statementCapacityValues(row.ToStrings())
			require.True(t, strings.HasPrefix(first["statement"], "/* 13 queries */ \n"))
			require.LessOrEqual(t, len(first["statement"]), db_holder.StatementInfoTextLimit)
			require.True(t, utf8.ValidString(first["statement"]))
			if merge || largeFirst {
				require.True(t, strings.HasSuffix(first["statement"], db_holder.StatementInfoTruncationMarker))
			} else {
				require.Equal(t, "/* 13 queries */ \nselect 1", first["statement"])
			}
			aggregate.FillRow(ctx, row)
			require.Equal(t, first, statementCapacityValues(row.ToStrings()))
			aggregate.end = true
			aggregate.Free()
			require.Zero(t, aggregate.StmtBuilder.Len())
			row.Free()
		}
	}
	provider.enableStmtMerge = false
	text := strings.Repeat("a", db_holder.StatementInfoTextLimit-len("/* 9 queries */ \n"))
	s := &StatementInfo{Statement: []byte(text), AggrCount: 9}
	s.statsArray.Init()
	row := SingleStatementTable.GetRow(ctx)
	defer row.Free()
	s.FillRow(ctx, row)
	require.Equal(t, "/* 9 queries */ \n"+text, statementCapacityValues(row.ToStrings())["statement"])
	s.AggrCount = 10
	s.FillRow(ctx, row)
	got := statementCapacityValues(row.ToStrings())["statement"]
	require.Len(t, got, db_holder.StatementInfoTextLimit)
	require.True(t, strings.HasSuffix(got, db_holder.StatementInfoTruncationMarker))
}

func TestStatementInfoCachedMalformedPlanCapacity(t *testing.T) {
	s := &StatementInfo{jsonByte: []byte(strings.Repeat("x", 65536))}
	got := s.ExecPlan2Json(context.Background())
	var summary map[string]any
	require.NoError(t, json.Unmarshal(got, &summary))
	require.Equal(t, float64(65536), summary["original_bytes"])
	require.Equal(t, got, s.ExecPlan2Json(context.Background()))
}

func BenchmarkStatementInfoReporting(b *testing.B) {
	provider := GetTracerProvider()
	oldWriter, oldMerge := provider.disableSqlWriter, provider.enableStmtMerge
	defer func() { provider.disableSqlWriter = oldWriter; provider.enableStmtMerge = oldMerge }()
	ctx := context.Background()
	for _, disable := range []bool{false, true} {
		provider.disableSqlWriter = disable
		for _, kind := range []string{"success", "error", "plan", "aggregate", "representative"} {
			b.Run(fmt.Sprintf("csv=%t/%s", disable, kind), func(b *testing.B) {
				provider.enableStmtMerge = kind == "aggregate"
				s := &StatementInfo{Statement: []byte("select 1"), Status: StatementStatusSuccess}
				s.statsArray.Init()
				switch kind {
				case "error":
					s.Error = errors.New("normal diagnostic")
				case "plan":
					s.jsonByte = []byte(`{"plan":1}`)
				case "aggregate", "representative":
					s.AggrCount = 10
				}
				row := SingleStatementTable.GetRow(ctx)
				defer row.Free()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if kind == "aggregate" {
						s.StmtBuilder.Reset()
						s.StmtBuilder.WriteString("select 1;\nselect 2")
					}
					s.FillRow(ctx, row)
				}
			})
		}
		b.Run(fmt.Sprintf("csv=%t/content", disable), func(b *testing.B) {
			s := &StatementInfo{Statement: []byte("select 'normal'"), Status: StatementStatusSuccess}
			s.statsArray.Init()
			buffer := NewContentBuffer()
			buffer.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buffer.Add(s)
				buffer.formatter.Flush()
				buffer.buf.Reset()
			}
		})
	}
}

func BenchmarkStatementInfoAggregation(b *testing.B) {
	provider := GetTracerProvider()
	old := provider.enableStmtMerge
	defer func() { provider.enableStmtMerge = old }()
	ctx := context.WithValue(context.Background(), DurationKey, time.Second)
	for _, tc := range []struct {
		name          string
		merge         bool
		first, update string
		count         int
	}{
		{"normal/representative", false, "select 1", "select 2", 8},
		{"normal/merged", true, "select 1", "select 2", 8},
		{"saturating", true, "select 1", strings.Repeat("a", 8192), 80},
		{"after-saturation", true, strings.Repeat("a", 65535), strings.Repeat("b", 8192), 64},
	} {
		b.Run(tc.name, func(b *testing.B) {
			provider.enableStmtMerge = tc.merge
			newStatement := func(text string) *StatementInfo {
				s := NewStatementInfo()
				s.Statement = append(s.Statement, text...)
				s.Status = StatementStatusSuccess
				s.Duration = time.Millisecond
				s.end = true
				s.statsArray.Init()
				return s
			}
			row := SingleStatementTable.GetRow(ctx)
			defer row.Free()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				first := newStatement(tc.first)
				group := StatementInfoNew(first, ctx).(*StatementInfo)
				first.Free()
				for j := 1; j < tc.count; j++ {
					next := newStatement(tc.update)
					StatementInfoUpdate(ctx, group, next)
					next.Free()
				}
				group.FillRow(ctx, row)
				group.Free()
			}
		})
	}
}
