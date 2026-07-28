// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestDropCcprSubscriptionLifecycle(t *testing.T) {
	// Test creation with NewDropCcprSubscription
	stmt := NewDropCcprSubscription(false, "test_sub")
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Drop Ccpr Subscription", stmt.GetStatementType())
	require.Equal(t, QueryTypeDCL, stmt.GetQueryType())
	require.Equal(t, "tree.DropCcprSubscription", stmt.TypeName())

	// Verify fields set by constructor
	require.Equal(t, "test_sub", stmt.TaskID)
	require.False(t, stmt.IfExists)

	// Test Format without IfExists
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "drop ccpr subscription 'test_sub'", ctx.String())

	stmt.Free()

	// Test creation with IfExists = true
	stmt2 := NewDropCcprSubscription(true, "another_sub")
	require.NotNil(t, stmt2)
	require.Equal(t, "another_sub", stmt2.TaskID)
	require.True(t, stmt2.IfExists)

	// Test Format with IfExists
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "drop ccpr subscription if exists 'another_sub'", ctx2.String())

	// Test reset
	stmt2.reset()
	require.Equal(t, "", stmt2.TaskID)
	require.False(t, stmt2.IfExists)

	stmt2.Free()
}

func TestDropCcprSubscriptionEdgeCases(t *testing.T) {
	// Test with empty name
	stmt := NewDropCcprSubscription(false, "")
	require.NotNil(t, stmt)
	require.Equal(t, "", stmt.TaskID)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "drop ccpr subscription ''", ctx.String())

	stmt.Free()

	// Test with special characters in name
	stmt2 := NewDropCcprSubscription(true, "sub_with_underscore")
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "drop ccpr subscription if exists 'sub_with_underscore'", ctx2.String())

	stmt2.Free()
}

func TestResumeCcprSubscriptionLifecycle(t *testing.T) {
	// Test creation with NewResumeCcprSubscription
	stmt := NewResumeCcprSubscription("test_sub")
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Resume Ccpr Subscription", stmt.GetStatementType())
	require.Equal(t, QueryTypeDCL, stmt.GetQueryType())
	require.Equal(t, "tree.ResumeCcprSubscription", stmt.TypeName())

	// Verify field set by constructor
	require.Equal(t, "test_sub", stmt.TaskID)

	// Test Format
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "resume ccpr subscription 'test_sub'", ctx.String())

	// Test reset
	stmt.reset()
	require.Equal(t, "", stmt.TaskID)

	stmt.Free()
}

func TestResumeCcprSubscriptionEdgeCases(t *testing.T) {
	// Test with empty name
	stmt := NewResumeCcprSubscription("")
	require.NotNil(t, stmt)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "resume ccpr subscription ''", ctx.String())

	stmt.Free()

	// Test with underscore name
	stmt2 := NewResumeCcprSubscription("my_subscription")
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "resume ccpr subscription 'my_subscription'", ctx2.String())

	stmt2.Free()
}

func TestPauseCcprSubscriptionLifecycle(t *testing.T) {
	// Test creation with NewPauseCcprSubscription
	stmt := NewPauseCcprSubscription("test_sub")
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Pause Ccpr Subscription", stmt.GetStatementType())
	require.Equal(t, QueryTypeDCL, stmt.GetQueryType())
	require.Equal(t, "tree.PauseCcprSubscription", stmt.TypeName())

	// Verify field set by constructor
	require.Equal(t, "test_sub", stmt.TaskID)

	// Test Format
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "pause ccpr subscription 'test_sub'", ctx.String())

	// Test reset
	stmt.reset()
	require.Equal(t, "", stmt.TaskID)

	stmt.Free()
}

func TestPauseCcprSubscriptionEdgeCases(t *testing.T) {
	// Test with empty name
	stmt := NewPauseCcprSubscription("")
	require.NotNil(t, stmt)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "pause ccpr subscription ''", ctx.String())

	stmt.Free()

	// Test with underscore name
	stmt2 := NewPauseCcprSubscription("paused_sub")
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "pause ccpr subscription 'paused_sub'", ctx2.String())

	stmt2.Free()
}

func TestShowPublicationCoverageLifecycle(t *testing.T) {
	// Test direct creation
	stmt := &ShowPublicationCoverage{
		Name: "test_pub",
	}
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "Show Publication Coverage", stmt.GetStatementType())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	// Verify field
	require.Equal(t, "test_pub", stmt.Name)

	// Test Format
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "show publication coverage test_pub", ctx.String())
}

func TestShowPublicationCoverageEdgeCases(t *testing.T) {
	// Test with empty name
	stmt := &ShowPublicationCoverage{
		Name: "",
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "show publication coverage ", ctx.String())

	// Test with underscore name
	stmt2 := &ShowPublicationCoverage{
		Name: "my_publication",
	}
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "show publication coverage my_publication", ctx2.String())
}

func TestCheckSnapshotFlushedLifecycle(t *testing.T) {
	// Test direct creation
	stmt := &CheckSnapshotFlushed{
		Name: Identifier("test_snapshot"),
	}
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "Check Snapshot Flushed", stmt.GetStatementType())
	require.Equal(t, QueryTypeDQL, stmt.GetQueryType())

	// Verify field
	require.Equal(t, Identifier("test_snapshot"), stmt.Name)

	// Test Format
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "checkSnapshotFlushed test_snapshot", ctx.String())
}

func TestCheckSnapshotFlushedEdgeCases(t *testing.T) {
	// Test with empty name
	stmt := &CheckSnapshotFlushed{
		Name: Identifier(""),
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "checkSnapshotFlushed ", ctx.String())

	// Test with underscore name
	stmt2 := &CheckSnapshotFlushed{
		Name: Identifier("my_snapshot_2024"),
	}
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "checkSnapshotFlushed my_snapshot_2024", ctx2.String())
}

func TestCheckSnapshotFlushedFormat_WithAccountAndPublication(t *testing.T) {
	// Test with AccountName only
	stmt := &CheckSnapshotFlushed{
		Name:        Identifier("snap1"),
		AccountName: Identifier("sys"),
	}
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "checkSnapshotFlushed snap1 account sys", ctx.String())

	// Test with PublicationName only
	stmt2 := &CheckSnapshotFlushed{
		Name:            Identifier("snap2"),
		PublicationName: Identifier("mypub"),
	}
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "checkSnapshotFlushed snap2 publication mypub", ctx2.String())

	// Test with both AccountName and PublicationName
	stmt3 := &CheckSnapshotFlushed{
		Name:            Identifier("snap3"),
		AccountName:     Identifier("acc1"),
		PublicationName: Identifier("pub1"),
	}
	ctx3 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt3.Format(ctx3)
	require.Equal(t, "checkSnapshotFlushed snap3 account acc1 publication pub1", ctx3.String())
}

func TestDropSnapShotFormat_GoodPath(t *testing.T) {
	// Test basic drop snapshot
	stmt := NewDropSnapShot(false, Identifier("snap1"), Identifier(""), Identifier(""))
	require.NotNil(t, stmt)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "drop snapshot snap1", ctx.String())
	stmt.Free()

	// Test with IfExists
	stmt2 := NewDropSnapShot(true, Identifier("snap2"), Identifier(""), Identifier(""))
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "drop snapshot if exists snap2", ctx2.String())
	stmt2.Free()

	// Test with AccountName and PubName (cross-cluster drop)
	stmt3 := NewDropSnapShot(false, Identifier("snap3"), Identifier("sys"), Identifier("mypub"))
	ctx3 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt3.Format(ctx3)
	require.Equal(t, "drop snapshot snap3 from sys publication mypub", ctx3.String())
	stmt3.Free()

	// Test with IfExists, AccountName and PubName
	stmt4 := NewDropSnapShot(true, Identifier("snap4"), Identifier("account1"), Identifier("pub1"))
	ctx4 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt4.Format(ctx4)
	require.Equal(t, "drop snapshot if exists snap4 from account1 publication pub1", ctx4.String())
	stmt4.Free()
}
