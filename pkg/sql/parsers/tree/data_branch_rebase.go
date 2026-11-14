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

package tree

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[DataBranchRebase](func() *DataBranchRebase { return &DataBranchRebase{} },
		func(s *DataBranchRebase) { *s = DataBranchRebase{} }, reuse.DefaultOptions[DataBranchRebase]())
}

type RebaseDatabaseEndpoint struct {
	Database Identifier
	AtTsExpr *AtTimeStamp
}

type RebaseSourceExpectation struct {
	DatabaseID uint64
	Epoch      uint64
}

func (e *RebaseDatabaseEndpoint) Format(ctx *FmtCtx) {
	ctx.WriteIdentifier(e.Database)
	if e.AtTsExpr != nil {
		e.AtTsExpr.Format(ctx)
	}
}

// DataBranchRebase creates a new database. It does not move a live branch name
// or publish the driver's history/generation registry.
type DataBranchRebase struct {
	statementImpl
	Destination               Identifier
	Feature, Upstream         RebaseDatabaseEndpoint
	FeatureBase, UpstreamBase RebaseDatabaseEndpoint
	Tables                    IdentifierList
	ConflictOpt               *ConflictOpt
	SourceExpectation         *RebaseSourceExpectation
}

func NewDataBranchRebase() *DataBranchRebase         { return reuse.Alloc[DataBranchRebase](nil) }
func (s *DataBranchRebase) Free()                    { reuse.Free[DataBranchRebase](s, nil) }
func (s *DataBranchRebase) TypeName() string         { return "DataBranchRebase" }
func (s *DataBranchRebase) GetStatementType() string { return "Data Branch Rebase Database" }
func (s *DataBranchRebase) GetQueryType() string     { return QueryTypeOth }
func (s *DataBranchRebase) StmtKind() StmtKind       { return compositeResRowType }
func (s *DataBranchRebase) String() string           { return s.GetStatementType() }

func (s *DataBranchRebase) Format(ctx *FmtCtx) {
	formatDataBranchStatement(ctx, func() {
		ctx.WriteString("data branch rebase database ")
		ctx.WriteIdentifier(s.Destination)
		ctx.WriteString(" from ")
		s.Feature.Format(ctx)
		ctx.WriteString(" against ")
		s.Upstream.Format(ctx)
		ctx.WriteString(" between ")
		s.FeatureBase.Format(ctx)
		ctx.WriteString(" and ")
		s.UpstreamBase.Format(ctx)
		if len(s.Tables) != 0 {
			ctx.WriteString(" tables (")
			s.Tables.Format(ctx)
			ctx.WriteByte(')')
		}
		if s.SourceExpectation != nil {
			ctx.WriteString(" for update expect (")
			ctx.WriteString(strconv.FormatUint(s.SourceExpectation.DatabaseID, 10))
			ctx.WriteString(", ")
			ctx.WriteString(strconv.FormatUint(s.SourceExpectation.Epoch, 10))
			ctx.WriteByte(')')
		}
		if s.ConflictOpt != nil {
			s.ConflictOpt.Format(ctx)
		}
	})
}
