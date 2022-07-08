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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildBeginTransaction(stmt *tree.BeginTransaction, ctx CompilerContext) (*Plan, error) {
	beginTransation := &plan.TransationBegin{}
	switch stmt.Modes.RwMode {
	case tree.READ_WRITE_MODE_NONE:
		beginTransation.Mode = plan.TransationBegin_NONE
	case tree.READ_WRITE_MODE_READ_ONLY:
		beginTransation.Mode = plan.TransationBegin_READ_ONLY
	case tree.READ_WRITE_MODE_READ_WRITE:
		beginTransation.Mode = plan.TransationBegin_READ_WRITE
	}

	return &Plan{
		Plan: &plan.Plan_Tcl{
			Tcl: &plan.TransationControl{
				TclType: plan.TransationControl_BEGIN,
				Action: &plan.TransationControl_Begin{
					Begin: beginTransation,
				},
			},
		},
	}, nil
}

func buildCommitTransaction(stmt *tree.CommitTransaction, ctx CompilerContext) (*Plan, error) {
	commitTransation := &plan.TransationCommit{}
	switch stmt.Type {
	case tree.COMPLETION_TYPE_CHAIN:
		commitTransation.CompletionType = plan.TransationCompletionType_CHAIN
	case tree.COMPLETION_TYPE_RELEASE:
		commitTransation.CompletionType = plan.TransationCompletionType_RELEASE
	case tree.COMPLETION_TYPE_NO_CHAIN:
		commitTransation.CompletionType = plan.TransationCompletionType_NO_CHAIN
	}
	return &Plan{
		Plan: &plan.Plan_Tcl{
			Tcl: &plan.TransationControl{
				TclType: plan.TransationControl_COMMIT,
				Action: &plan.TransationControl_Commit{
					Commit: commitTransation,
				},
			},
		},
	}, nil
}

func buildRollbackTransaction(stmt *tree.RollbackTransaction, ctx CompilerContext) (*Plan, error) {
	rollbackTransation := &plan.TransationRollback{}
	switch stmt.Type {
	case tree.COMPLETION_TYPE_CHAIN:
		rollbackTransation.CompletionType = plan.TransationCompletionType_CHAIN
	case tree.COMPLETION_TYPE_RELEASE:
		rollbackTransation.CompletionType = plan.TransationCompletionType_RELEASE
	case tree.COMPLETION_TYPE_NO_CHAIN:
		rollbackTransation.CompletionType = plan.TransationCompletionType_NO_CHAIN
	}
	return &Plan{
		Plan: &plan.Plan_Tcl{
			Tcl: &plan.TransationControl{
				TclType: plan.TransationControl_ROLLBACK,
				Action: &plan.TransationControl_Rollback{
					Rollback: rollbackTransation,
				},
			},
		},
	}, nil
}
