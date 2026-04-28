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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRewriteHiddenIndexDupEntrySingleColumn(t *testing.T) {
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					{
						TableDef: &plan.TableDef{
							Name: "decimal15",
							Indexes: []*plan.IndexDef{
								{Unique: true, IndexName: "a_index", Parts: []string{"a"}},
							},
						},
					},
				},
			},
		},
	}

	src := moerr.NewDuplicateEntryNoCtx("271.21212", catalog.IndexTableIndexColName)
	rewritten := RewriteHiddenIndexDupEntry(p, src)
	require.Equal(t, "Duplicate entry '271.21212' for key 'a'", rewritten.Error())
}

func TestRewriteHiddenIndexDupEntryCompositeIndex(t *testing.T) {
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					{
						TableDef: &plan.TableDef{
							Name: "t1",
							Indexes: []*plan.IndexDef{
								{Unique: true, IndexName: "tempKey", Parts: []string{"col1", "col2"}},
							},
						},
					},
				},
			},
		},
	}

	src := moerr.NewDuplicateEntryNoCtx("(1,2)", catalog.IndexTableIndexColName)
	rewritten := RewriteHiddenIndexDupEntry(p, src)
	require.Equal(t, "Duplicate entry '(1,2)' for key 'tempKey'", rewritten.Error())
}

func TestRewriteHiddenIndexDupEntryUnchanged(t *testing.T) {
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					{
						TableDef: &plan.TableDef{
							Name: "t1",
							Indexes: []*plan.IndexDef{
								{Unique: true, IndexName: "a_index", Parts: []string{"a"}},
							},
						},
					},
				},
			},
		},
	}

	src := moerr.NewDuplicateEntryNoCtx("v", "pk")
	rewritten := RewriteHiddenIndexDupEntry(p, src)
	require.Equal(t, src.Error(), rewritten.Error())
}

func TestRewriteHiddenIndexDupEntryMultipleUniqueIndexesUnchanged(t *testing.T) {
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					{
						TableDef: &plan.TableDef{
							Name: "t1",
							Indexes: []*plan.IndexDef{
								{Unique: true, IndexName: "a_index", Parts: []string{"a"}},
								{Unique: true, IndexName: "b_index", Parts: []string{"b"}},
							},
						},
					},
				},
			},
		},
	}

	src := moerr.NewDuplicateEntryNoCtx("1", catalog.IndexTableIndexColName)
	rewritten := RewriteHiddenIndexDupEntry(p, src)
	require.Equal(t, src.Error(), rewritten.Error(),
		"with multiple unique indexes we cannot tell which one was hit, so keep the original error")
}
