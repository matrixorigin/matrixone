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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	subscriptionTablesFunctionName  = "mo_subscription_tables"
	subscriptionColumnsFunctionName = "mo_subscription_columns"
)

var (
	subscriptionTablesColDefs  []*planpb.ColDef
	subscriptionColumnsColDefs []*planpb.ColDef
)

func init() {
	subscriptionTablesColDefs = subscriptionMetadataColDefs(
		[]string{
			"account_id",
			"rel_id",
			"relname",
			"reldatabase",
			"reldatabase_id",
			"relkind",
			"rel_createsql",
			"created_time",
			"partitioned",
			"rel_comment",
			"extra_info",
			"rel_logical_id",
			"owner",
		},
		[]types.Type{
			catalog.MoTablesTypes[catalog.MO_TABLES_ACCOUNT_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_NAME_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_RELDATABASE_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_RELDATABASE_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_RELKIND_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_CREATESQL_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_CREATED_TIME_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_PARTITIONED_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_COMMENT_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_EXTRA_INFO_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_LOGICAL_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_OWNER_IDX],
		},
	)

	subscriptionColumnsColDefs = subscriptionMetadataColDefs(
		[]string{
			"account_id",
			"att_database_id",
			"att_database",
			"att_relname_id",
			"att_relname",
			"attname",
			"atttyp",
			"attnum",
			"attnotnull",
			"att_default",
			"att_constraint_type",
			"att_is_auto_increment",
			"att_comment",
			"att_is_hidden",
			"attr_enum",
			"attr_has_generated",
			"attr_generated",
			"key_priority",
			"rel_id",
			"relkind",
			"rel_createsql",
			"partitioned",
			"extra_info",
			"rel_logical_id",
			"table_owner",
		},
		[]types.Type{
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ACCOUNT_ID_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DATABASE_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_RELNAME_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNAME_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTTYP_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNUM_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNOTNULL_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DEFAULT_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_COMMENT_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_ENUM_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX],
			catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_GENERATED_IDX],
			types.New(types.T_int64, 0, 0),
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_RELKIND_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_REL_CREATESQL_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_PARTITIONED_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_EXTRA_INFO_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_LOGICAL_ID_IDX],
			catalog.MoTablesTypes[catalog.MO_TABLES_OWNER_IDX],
		},
	)
}

func subscriptionMetadataColDefs(names []string, columnTypes []types.Type) []*planpb.ColDef {
	cols := make([]*planpb.ColDef, len(names))
	for i := range names {
		cols[i] = &planpb.ColDef{Name: names[i], Typ: makePlan2Type(&columnTypes[i])}
	}
	return cols
}

func requireSubscriptionMetadataProtocol(ctx context.Context, proc *process.Process) error {
	if proc == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(
			ctx,
			"subscription information-schema metadata requires all CNs to support protocol version 46",
		)
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < defines.MORPCVersion46 {
		return moerr.NewNotSupported(
			ctx,
			"subscription information-schema metadata requires all CNs to support protocol version 46",
		)
	}
	return nil
}

func requireSubscriptionMetadataView(
	ctx context.Context,
	bindCtx *BindContext,
	persistedViewTarget string,
	functionName string,
) error {
	for current := bindCtx; current != nil; current = current.parent {
		if len(current.viewChain) > 0 &&
			subscriptionMetadataOwnerAllowed(current.viewChain[len(current.viewChain)-1], functionName) {
			return nil
		}
	}
	if persistedViewTarget != "" &&
		subscriptionMetadataOwnerAllowed(persistedViewTarget, functionName) {
		return nil
	}
	return moerr.NewNotSupportedf(ctx,
		"%s is private to information_schema metadata views", functionName)
}

func subscriptionMetadataOwnerAllowed(ownerKey string, functionName string) bool {
	database, view := objectkey.Decode(ownerKey)
	if !strings.EqualFold(database, "information_schema") {
		return false
	}
	if functionName == subscriptionTablesFunctionName {
		return strings.EqualFold(view, "tables")
	}
	return strings.EqualFold(view, "columns")
}

func (builder *QueryBuilder) buildSubscriptionMetadata(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
	functionName string,
	cols []*planpb.ColDef,
) (int32, error) {
	if len(tbl.Func.Exprs) != 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(),
			functionName+" function has invalid input args length", len(tbl.Func.Exprs))
	}
	if err := requireSubscriptionMetadataProtocol(builder.GetContext(), builder.compCtx.GetProcess()); err != nil {
		return 0, err
	}
	if err := requireSubscriptionMetadataView(
		builder.GetContext(), ctx, builder.persistedViewTarget, functionName); err != nil {
		return 0, err
	}

	node := &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		Stats:    &planpb.Stats{},
		TableDef: &planpb.TableDef{
			TableType: "func_table",
			TblFunc: &planpb.TableFunction{
				Name: functionName,
			},
			Cols: DeepCopyColDefList(cols),
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) buildSubscriptionTables(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	return builder.buildSubscriptionMetadata(
		tbl, ctx, exprs, children, subscriptionTablesFunctionName, subscriptionTablesColDefs)
}

func (builder *QueryBuilder) buildSubscriptionColumns(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	return builder.buildSubscriptionMetadata(
		tbl, ctx, exprs, children, subscriptionColumnsFunctionName, subscriptionColumnsColDefs)
}
