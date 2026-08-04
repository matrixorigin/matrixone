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

package compile

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type viewMetadataResolver interface {
	ResolveSnapshot(context.Context, string) (*plan.Snapshot, error)
	ResolveUdf(context.Context, string, []*plan.Expr) (*function.Udf, error)
}

type viewMetadataSnapshotNotFoundError struct {
	name string
}

func (e *viewMetadataSnapshotNotFoundError) Error() string {
	return fmt.Sprintf("snapshot %s does not exist", e.name)
}

type viewMetadataRefreshResolver struct {
	compile         *Compile
	accountID       uint32
	defaultDatabase string
	subscriptions   currentViewSubscriptionResolver
	dependencies    []plan2.ViewDependency
	sourceAccountID uint32
	sourceDatabase  string
	sourceTable     string
}

func (r viewMetadataRefreshResolver) relationAccountID(
	database string,
	table string,
) (uint32, bool) {
	for _, dependency := range r.dependencies {
		if dependency.Subscription ||
			!dependency.AccountIDSet ||
			dependency.DatabaseName != database || dependency.TableName != table {
			continue
		}
		return dependency.AccountID, true
	}
	if strings.EqualFold(r.sourceDatabase, database) && strings.EqualFold(r.sourceTable, table) {
		return r.sourceAccountID, true
	}
	return 0, false
}

type viewMetadataUDFNotFoundError struct {
	cause error
}

func (e *viewMetadataUDFNotFoundError) Error() string { return e.cause.Error() }
func (e *viewMetadataUDFNotFoundError) Unwrap() error { return e.cause }

func (r viewMetadataRefreshResolver) GetSubscriptionMeta(
	database string,
	snapshot *plan.Snapshot,
) (*plan.SubscriptionMeta, error) {
	return r.subscriptions.GetSubscriptionMeta(database, snapshot)
}

func snapshotTenantID(level string, objectID uint64, currentAccountID uint32) uint32 {
	switch level {
	case tree.RESTORELEVELCLUSTER.String():
		return catalog.System_Account
	case tree.RESTORELEVELACCOUNT.String():
		return uint32(objectID)
	default:
		return currentAccountID
	}
}

func (r viewMetadataRefreshResolver) ResolveSnapshot(
	ctx context.Context,
	name string,
) (*plan.Snapshot, error) {
	sql := fmt.Sprintf(
		"select sname, ts, level, account_name, obj_id from %s.%s "+
			"where sname = %s and coalesce(kind, '') != 'branch' order by snapshot_id",
		catalog.MO_CATALOG,
		catalog.MO_SNAPSHOTS,
		sqlquote.String(name),
	)
	result, err := r.compile.runSqlWithResultAndOptions(
		sql,
		int32(r.accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		result.Close()
		return nil, err
	}
	defer result.Close()

	var snapshots []*plan.Snapshot
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		names := executor.GetStringRows(cols[0])
		timestamps := executor.GetFixedRows[int64](cols[1])
		levels := executor.GetStringRows(cols[2])
		accountNames := executor.GetStringRows(cols[3])
		objectIDs := executor.GetFixedRows[uint64](cols[4])
		for i := 0; i < rows; i++ {
			tenantID := snapshotTenantID(levels[i], objectIDs[i], r.accountID)
			snapshots = append(snapshots, &plan.Snapshot{
				TS: &timestamp.Timestamp{PhysicalTime: timestamps[i]},
				Tenant: &plan.SnapshotTenant{
					TenantName: accountNames[i],
					TenantID:   tenantID,
				},
				ExtraInfo: &plan.SnapshotExtraInfo{
					Level: levels[i],
					ObjId: objectIDs[i],
					Name:  names[i],
				},
			})
		}
		return true
	})
	if len(snapshots) == 0 {
		return nil, &viewMetadataSnapshotNotFoundError{name: name}
	}
	if len(snapshots) != 1 {
		return nil, moerr.NewInternalErrorf(
			ctx,
			"find %d snapshot records by name(%s), expect only 1",
			len(snapshots),
			name,
		)
	}
	return snapshots[0], nil
}

func (r viewMetadataRefreshResolver) ResolveUdf(
	ctx context.Context,
	name string,
	args []*plan.Expr,
) (*function.Udf, error) {
	sql := fmt.Sprintf(
		"select cast(args as char), body, language, rettype, db, cast(modified_time as char), sql_mode "+
			"from %s.mo_user_defined_function where name = %s and db = %s",
		catalog.MO_CATALOG,
		sqlquote.String(name),
		sqlquote.String(r.defaultDatabase),
	)
	result, err := r.compile.runSqlWithResultAndOptions(
		sql,
		int32(r.accountID),
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		result.Close()
		return nil, err
	}
	defer result.Close()

	fromTypes := make([]types.Type, len(args))
	argTypeNames := make([]string, len(args))
	for i, arg := range args {
		fromTypes[i] = types.Type{
			Oid:   types.T(arg.GetTyp().Id),
			Width: arg.GetTyp().Width,
			Scale: arg.GetTyp().Scale,
		}
		argTypeNames[i] = strings.ToLower(fromTypes[i].String())
	}
	type match struct {
		udf     *function.Udf
		cost    int
		toTypes []types.T
	}
	var matches []match
	var foundRows bool
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		foundRows = foundRows || rows > 0
		argDefinitions := executor.GetStringRows(cols[0])
		bodies := executor.GetStringRows(cols[1])
		languages := executor.GetStringRows(cols[2])
		returnTypes := executor.GetStringRows(cols[3])
		databases := executor.GetStringRows(cols[4])
		modifiedTimes := executor.GetStringRows(cols[5])
		sqlModes := executor.GetStringRows(cols[6])
		for i := 0; i < rows; i++ {
			var udfArgs []*function.Arg
			if err = json.Unmarshal([]byte(argDefinitions[i]), &udfArgs); err != nil {
				return false
			}
			if len(udfArgs) != len(args) {
				continue
			}
			toTypes := make([]types.T, len(args))
			for j := range udfArgs {
				if fromTypes[j].IsDecimal() && udfArgs[j].Type == "decimal" {
					toTypes[j] = fromTypes[j].Oid
				} else {
					toTypes[j] = types.Types[udfArgs[j].Type]
				}
			}
			canCast, cost := function.UdfArgTypeMatch(fromTypes, toTypes)
			if !canCast {
				continue
			}
			mode := sqlModes[i]
			matches = append(matches, match{
				udf: &function.Udf{
					Body:         bodies[i],
					Language:     languages[i],
					RetType:      returnTypes[i],
					Args:         udfArgs,
					Db:           databases[i],
					ModifiedTime: strings.NewReplacer(" ", "_", ":", "-").Replace(modifiedTimes[i]),
					SQLMode:      &mode,
				},
				cost:    cost,
				toTypes: toTypes,
			})
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	if !foundRows {
		return nil, &viewMetadataUDFNotFoundError{
			cause: moerr.NewNotSupportedf(ctx, "function or operator '%s'", name),
		}
	}
	if len(matches) == 0 {
		return nil, moerr.NewInvalidInputf(
			ctx,
			"No matching function for call to %s(%s)",
			name,
			strings.Join(argTypeNames, ", "),
		)
	}
	slices.SortFunc(matches, func(left, right match) int {
		return cmp.Compare(left.cost, right.cost)
	})
	if len(matches) > 1 && matches[0].cost == matches[1].cost {
		return nil, moerr.NewInvalidInputf(
			ctx,
			"call to %s(%s) is ambiguous",
			name,
			strings.Join(argTypeNames, ", "),
		)
	}
	matches[0].udf.ArgsType = function.UdfArgTypeCast(fromTypes, matches[0].toTypes)
	return matches[0].udf, nil
}
