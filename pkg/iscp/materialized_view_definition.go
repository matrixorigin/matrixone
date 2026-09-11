// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func MaterializedViewInfo(d *mvdefinition.Definition) (*ConsumerInfo, error) {
	ref, err := d.Reference()
	if err != nil {
		return nil, err
	}
	info := &ConsumerInfo{ConsumerType: int8(ConsumerType_MaterializedView), MVReference: ref, DBName: d.Target.Database, TableName: d.Target.Name, Columns: append([]string(nil), d.Columns...), RefreshSQL: d.RefreshSQL, IncrementalSpec: d.Incremental, RefreshMethod: d.Method}
	for _, source := range d.Sources {
		info.SrcTables = append(info.SrcTables, TableInfo{DBName: source.Database, DBID: source.DatabaseID, TableName: source.Name, TableID: source.ID})
	}
	info.SrcTable = info.SrcTables[0]
	info.SourceSQL = sqlquote.QualifiedIdent(info.SrcTable.DBName, info.SrcTable.TableName)
	return info, nil
}

func LoadMaterializedViewDefinition(ctx context.Context, eng engine.Engine, service string, txn client.TxnOperator, info *ConsumerInfo, lockDefinition bool) (*mvdefinition.Definition, error) {
	if info == nil {
		return nil, mvdefinition.Invalid("missing job reference")
	}
	if err := info.MVReference.Validate(); err != nil {
		return nil, err
	}
	if eng == nil || txn == nil {
		return nil, mvdefinition.Invalid("catalog snapshot is unavailable")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	resolve := func(database, name string) (*plan.TableDef, error) {
		db, err := eng.Database(ctx, database, txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(ctx, name, nil)
		if err != nil {
			return nil, err
		}
		return rel.GetTableDef(ctx), nil
	}
	target, err := resolve(info.DBName, info.TableName)
	if err != nil {
		return nil, mvdefinition.Invalid("target is unavailable: %v", err)
	}
	d, err := mvdefinition.FromTable(target)
	if err != nil {
		return nil, err
	}
	if err = d.Match(info.MVReference); err != nil {
		return nil, err
	}
	if d.AccountID != accountID {
		return nil, mvdefinition.Invalid("account identity changed")
	}
	if lockDefinition {
		if err = lockMaterializedViewDefinition(ctx, service, txn, d); err != nil {
			return nil, err
		}
	}
	if err = d.ValidateSources(func(source mvdefinition.Source) (*plan.TableDef, error) { return resolve(source.Database, source.Name) }); err != nil {
		return nil, err
	}
	expected, _ := MaterializedViewInfo(d)
	actualSources := info.SourceTableInfos()
	if len(actualSources) != len(expected.SrcTables) {
		return nil, mvdefinition.Invalid("job source projection changed")
	}
	for i, source := range actualSources {
		if source != expected.SrcTables[i] {
			return nil, mvdefinition.Invalid("job source projection changed")
		}
	}
	if d.State != nil {
		state, err := resolve(d.State.Database, d.State.Name)
		if err != nil {
			return nil, mvdefinition.Invalid("auxiliary relation is unavailable: %v", err)
		}
		owner, err := mvdefinition.StateOwner(state)
		if err != nil {
			return nil, err
		}
		if state.DbId != d.State.DatabaseID || state.TblId != d.State.ID || owner.AccountID != d.AccountID || owner.TargetID != d.Target.ID || owner.Generation != d.Generation {
			return nil, mvdefinition.Invalid("auxiliary relation identity changed")
		}
	}
	return d, nil
}

// Lock the same catalog primary keys used by DROP/ALTER, using the existing SQL
// locking owner. Shared locks last through the target/state/watermark commit.
// Lock databases before relation names, in a stable order, including the target.
func lockMaterializedViewDefinition(ctx context.Context, service string, txn client.TxnOperator, d *mvdefinition.Definition) error {
	databases := map[string]bool{d.Target.Database: true}
	relations := []mvdefinition.Relation{d.Target}
	for _, source := range d.Sources {
		databases[source.Database] = true
		relations = append(relations, source.Relation)
	}
	if d.State != nil {
		relations = append(relations, *d.State)
	}
	names := make([]string, 0, len(databases))
	for name := range databases {
		names = append(names, name)
	}
	sort.Strings(names)
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = sqlquote.String(name)
	}
	queries := []struct {
		sql  string
		rows int
	}{{fmt.Sprintf("SELECT dat_id FROM mo_catalog.mo_database WHERE account_id=%d AND datname IN (%s) ORDER BY datname FOR SHARE", d.AccountID, strings.Join(quoted, ",")), len(names)}}
	sort.Slice(relations, func(i, j int) bool {
		if relations[i].Database != relations[j].Database {
			return relations[i].Database < relations[j].Database
		}
		return relations[i].Name < relations[j].Name
	})
	ids := make([]string, len(relations))
	for i, rel := range relations {
		ids[i] = strconv.FormatUint(rel.ID, 10)
	}
	queries = append(queries, struct {
		sql  string
		rows int
	}{fmt.Sprintf("SELECT rel_id FROM mo_catalog.mo_tables WHERE account_id=%d AND rel_id IN (%s) ORDER BY reldatabase,relname FOR SHARE", d.AccountID, strings.Join(ids, ",")), len(relations)})
	for _, query := range queries {
		result, err := ExecWithResult(ctx, query.sql, service, txn)
		if err != nil {
			return err
		}
		rows := 0
		for _, bat := range result.Batches {
			rows += bat.RowCount()
		}
		result.Close()
		if rows != query.rows {
			return mvdefinition.Invalid("catalog identity disappeared before publication")
		}
	}
	return nil
}
