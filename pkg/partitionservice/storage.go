// Copyright 2021-2024 Matrix Origin
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

package partitionservice

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	txnBasedMetadataCacheKey = "partitionservice.metadata.%d"
)

type storage struct {
	sid  string
	exec executor.SQLExecutor
	eng  engine.Engine
}

func NewStorage(
	sid string,
	exec executor.SQLExecutor,
	eng engine.Engine,
) PartitionStorage {
	s := &storage{
		sid:  sid,
		exec: exec,
		eng:  eng,
	}
	return s
}

func (s *storage) GetTableDef(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (*plan.TableDef, error) {
	_, _, rel, err := s.eng.GetRelationById(
		ctx,
		txnOp,
		tableID,
	)
	if err != nil {
		return nil, err
	}
	return rel.GetTableDef(ctx), nil
}

func (s *storage) GetMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, bool, error) {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return partition.PartitionMetadata{}, false, err
	}

	key := ""

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(accountID)
	if txnOp != nil {
		key = fmt.Sprintf(txnBasedMetadataCacheKey, tableID)
		if v, ok := txnOp.Get(key); ok {
			return v.(partition.PartitionMetadata), true, nil
		}
		opts = opts.WithDisableIncrStatement()
	}

	var metadata partition.PartitionMetadata
	var found bool
	err = s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			res, err := txn.Exec(
				fmt.Sprintf(
					`
						select 		          
							table_name,    
							database_name,             
							partition_method,           
							partition_description,      
							partition_count         
						from %s
					 	where 
							table_id = %d
					`,
					catalog.MOPartitionMetadata,
					tableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				if strings.Contains(err.Error(), "does not exist") &&
					strings.Contains(err.Error(), catalog.MOPartitionMetadata) {
					return nil
				}

				return err
			}

			n := uint32(0)
			res.ReadRows(
				func(
					rows int,
					cols []*vector.Vector,
				) bool {
					if rows > 1 {
						panic(fmt.Sprintf("BUG: read %d partition metadata rows, expect 1", rows))
					}

					found = true
					for i := 0; i < rows; i++ {
						metadata.TableID = tableID
						metadata.TableName = executor.GetStringRows(cols[0])[i]
						metadata.DatabaseName = executor.GetStringRows(cols[1])[i]
						metadata.Method = partition.PartitionMethod(
							partition.PartitionMethod_value[executor.GetStringRows(cols[2])[i]],
						)
						metadata.Description = executor.GetStringRows(cols[3])[i]
						n = executor.GetFixedRows[uint32](cols[4])[i]
					}
					return true
				},
			)
			res.Close()

			if !found {
				return nil
			}

			res, err = txn.Exec(
				fmt.Sprintf(
					`
					select 
						partition_id              ,
						partition_table_name      ,
						partition_name            ,
						partition_ordinal_position,
						partition_expression_str  ,
						partition_expression
					from %s
					where 
						primary_table_id = %d
					order by 
						partition_ordinal_position
				`,
					catalog.MOPartitionTables,
					tableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}

			res.ReadRows(
				func(
					rows int,
					cols []*vector.Vector,
				) bool {
					found = true
					for i := 0; i < rows; i++ {
						v := []byte(executor.GetStringRows(cols[5])[i])
						expr := &plan.Expr{}
						err = expr.Unmarshal(v)
						if err != nil {
							panic(err)
						}
						metadata.Partitions = append(
							metadata.Partitions,
							partition.Partition{
								PartitionID:        executor.GetFixedRows[uint64](cols[0])[i],
								PrimaryTableID:     tableID,
								PartitionTableName: executor.GetStringRows(cols[1])[i],
								Name:               executor.GetStringRows(cols[2])[i],
								Position:           executor.GetFixedRows[uint32](cols[3])[i],
								ExprStr:            executor.GetStringRows(cols[4])[i],
								Expr:               expr,
								ExprWithRowID:      getExprWithRowID(v),
							},
						)
					}
					return true
				},
			)
			res.Close()

			if n != uint32(len(metadata.Partitions)) {
				panic(
					fmt.Sprintf("partition count not match, expect %d, got %d",
						n,
						len(metadata.Partitions)),
				)
			}

			return nil
		},
		opts,
	)
	if found && txnOp != nil {
		txnOp.Set(key, metadata)
	}
	return metadata, found, err
}

func (s *storage) Create(
	ctx context.Context,
	def *plan.TableDef,
	stmt *tree.CreateTable,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID).
		WithAdjustTableExtraFunc(
			func(extra *api.SchemaExtra) error {
				extra.ParentTableID = def.TblId
				extra.FeatureFlag |= features.Partition
				return nil
			},
		)
	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			err = s.createPartitionMetadata(
				metadata,
				txn,
			)
			if err != nil {
				return err
			}

			for _, p := range metadata.Partitions {
				err := s.createPartitionTable(
					def,
					stmt,
					metadata,
					p,
					txn,
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
		opts,
	)
}

func (s *storage) Redefine(
	ctx context.Context,
	def *plan.TableDef,
	options *tree.PartitionOption,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID)
	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			v, _ := parsers.ParseOne(
				ctx,
				dialect.MYSQL,
				def.Createsql,
				1,
			)
			tmp := uuid.NewString()
			stmt := v.(*tree.CreateTable)
			stmt.PartitionOption = options
			table := stmt.Table
			stmt.Table = *tree.NewTableName(
				tree.Identifier(tmp),
				table.ObjectNamePrefix,
				table.AtTsExpr,
			)
			sql := tree.StringWithOpts(
				stmt,
				dialect.MYSQL,
				tree.WithQuoteIdentifier(),
				tree.WithSingleQuoteString(),
			)

			// 1. create a temporary table
			txn.Use(def.DbName)
			rs, err := txn.Exec(
				sql,
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			rs.Close()

			// 2. select data into new temporary table
			sql = fmt.Sprintf("insert into `%s` select * from `%s`",
				tmp,
				def.Name,
			)
			rs, err = txn.Exec(
				sql,
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			rs.Close()

			// 3. drop old table
			sql = fmt.Sprintf("drop table `%s`", def.Name)
			rs, err = txn.Exec(
				sql,
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			rs.Close()

			// 4. rename tmp to old table name
			sql = fmt.Sprintf("rename table `%s` to `%s`", tmp, def.Name)
			rs, err = txn.Exec(
				sql,
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			rs.Close()

			return nil
		},
		opts,
	)
}

func (s *storage) Rename(
	ctx context.Context,
	def *plan.TableDef,
	oldName, newName string,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID)

	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			for _, p := range metadata.Partitions {
				txn.Use(metadata.DatabaseName)
				res, err := txn.Exec(
					fmt.Sprintf(
						"rename table `%s` to `%s`",
						p.PartitionTableName,
						GetPartitionTableName(newName, p.Name),
					),
					executor.StatementOption{}.
						WithIgnoreForeignKey().
						WithIgnorePublish(),
				)
				if err != nil {
					return err
				}
				res.Close()

				txn.Use(catalog.MO_CATALOG)
				res, err = txn.Exec(
					fmt.Sprintf("update %s set partition_table_name = '%s' where partition_table_name = '%s' and primary_table_id = %d",
						catalog.MOPartitionTables,
						GetPartitionTableName(newName, p.Name),
						p.PartitionTableName,
						metadata.TableID,
					),
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()

				res, err = txn.Exec(
					fmt.Sprintf("update %s set table_name = '%s' where table_id = %d",
						catalog.MOPartitionMetadata,
						newName,
						metadata.TableID,
					),
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		opts,
	)
}

func (s *storage) AddPartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []partition.Partition,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID).
		WithAdjustTableExtraFunc(
			func(extra *api.SchemaExtra) error {
				extra.ParentTableID = metadata.TableID
				extra.FeatureFlag |= features.Partition
				return nil
			},
		)
	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	metadata.Partitions = append(metadata.Partitions, partitions...)
	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			stmt, _ := parsers.ParseOne(
				ctx,
				dialect.MYSQL,
				def.Createsql,
				1,
			)

			err = s.updatePartitionCount(
				metadata,
				txn,
			)
			if err != nil {
				return err
			}

			for _, p := range partitions {
				err := s.createPartitionTable(
					def,
					stmt.(*tree.CreateTable),
					metadata,
					p,
					txn,
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
		opts,
	)
}

func (s *storage) DropPartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID)

	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	whereCause := ""
	tables := make([]string, 0, len(partitions))
	for _, p := range partitions {
		found := false
		for i, mp := range metadata.Partitions {
			if p == mp.Name {
				metadata.Partitions = append(metadata.Partitions[:i], metadata.Partitions[i+1:]...)
				whereCause += fmt.Sprintf("'%s',", p)
				tables = append(tables, mp.PartitionTableName)
				found = true
				break
			}
		}
		if !found {
			return moerr.NewInvalidInput(ctx, fmt.Sprintf("partition %s not found", p))
		}
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			err = s.updatePartitionCount(
				metadata,
				txn,
			)
			if err != nil {
				return err
			}

			res, err := txn.Exec(
				fmt.Sprintf("delete from %s where primary_table_id = %d and partition_name in (%s)",
					catalog.MOPartitionTables,
					metadata.TableID,
					whereCause[:len(whereCause)-1],
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()

			for i, p := range metadata.Partitions {
				p.Position = uint32(i)

				res, err := txn.Exec(
					fmt.Sprintf("update %s set partition_ordinal_position = %d where partition_id = %d and primary_table_id = %d",
						catalog.MOPartitionTables,
						p.Position,
						p.PartitionID,
						metadata.TableID,
					),
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()
			}

			txn.Use(metadata.DatabaseName)
			for _, name := range tables {
				res, err = txn.Exec(
					fmt.Sprintf(
						"drop table `%s`",
						name,
					),
					executor.StatementOption{}.
						WithIgnoreForeignKey().
						WithIgnorePublish(),
				)
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		opts,
	)
}

func (s *storage) TruncatePartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID).
		WithAdjustTableExtraFunc(
			func(extra *api.SchemaExtra) error {
				extra.ParentTableID = def.TblId
				extra.FeatureFlag |= features.Partition
				return nil
			},
		)

	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, def.TblId))
		opts = opts.WithDisableIncrStatement()
	}

	tables := make([]string, 0, len(partitions))
	for _, p := range partitions {
		found := false
		for _, mp := range metadata.Partitions {
			if p == mp.Name {
				tables = append(tables, mp.PartitionTableName)
				found = true
				break
			}
		}
		if !found {
			return moerr.NewInvalidInput(ctx, fmt.Sprintf("partition %s not found", p))
		}
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			for _, name := range tables {
				txn.Use(metadata.DatabaseName)
				res, err := txn.Exec(
					fmt.Sprintf(
						"truncate table `%s`",
						name,
					),
					executor.StatementOption{}.
						WithIgnoreForeignKey().
						WithIgnorePublish(),
				)
				if err != nil {
					return err
				}
				res.Close()

				id, err := s.getTableIDByTableNameAndDatabaseName(
					name,
					metadata.DatabaseName,
					txn,
				)
				if err != nil {
					return err
				}

				txn.Use(catalog.MO_CATALOG)
				res, err = txn.Exec(
					fmt.Sprintf("update %s set partition_id = %d where partition_table_name = '%s' and primary_table_id = %d",
						catalog.MOPartitionTables,
						id,
						name,
						metadata.TableID,
					),
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()
			}

			return nil
		},
		opts,
	)
}

func (s *storage) Delete(
	ctx context.Context,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	opts := executor.Options{}.
		WithTxn(txnOp).
		WithAccountID(accountID)
	if txnOp != nil {
		txnOp.Delete(fmt.Sprintf(txnBasedMetadataCacheKey, metadata.TableID))
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			res, err := txn.Exec(
				fmt.Sprintf(
					"delete from %s where table_id = %d",
					catalog.MOPartitionMetadata,
					metadata.TableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()

			res, err = txn.Exec(
				fmt.Sprintf(
					"delete from %s where primary_table_id = %d",
					catalog.MOPartitionTables,
					metadata.TableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()

			txn.Use(metadata.DatabaseName)
			for _, p := range metadata.Partitions {
				res, err = txn.Exec(
					fmt.Sprintf(
						"drop table `%s`",
						p.PartitionTableName,
					),
					executor.StatementOption{}.
						WithIgnorePublish().
						WithIgnoreForeignKey(),
				)
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		opts,
	)
}

func (s *storage) createPartitionTable(
	def *plan.TableDef,
	stmt *tree.CreateTable,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
	txn executor.TxnExecutor,
) error {
	// create partition table using primary table's schema
	createPartitionTable := func() error {
		txn.Use(def.DbName)
		sql := getPartitionTableCreateSQL(
			stmt,
			partition,
		)
		res, err := txn.Exec(
			sql,
			executor.StatementOption{}.
				WithDisableLock().
				WithIgnoreForeignKey().
				WithIgnorePublish(),
		)
		if err != nil {
			return err
		}
		res.Close()

		partitionID, err := s.getTableIDByTableNameAndDatabaseName(
			partition.PartitionTableName,
			def.DbName,
			txn,
		)
		if err != nil {
			return err
		}
		partition.PartitionID = partitionID
		return nil
	}

	bs, err := partition.Expr.Marshal()
	if err != nil {
		return err
	}

	// add partition metadata to mo_catalog.mo_partitions
	addPartitionMetadata := func() error {
		txn.Use(catalog.MO_CATALOG)
		sql := fmt.Sprintf(
			`insert into %s.%s 
			(
				partition_id, 
				partition_table_name, 
				primary_table_id, 
				partition_name, 
				partition_ordinal_position, 
				partition_expression_str,
				partition_expression
			)
			values
			(
				?,
				?, 
				?, 
				?, 
				?, 
				?,
				?
			)`,
			catalog.MO_CATALOG,
			catalog.MOPartitionTables,
		)

		res, err := txn.Exec(
			sql,
			executor.StatementOption{}.WithParams(
				[]string{
					fmt.Sprintf("%d", partition.PartitionID),
					partition.PartitionTableName,
					fmt.Sprintf("%d", metadata.TableID),
					partition.Name,
					fmt.Sprintf("%d", partition.Position),
					partition.ExprStr,
					string(bs),
				},
			),
		)
		if err != nil {
			return err
		}

		res.Close()
		return nil
	}

	if err := createPartitionTable(); err != nil {
		return err
	}
	return addPartitionMetadata()
}

func (s *storage) getTableIDByTableNameAndDatabaseName(
	tableName string,
	databaseName string,
	txn executor.TxnExecutor,
) (uint64, error) {
	txn.Use(catalog.MO_CATALOG)

	sql := fmt.Sprintf("select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
		tableName,
		databaseName,
	)
	res, err := txn.Exec(
		sql,
		executor.StatementOption{},
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	id := uint64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			id = executor.GetFixedRows[uint64](cols[0])[0]
			return false
		},
	)
	return id, nil
}

func (s *storage) createPartitionMetadata(
	metadata partition.PartitionMetadata,
	txn executor.TxnExecutor,
) error {
	txn.Use(catalog.MO_CATALOG)

	sql := getInsertMetadataSQL(
		metadata,
	)

	res, err := txn.Exec(
		sql,
		executor.StatementOption{},
	)
	if err != nil {
		return err
	}

	res.Close()
	return nil
}

func (s *storage) updatePartitionCount(
	metadata partition.PartitionMetadata,
	txn executor.TxnExecutor,
) error {
	txn.Use(catalog.MO_CATALOG)

	sql := fmt.Sprintf(`
		update %s.%s
		set partition_count = %d
		where table_id = %d
	`,
		catalog.MO_CATALOG,
		catalog.MOPartitionMetadata,
		len(metadata.Partitions),
		metadata.TableID,
	)

	res, err := txn.Exec(
		sql,
		executor.StatementOption{},
	)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func getPartitionTableCreateSQL(
	stmt *tree.CreateTable,
	partition partition.Partition,
) string {
	v := stmt.PartitionOption
	table := stmt.Table
	stmt.PartitionOption = nil
	defer func() {
		stmt.PartitionOption = v
		stmt.Table = table
	}()

	stmt.Table = *tree.NewTableName(
		tree.Identifier(partition.PartitionTableName),
		table.ObjectNamePrefix,
		table.AtTsExpr,
	)
	return tree.StringWithOpts(
		stmt,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
}

func getInsertMetadataSQL(
	metadata partition.PartitionMetadata,
) string {
	return fmt.Sprintf(`
		insert into %s.%s 
			(
				table_id,
				table_name,
				database_name,
				partition_method,
				partition_description,
				partition_count
			)
		values
			(
				%d, 
				'%s', 
				'%s',
				'%s', 
				'%s',
				 %d
			)`,
		catalog.MO_CATALOG,
		catalog.MOPartitionMetadata,
		metadata.TableID,
		metadata.TableName,
		metadata.DatabaseName,
		metadata.Method.String(),
		metadata.Description,
		len(metadata.Partitions),
	)
}

func getExprWithRowID(v []byte) *plan.Expr {
	e := &plan.Expr{}
	err := e.Unmarshal(v)
	if err != nil {
		panic(err)
	}
	resetPosWithRowID(e)
	return e
}

func resetPosWithRowID(expr *plan.Expr) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range e.F.Args {
			switch col := e.F.Args[i].Expr.(type) {
			case *plan.Expr_Col:
				col.Col.ColPos++
				return
			case *plan.Expr_F:
				resetPosWithRowID(e.F.Args[i])
			}
		}
	}
}
