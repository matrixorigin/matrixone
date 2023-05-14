// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// FIXME: Due to the bootstrap system, the implementation here is very ugly. 0.9 needs to
// be changed out.
var (
	name               = "mo_increment_columns"
	createMoIndexesSql = `create table if not exists mo_indexes(
		id 			bigint unsigned not null,
		table_id 	bigint unsigned not null,
		database_id bigint unsigned not null,
		name 		varchar(64) not null,
		type        varchar(11) not null,
		is_visible  tinyint not null,
		hidden      tinyint not null,
		comment 	varchar(2048) not null,
		column_name    varchar(256) not null,
		ordinal_position  int unsigned  not null,
		options     text,
		index_table_name varchar(5000),
		primary key(id, column_name)
	);`
	createAutoTableSql = fmt.Sprintf(`create table if not exists %s (
		table_id   bigint unsigned, 
		col_name     varchar(770), 
		col_index      int,
		offset     bigint unsigned, 
		step       bigint unsigned,  
		primary key(table_id, col_name)
	);`, name)
)

func (c AutoColumn) TableName() string {
	return name
}

type sqlStore struct {
	db *gorm.DB
}

func NewSQLStore(dsn string) (IncrValueStore, error) {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{
			PrepareStmt: false,
			Logger:      gormlogger.Default.LogMode(gormlogger.Error),
		})
	if err != nil {
		return nil, err
	}
	return &sqlStore{db: db}, nil
}

func (s *sqlStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn) error {
	db, err := s.getSession(ctx)
	if err != nil {
		return err
	}

	if err := db.Exec(createMoIndexesSql).Error; err != nil {
		return err
	}
	if err := db.Exec(createAutoTableSql).Error; err != nil {
		return err
	}
	return db.Create(&cols).Error
}

func (s *sqlStore) Alloc(
	ctx context.Context,
	tableID uint64,
	key string,
	count int) (uint64, uint64, error) {
	db, err := s.getSession(ctx)
	if err != nil {
		return 0, 0, err
	}

	var col AutoColumn
	var curr, next uint64
	ok := false
	for {
		err := db.Transaction(func(tx *gorm.DB) error {
			result := tx.First(
				&col, "table_id = ? and col_name = ?",
				tableID,
				key)
			if result.Error != nil {
				return result.Error
			}

			curr = col.Offset
			next = getNext(curr, count, int(col.Step))
			result = tx.Model(&AutoColumn{}).
				Where("table_id = ? and col_name = ? and offset = ?",
					tableID,
					key,
					curr).
				Update("offset", next)
			if result.Error != nil {
				return result.Error
			}

			if result.RowsAffected == 1 {
				ok = true
				return nil
			}
			return nil
		})
		if err != nil {
			return 0, 0, err
		}
		if ok {
			break
		}
	}

	from, to := getNextRange(curr, next, int(col.Step))
	return from, to, nil
}

func (s *sqlStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64) error {
	db, err := s.getSession(ctx)
	if err != nil {
		return err
	}

	err = db.Model(&AutoColumn{}).
		Where("table_id = ? and col_name = ? and offset < ?",
			tableID,
			col,
			minValue).
		Update("offset", minValue).Error
	if err != nil {
		logutil.Fatalf("2")
	}
	return err
}

func (s *sqlStore) Delete(
	ctx context.Context,
	tableID uint64) error {
	db, err := s.getSession(ctx)
	if err != nil {
		return err
	}
	err = db.Delete(&AutoColumn{}, "table_id = ?", tableID).Error
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlStore) GetCloumns(
	ctx context.Context,
	tableID uint64) ([]AutoColumn, error) {
	db, err := s.getSession(ctx)
	if err != nil {
		return nil, err
	}
	var cols []AutoColumn
	if err := db.Order("col_index").
		Find(&cols, "table_id = ?", tableID).Error; err != nil {
		return nil, err
	}
	return cols, nil
}

func (s *sqlStore) Close() {

}

func (s *sqlStore) getSession(ctx context.Context) (*gorm.DB, error) {
	db := s.db.Session(&gorm.Session{PrepareStmt: false})
	v := ctx.Value(defines.TenantIDKey{})
	if v == nil {
		return db, nil
	}
	err := db.Exec(fmt.Sprintf("SET SESSION default_account = '%d'", v)).Error
	if err != nil {
		return nil, err
	}
	return db, nil
}
