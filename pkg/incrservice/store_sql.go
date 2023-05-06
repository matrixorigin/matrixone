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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// AutoColumn model
type AutoColumn struct {
	Name   string `gorm:"column:name;primaryKey"`
	Offset uint64 `gorm:"column:offset"`
	Step   uint64 `gorm:"column:step"`
}

func (c AutoColumn) TableName() string {
	_ = catalog.AutoIncrTableName
	return "mo_increment_columns"
}

type sqlStore struct {
	db *gorm.DB
}

func NewSQLStore(dsn string) (IncrValueStore, error) {
	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{
			PrepareStmt: true,
			Logger:      gormlogger.Default.LogMode(gormlogger.Error),
		})
	if err != nil {
		return nil, err
	}
	return &sqlStore{db: db}, nil
}

func (s *sqlStore) Create(
	ctx context.Context,
	key string,
	value uint64,
	step int) error {
	col := AutoColumn{
		Name:   key,
		Offset: value,
		Step:   uint64(step),
	}
	result := s.db.Create(&col)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func (s *sqlStore) Alloc(
	ctx context.Context,
	key string,
	count int) (uint64, uint64, error) {

	var col AutoColumn
	var curr, next uint64
	ok := false
	for {
		err := s.db.Transaction(func(tx *gorm.DB) error {
			result := tx.First(&col, "name = ?", key)
			if result.Error != nil {
				return result.Error
			}

			curr = col.Offset
			next = getNext(curr, count, int(col.Step))
			result = tx.Model(&AutoColumn{}).
				Where("offset = ?", curr).
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

func (s *sqlStore) Delete(
	ctx context.Context,
	keys []string) error {

	return nil
}

func (s *sqlStore) Close() {

}
