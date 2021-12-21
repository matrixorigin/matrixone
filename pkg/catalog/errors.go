// Copyright 2021 Matrix Origin
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

package catalog

import (
	"errors"
)

var (
	// ErrDBCreate is the err
	ErrDBCreate = errors.New("db create failed")
	// ErrDBCreateExists is the error for db exists.
	ErrDBCreateExists = errors.New("db already exists")
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = errors.New("db not exist")
	// ErrTableCreateExists is the error for table exists.
	ErrTableCreateExists = errors.New("table already exists")
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = errors.New("table not exist")
	//ErrTableCreateFailed is the error for fail in creating table.
	ErrTableCreateFailed = errors.New("create table failed")
	//ErrTooMuchTableExists is the error for the number of tables exceeds the limit.
	ErrTooMuchTableExists = errors.New("the maximum limit of tables has been exceeded")
	//ErrNoAvailableShard is the error for fail in fetching a shard.
	ErrNoAvailableShard = errors.New("no available raft group")
	//ErrTableCreateTimeout is the error for timeout when creating a table.
	ErrTableCreateTimeout = errors.New("create table timeout")
	//ErrShardidTimeout is the error for timeout when creating a shard id.
	ErrShardidTimeout = errors.New("create table timeout")
	//ErrTableCreateFailed is the error for fail in creating tablet.
	ErrTabletCreateFailed = errors.New("create tablet failed")
	//ErrInvalidIndexType is the error for invalid index type.
	ErrInvalidIndexType = errors.New("index type invalid")
	//ErrColumnNotExist is the error that the column required by index or primary key doesn't exist in the table.
	ErrColumnNotExist = errors.New("column not exist")
	//ErrPrimaryKeyNotExist is the error for primary key not exist.
	ErrPrimaryKeyNotExist = errors.New("primary key not exist")
	//ErrIndexExist is the error for duplicated index name.
	ErrIndexExist = errors.New("index already exist")
	//ErrIndexNotExist is the error for trying to drop an index that doesn't exit.
	ErrIndexNotExist = errors.New("index not exist")
	//ErrShardPending is for pending shards
	ErrShardPending = errors.New("shard is pending")
)
