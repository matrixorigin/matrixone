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
	ErrTableNotExists     = errors.New("table not exist")
	ErrTableCreateFailed  = errors.New("create table failed")
	ErrTooMuchTableExists = errors.New("the maximum limit of tables has been exceeded")
	ErrNoAvailableShard   = errors.New("no available raft group")
	ErrTableCreateTimeout = errors.New("create table timeout")
)
