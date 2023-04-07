// Copyright 2022 Matrix Origin
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

package main

import (
	"database/sql"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"time"
)

const (
	defaultUsername        = "dump"
	defaultPassword        = "111"
	defaultHost            = "127.0.0.1"
	defaultPort            = 6001
	defaultNetBufferLength = mpool.MB
	minNetBufferLength     = mpool.KB * 16
	maxNetBufferLength     = mpool.MB * 16
	defaultCsv             = false
	defaultLocalInfile     = true
	timeout                = 10 * time.Second
)

const (
	quoteFmt   = "%q"
	defaultFmt = "%s"
	jsonFmt    = "\"%s\""
)

var (
	conn      *sql.DB
	nullBytes = []byte("\\N")
)

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name string
	Kind string
}

type Tables []Table
