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

package plan

const (
	/*
		https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html
		MySQL has hard limit of 4096 columns per table, but the effective maximum may be less for a given table.
	*/
	TableColumnCountLimit = 4096
)

const (
	/*
		Identifier Length Limits
		See MySQL: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	*/
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
)

const (
	// PrimaryKeyName defines primary key name.
	PrimaryKeyName = "PRIMARY"
)
