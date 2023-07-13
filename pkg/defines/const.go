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

package defines

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

const (
	SharedFileServiceName = "SHARED"
	LocalFileServiceName  = "LOCAL"
	PublicFileServiceName = "PUBLIC"
)

const (
	// TEMPORARY_DBNAME used to store all temporary table created by session.
	// when a user tries to create a database with this name, will be rejected at the plan stage.
	TEMPORARY_DBNAME = "%!%mo_temp_db"

	// TEMPORARY_TABLE_DN_ADDR marked as virtual dn address only for temporary table
	// When a TargetDN.address in TxnRequest is TEMPORARY_TABLE_DN_ADDR, this TxnRequest is for temporary table
	// and execution flow will go to the func in handleTemp
	TEMPORARY_TABLE_DN_ADDR = "%!%mo_temp_db_dn_address"
)
