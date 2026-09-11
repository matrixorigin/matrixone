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

package function

const (
	// HexMySQLNumericOverloadStart separates the pre-MORPCVersion64 HEX
	// identities from DECIMAL and explicit-REAL-CAST identities. Keep IDs 0..7
	// stable for persisted expressions compiled by older versions.
	HexMySQLNumericOverloadStart = 8
	HexExplicitFloat32Overload   = 11
	HexExplicitFloat64Overload   = 12
)
