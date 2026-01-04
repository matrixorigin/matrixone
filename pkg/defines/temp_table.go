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

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	// TempTableNamePrefix is used to identify session temporary tables.
	TempTableNamePrefix = "__mo_tmp_"
)

// GenTempTableName generates the physical table name for a session-scoped
// temporary table. Hyphens are stripped from the session ID to keep the name
// a valid identifier.
func GenTempTableName(sessionID uuid.UUID, dbName, alias string) string {
	sid := strings.ReplaceAll(sessionID.String(), "-", "")
	return fmt.Sprintf("%s%s_%s_%s", TempTableNamePrefix, sid, dbName, alias)
}

// IsTempTableName reports whether the given name belongs to a session temporary
// table.
func IsTempTableName(name string) bool {
	return strings.HasPrefix(name, TempTableNamePrefix)
}
