// Copyright 2024 Matrix Origin
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

package versions

import (
	"fmt"
	"strings"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func (v Version) IsReady() bool {
	return v.State == StateReady
}

func (v Version) IsUpgradingTenant() bool {
	return v.State == StateUpgradingTenant
}

func (v Version) NeedUpgradeCluster() bool {
	return v.UpgradeCluster == Yes
}

func (v Version) NeedUpgradeTenant() bool {
	return v.UpgradeTenant == Yes
}

func (v Version) CanDirectUpgrade(version string) bool {
	return Compare(v.MinUpgradeVersion, version) <= 0
}

func (v Version) GetInsertSQL(state int32) string {
	return fmt.Sprintf(`insert into %s values ('%s', %d, %d, current_timestamp(), current_timestamp())`,
		catalog.MOVersionTable,
		v.Version,
		v.VersionOffset,
		state,
	)
}

func (v Version) GetInitVersionSQL(state int32) string {
	return fmt.Sprintf(`insert into %s values ('%s', %d, %d, current_timestamp(), current_timestamp())`,
		catalog.MOVersionTable,
		v.MinUpgradeVersion,
		0,
		state,
	)
}

func AddVersion(
	version string,
	versionOffset uint32,
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf(`insert into %s values ('%s', %d, %d, current_timestamp(), current_timestamp())`,
		catalog.MOVersionTable,
		version,
		versionOffset,
		state)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func GetLatestVersion(txn executor.TxnExecutor) (Version, error) {
	sql := fmt.Sprintf(`select version, version_offset, state from %s order by create_at desc limit 1`, catalog.MOVersionTable)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return Version{}, err
	}
	defer res.Close()

	var version Version
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		version.Version = cols[0].GetStringAt(0)
		version.VersionOffset = vector.GetFixedAt[uint32](cols[1], 0)
		version.State = vector.GetFixedAt[int32](cols[2], 0)
		return true
	})
	return version, nil
}

func GetLatestUpgradeVersion(txn executor.TxnExecutor) (Version, error) {
	sql := fmt.Sprintf(`select version from %s where state != %d order by create_at desc limit 1`,
		catalog.MOVersionTable,
		StateReady)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return Version{}, err
	}
	defer res.Close()

	var version Version
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		version.Version = cols[0].GetStringAt(0)
		return true
	})
	return version, nil
}

func MustGetLatestReadyVersion(
	txn executor.TxnExecutor) (string, error) {
	sql := fmt.Sprintf(`select version from %s 
			where state = %d 
			order by create_at desc limit 1`,
		catalog.MOVersionTable,
		StateReady)

	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return "", err
	}
	defer res.Close()

	version := ""
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		version = cols[0].GetStringAt(0)
		return true
	})
	if version == "" {
		panic("missing latest ready version")
	}
	return version, nil
}

func GetVersionState(
	version string,
	versionOffset uint32,
	txn executor.TxnExecutor,
	forUpdate bool) (int32, bool, error) {
	withForUpdate := ""
	if forUpdate {
		withForUpdate = "for update"
	}
	sql := fmt.Sprintf(`select state from %s where version = '%s' and version_offset = %d %s`,
		catalog.MOVersionTable,
		version,
		versionOffset,
		withForUpdate)

	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return 0, false, err
	}
	defer res.Close()

	state := int32(0)
	loaded := false
	n := 0
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		state = vector.GetFixedAt[int32](cols[0], 0)
		loaded = true
		n++
		return true
	})
	if loaded && n > 1 {
		panic("BUG: missing version " + version)
	}
	return state, loaded, nil
}

func UpdateVersionState(
	version string,
	versionOffset uint32,
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("update %s set state = %d, update_at = current_timestamp() where version = '%s' and version_offset = %d",
		catalog.MOVersionTable,
		state,
		version,
		versionOffset)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func Compare(v1, v2 string) int {
	x1, y1, z1 := parseVersion(v1)
	x2, y2, z2 := parseVersion(v2)

	if x1 > x2 {
		return 1
	} else if x1 < x2 {
		return -1
	}

	if y1 > y2 {
		return 1
	} else if y1 < y2 {
		return -1
	}

	return z1 - z2
}

// x.y.z
func parseVersion(version string) (int, int, int) {
	fields := strings.SplitN(version, ".", 3)
	return format.MustParseStringInt(fields[0]),
		format.MustParseStringInt(fields[1]),
		format.MustParseStringInt(fields[2])
}

func IsFrameworkTablesCreated(txn executor.TxnExecutor) (bool, error) {
	res, err := txn.Exec("show tables", executor.StatementOption{})
	if err != nil {
		return false, err
	}
	defer res.Close()

	var tables []string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			tables = append(tables, cols[0].GetStringAt(i))
		}
		return true
	})
	for _, t := range tables {
		if strings.EqualFold(t, catalog.MOVersionTable) {
			return true, nil
		}
	}
	return false, nil
}

// FetchAllTenants get all tenantIDs in mo system, including system tenants
func FetchAllTenants(txn executor.TxnExecutor) ([]int32, error) {
	ids := make([]int32, 0, 10)
	sql := "select account_id from mo_account where account_id >= 0 order by account_id"
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return ids, err
	}

	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			last := vector.GetFixedAt[int32](cols[0], i)
			ids = append(ids, last)
		}
		return true
	})
	res.Close()
	return ids, nil
}
