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

func (v Version) GetInsertSQL() string {
	return fmt.Sprintf(`insert into %s values ('%s', %d, current_timestamp(), current_timestamp())`,
		catalog.MOVersionTable,
		v.Version,
		StateCreated,
	)
}

func AddVersion(
	version string,
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf(`insert into %s values ('%s', %d, current_timestamp(), current_timestamp())`,
		catalog.MOVersionTable,
		version,
		state)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func GetLatestVersion(txn executor.TxnExecutor) (Version, error) {
	sql := fmt.Sprintf(`select version, state from %s where order by create_at desc limit 1`, catalog.MOVersionTable)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return Version{}, err
	}
	defer res.Close()

	var version Version
	res.ReadRows(func(cols []*vector.Vector) bool {
		version.Version = executor.GetStringRows(cols[0])[0]
		version.State = executor.GetFixedRows[int32](cols[1])[0]
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
	res.ReadRows(func(cols []*vector.Vector) bool {
		version.Version = executor.GetStringRows(cols[0])[0]
		return true
	})
	return version, nil
}

func GetVersionState(
	version string,
	txn executor.TxnExecutor,
	forUpdate bool) (int32, bool, error) {
	withForUpdate := ""
	if forUpdate {
		withForUpdate = "for update"
	}
	sql := fmt.Sprintf(`select state from %s where version = '%s' %s`,
		catalog.MOVersionTable,
		version,
		withForUpdate)

	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return 0, false, err
	}
	defer res.Close()

	state := int32(0)
	loaded := false
	n := 0
	res.ReadRows(func(cols []*vector.Vector) bool {
		state = executor.GetFixedRows[int32](cols[0])[0]
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
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("update %s set state = %d, update_at = current_timestamp() where version = '%s'",
		catalog.MOVersionTable,
		state,
		version)
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
