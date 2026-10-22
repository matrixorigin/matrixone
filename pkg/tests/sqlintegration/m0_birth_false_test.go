//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/m0admission"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This in-process fixture proves SQL semantics with a real executable hash and
// lock. It is not qualification of the external launcher or Linux deployment.
func admitM0BirthSQL(t *testing.T, cn embed.ServiceOperator) func() {
	t.Helper()
	root, err := filepath.EvalSymlinks(cn.GetServiceConfig().DataDir)
	require.NoError(t, err)
	t.Chdir(root)
	hash := func(path string) string {
		f, e := os.Open(path)
		require.NoError(t, e)
		defer f.Close()
		h := sha256.New()
		_, e = io.Copy(h, f)
		require.NoError(t, e)
		return hex.EncodeToString(h.Sum(nil))
	}
	write := func(path string, value any) {
		b, e := json.Marshal(value)
		require.NoError(t, e)
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, e)
		_, e = f.Write(b)
		require.NoError(t, e)
		require.NoError(t, f.Close())
	}
	files := make(map[string]string)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".toml") {
			path := filepath.Join(root, entry.Name())
			files[path] = hash(path)
		}
	}
	require.GreaterOrEqual(t, len(files), 3)
	exe, err := os.Executable()
	require.NoError(t, err)
	marker := m0admission.Marker{FormatVersion: 1, BinarySHA256: hash(exe), SourceSHA: strings.Repeat("1", 40), NativeSHA256: strings.Repeat("2", 64)}
	manifest := m0admission.Manifest{Marker: marker, RunID: "embedded-real-sql", Root: root, Args: []string{"embedded-real-sql"}, Files: files}
	manifest.DataRoots = []string{root}
	markerBytes, err := json.Marshal(marker)
	require.NoError(t, err)
	require.NoError(t, unix.Setxattr(root, m0admission.DataXattrName, markerBytes, 0))
	markerPath := filepath.Join(root, m0admission.MarkerName)
	manifestPath := filepath.Join(root, "birth-sql-test-manifest.json")
	lockPath := filepath.Join(root, m0admission.LockName)
	write(markerPath, marker)
	write(manifestPath, manifest)
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	t.Setenv(m0admission.ManifestEnv, manifestPath)
	t.Setenv(m0admission.LockFDEnv, strconv.Itoa(int(lock.Fd())))
	close, err := m0admission.Initialize(manifest.Args)
	require.NoError(t, err)
	return func() {
		close()
		require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_UN))
		require.NoError(t, lock.Close())
		if !t.Failed() {
			require.NoError(t, unix.Removexattr(root, m0admission.DataXattrName))
			for _, path := range []string{markerPath, manifestPath, lockPath} {
				require.NoError(t, os.Remove(path))
			}
		}
	}
}

func m0BirthContents(t *testing.T, ctx context.Context, db *sql.DB, table string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance,cleaned,cleaned2 from "+table+" order by c_w_id,c_d_id,c_id")
	require.NoError(t, err)
	defer rows.Close()
	var contents [][]*string
	for rows.Next() {
		raw := make([]sql.RawBytes, 8)
		dest := make([]any, len(raw))
		for i := range raw {
			dest[i] = &raw[i]
		}
		require.NoError(t, rows.Scan(dest...))
		row := make([]*string, len(raw))
		for i, value := range raw {
			if value != nil {
				cell := string(value)
				row[i] = &cell
			}
		}
		contents = append(contents, row)
	}
	require.NoError(t, rows.Err())
	data, err := json.Marshal(contents)
	require.NoError(t, err)
	return string(data)
}

func TestM0BirthFalseSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		closeAdmission := admitM0BirthSQL(t, cn)
		defer closeAdmission()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		db := openM0SQL(t, cn)
		defer func() { _ = db.Close() }()
		const schema = "m0_birth_false_sql"
		const table = schema + ".customer"
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists m0_birth_false_before", "drop snapshot if exists m0_birth_false_history", "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".empty_customer"+m0RollbackCustomerSchema)
		m0Exec(t, ctx, db, "alter table "+schema+".empty_customer add column cleaned boolean default false")
		var emptyCount int
		var emptyMin, emptyMax sql.NullBool
		var emptyNegated sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "select count(cleaned),min(cleaned),max(cleaned),sum(not cleaned) from "+schema+".empty_customer").Scan(&emptyCount, &emptyMin, &emptyMax, &emptyNegated))
		require.Zero(t, emptyCount)
		require.False(t, emptyMin.Valid)
		require.False(t, emptyMax.Valid)
		require.False(t, emptyNegated.Valid)

		m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
		m0Exec(t, ctx, db, "insert into "+table+" values(1,1,1,50000,100,10),(2,1,1,50000,100,NULL)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		sourceID, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Zero(t, version)
		objects := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, objects)
		m0Exec(t, ctx, db, "create snapshot m0_birth_false_before for table "+schema+" customer")
		m0Exec(t, ctx, db, "alter table "+table+" add column cleaned boolean default false")
		currentID, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, sourceID, currentID)
		require.Equal(t, uint32(1), version)
		require.Equal(t, objects, m0ObjectFingerprints(t, ctx, db, table), "ADD must not rewrite the old objects")
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned=false"))
		require.Equal(t, 0, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned is null"))
		var nonnull, total, negated int
		var minimum, maximum bool
		require.NoError(t, db.QueryRowContext(ctx, "select count(cleaned),count(*),min(cleaned),max(cleaned),sum(not cleaned) from "+table).Scan(&nonnull, &total, &minimum, &maximum, &negated))
		require.Equal(t, 2, nonnull)
		require.Equal(t, 2, total)
		require.False(t, minimum)
		require.False(t, maximum)
		require.Equal(t, 2, negated)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where not cleaned"))
		require.Zero(t, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned=true"))

		m0Exec(t, ctx, db, "insert into "+table+" values(3,1,1,50000,100,30,NULL),(4,1,1,50000,100,40,true),(5,1,1,50000,100,50,false)")
		m0Exec(t, ctx, db, "alter table "+table+" add column cleaned2 boolean default false")
		_, version = m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, uint32(2), version)
		require.Equal(t, 5, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned2=false"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned is null"))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(cleaned) from "+table))
		m0Exec(t, ctx, db, "update "+table+" set cleaned=true where c_id=1")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		// Same-binary ordinary COPY is a content oracle here. Exact D binary
		// parity remains part of the separately gated Linux qualification.
		oracle := schema + ".copy_oracle"
		m0Exec(t, ctx, db, "create table "+oracle+m0RollbackCustomerSchema)
		m0Exec(t, ctx, db, "insert into "+oracle+" values(1,1,1,50000,100,10),(2,1,1,50000,100,NULL)")
		m0Exec(t, ctx, db, "alter table "+oracle+" add column cleaned boolean default false, algorithm=copy")
		m0Exec(t, ctx, db, "insert into "+oracle+" values(3,1,1,50000,100,30,NULL),(4,1,1,50000,100,40,true),(5,1,1,50000,100,50,false)")
		m0Exec(t, ctx, db, "alter table "+oracle+" add column cleaned2 boolean default false, algorithm=copy")
		m0Exec(t, ctx, db, "update "+oracle+" set cleaned=true where c_id=1")
		ordinaryContents := m0BirthContents(t, ctx, db, oracle)
		require.Equal(t, ordinaryContents, m0BirthContents(t, ctx, db, table))
		t.Logf("BIRTH_CONTENT_ORACLE rows=5 sha256=%x", sha256.Sum256([]byte(ordinaryContents)))
		m0Exec(t, ctx, db, "create table "+schema+".before_add clone "+table+" {snapshot='m0_birth_false_before'}")
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+schema+".before_add"))
		_, err = db.QueryContext(ctx, "select cleaned from "+schema+".before_add")
		require.Error(t, err)
		m0Exec(t, ctx, db, "create snapshot m0_birth_false_history for table "+schema+" customer")
		m0Exec(t, ctx, db, "create table "+schema+".child clone "+table)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+schema+".child where cleaned=false"))
		m0Exec(t, ctx, db, "create table "+schema+".delete_oracle like "+oracle)
		m0Exec(t, ctx, db, "insert into "+schema+".delete_oracle select * from "+oracle)
		m0Exec(t, ctx, db, "delete from "+schema+".child where c_balance is null")
		m0Exec(t, ctx, db, "delete from "+schema+".delete_oracle where c_balance is null")
		deleteContents := m0BirthContents(t, ctx, db, schema+".delete_oracle")
		require.Equal(t, deleteContents, m0BirthContents(t, ctx, db, schema+".child"))
		require.Equal(t, ordinaryContents, m0BirthContents(t, ctx, db, table), "child DELETE must not change its parent")
		// Materialize old-prefix objects through the real TN merge path, then
		// reopen CN and finally TN+CN to exercise durable birth state.
		m0Exec(t, ctx, db, "alter table "+schema+".child add column child_only boolean default false")
		m0Exec(t, ctx, db, "update "+schema+".child set child_only=true where c_id=3")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".child where child_only=true"))
		parentRows, parentErr := db.QueryContext(ctx, "select child_only from "+table)
		if parentRows != nil {
			parentRows.Close()
		}
		require.Error(t, parentErr)
		m0RunL0Merge(t, ctx, db, schema+".child")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".child where cleaned is null"))
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, cn.Start())
		db = openM0SQL(t, cn)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned=false"))
		tn := m0ServiceOfType(t, cluster, metadata.ServiceType_TN)
		// No checkpoint is requested before this restart; replay logs record
		// which WAL work was actually required, without claiming its coverage.
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, tn.Close())
		require.NoError(t, tn.Start())
		require.NoError(t, cn.Start())
		db = openM0SQL(t, cn)
		require.Equal(t, ordinaryContents, m0BirthContents(t, ctx, db, table))
		m0Exec(t, ctx, db, "select mo_ctl('dn','globalcheckpoint','')")
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, tn.Close())
		require.NoError(t, tn.Start())
		require.NoError(t, cn.Start())
		db = openM0SQL(t, cn)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned=false"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned is null"))
		require.Equal(t, ordinaryContents, m0BirthContents(t, ctx, db, table))
		require.Equal(t, deleteContents, m0BirthContents(t, ctx, db, schema+".child"))
		m0Exec(t, ctx, db, "alter table "+table+" alter column cleaned set default true")
		exitID, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.NotEqual(t, sourceID, exitID)
		require.Zero(t, version)
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned=false"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where cleaned is null"))
		m0Exec(t, ctx, db, "insert into "+table+"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values(6,1,1,50000,100,60)")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where c_id=6 and cleaned=true"))
		m0Exec(t, ctx, db, "alter table "+oracle+" alter column cleaned set default true")
		m0Exec(t, ctx, db, "insert into "+oracle+"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values(6,1,1,50000,100,60)")
		require.Equal(t, m0BirthContents(t, ctx, db, oracle), m0BirthContents(t, ctx, db, table))

		m0Exec(t, ctx, db, "create table "+schema+".history clone "+table+" {snapshot='m0_birth_false_history'}")
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+schema+".history where cleaned=false"))
		require.Equal(t, 5, m0Count(t, ctx, db, "select count(*) from "+schema+".history"))
		m0Exec(t, ctx, db, "alter table "+schema+".child rename to renamed_child")
		_, version = m0TableGeneration(t, ctx, db, schema, "renamed_child")
		require.Zero(t, version)
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".renamed_child where cleaned=false"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".renamed_child where child_only=true"))

		m0Exec(t, ctx, db, "alter table "+schema+".history comment='a''b'")
		_, version = m0TableGeneration(t, ctx, db, schema, "history")
		require.Zero(t, version)
		m0Exec(t, ctx, db, "alter table "+schema+".history drop column cleaned")
		m0Exec(t, ctx, db, "alter table "+schema+".history add column cleaned boolean default false")
		require.Equal(t, 5, m0Count(t, ctx, db, "select count(*) from "+schema+".history where cleaned=false"))
		m0Exec(t, ctx, db, "rename table "+schema+".history to "+schema+".history_tmp, "+schema+".history_tmp to "+schema+".renamed_history")
		_, version = m0TableGeneration(t, ctx, db, schema, "renamed_history")
		require.Zero(t, version)
		require.Equal(t, 5, m0Count(t, ctx, db, "select count(*) from "+schema+".renamed_history where cleaned=false"))

	})
}

func TestM0BirthFalseRollbackPrivate(t *testing.T) {
	m0RunRollbackAt(t, nil, "m0_birth_rollback_private", disttae.M0FaultAfterDefs, true)
}
func TestM0BirthFalseRollbackTNApplied(t *testing.T) {
	m0RunRollbackAt(t, nil, "m0_birth_rollback_tn", "fj/m0/alter/tn-after-apply", true)
}
func TestM0BirthFalseCancel(t *testing.T) {
	m0RunAlterCancelWindow(t, disttae.M0FaultAfterTNAlter, "m0_birth_cancel", false, true)
}
func TestM0BirthFalseDeadlineTNApplied(t *testing.T) {
	m0RunAlterCancelWindow(t, "fj/m0/alter/tn-after-apply", "m0_birth_deadline", true, true)
}

func TestM0BirthFalseDMLInterleave(t *testing.T) { m0RunAlterInsertInterleave(t, true) }
