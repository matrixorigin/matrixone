//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// Exercise same-table LOAD after a predicate DELETE leaves a committed object
// with no visible rows. This does not simulate remote dispatch.
func TestM0LoadRetainedSameObjectRejected(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema, table = "m0_load_same_object", "m0_load_same_object.customer"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
		tableID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		m0Exec(t, ctx, db, "insert into "+table+" values (1,1,1,50000,10000,10),(2,2,2,60000,6000,NULL)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		service := cn.RawService().(cnservice.Service)
		frontier, _ := service.GetClock().Now()
		applied, err := service.GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
		require.NoError(t, err)
		require.False(t, applied.Less(frontier))
		before := m0ObjectFingerprints(t, ctx, db, table)
		require.NotEmpty(t, before)
		dump := filepath.Join(t.TempDir(), "metadata")
		m0Exec(t, ctx, db, "dump table "+table+" to '"+dump+"' metadata only")
		m0Exec(t, ctx, db, "delete from "+table+" where c_id > 0")
		require.Zero(t, m0Count(t, ctx, db, "select count(*) from "+table))
		deletedID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, tableID, deletedID, "the diagnostic must not truncate or replace the relation")
		retained := m0ObjectFingerprints(t, ctx, db, table)
		t.Logf("metadata scan before delete: %d object(s); after delete: %d object(s)", len(before), len(retained))
		require.Equal(t, before, retained, "the original physical object must remain in the target relation")
		_, err = db.ExecContext(ctx, "load table "+table+" from '"+dump+"'")
		require.ErrorContains(t, err, "target relation customer must be empty")
		require.Zero(t, m0Count(t, ctx, db, "select count(*) from "+table))
		afterID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, tableID, afterID, "failed LOAD must not replace the relation")
		require.Equal(t, before, m0ObjectFingerprints(t, ctx, db, table), "failed LOAD must not rewrite the retained object")
		m0Exec(t, ctx, db, "insert into "+table+" values (3,3,3,70000,7000,30)")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where c_id=3 and c_balance=30"))
	})
}

// A logical schema match does not itself imply identical physical seqnums:
// the composite key precedes an M0 tail but follows columns of a fresh table.
func TestM0LoadAppendIntoFreshEquivalentSchema(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_load_append_layout"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".src (c_id int not null,c_sub int not null,primary key(c_id,c_sub))")
		m0Exec(t, ctx, db, "insert into "+schema+".src values(1,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".src')")
		beforeID, beforeVersion := m0TableGeneration(t, ctx, db, schema, "src")
		beforeObjects := m0ObjectFingerprints(t, ctx, db, schema+".src")
		require.NotEmpty(t, beforeObjects)
		m0Exec(t, ctx, db, "alter table "+schema+".src add column x int")
		afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, "src")
		require.Equal(t, beforeID, afterID)
		require.Equal(t, beforeVersion+1, afterVersion)
		require.Equal(t, beforeObjects, m0ObjectFingerprints(t, ctx, db, schema+".src"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".src where c_id=1 and c_sub=2 and x is null"))
		for _, tc := range []struct{ name, suffix string }{{"metadata", " metadata only"}, {"archive", ""}} {
			t.Run(tc.name, func(t *testing.T) {
				dst, matching := "dst_"+tc.name, "matching_"+tc.name
				m0Exec(t, ctx, db, "create table "+schema+"."+dst+" (c_id int not null,c_sub int not null,x int,primary key(c_id,c_sub))")
				targetID, targetVersion := m0TableGeneration(t, ctx, db, schema, dst)
				dump := filepath.Join(t.TempDir(), "dump")
				m0Exec(t, ctx, db, "dump table "+schema+".src to '"+dump+"'"+tc.suffix)
				_, err := db.ExecContext(ctx, "load table "+schema+"."+dst+" from '"+dump+"'")
				require.ErrorContains(t, err, "M0: physical table LOAD cannot join an evolution contract")
				checkDB := openM0SQL(t, cn)
				defer checkDB.Close()
				require.Zero(t, m0Count(t, ctx, checkDB, "select count(*) from "+schema+"."+dst), "rejected LOAD must not publish objects")
				readID, readVersion := m0TableGeneration(t, ctx, checkDB, schema, dst)
				require.Equal(t, targetID, readID)
				require.Equal(t, targetVersion, readVersion)
				require.Empty(t, m0ObjectFingerprints(t, ctx, checkDB, schema+"."+dst))
				require.Equal(t, 1, m0Count(t, ctx, checkDB, "select count(*) from "+schema+".src where c_id=1 and c_sub=2 and x is null"))
				// Identical geometry still cannot authorize foreign physical objects.
				m0Exec(t, ctx, db, "create table "+schema+"."+matching+" (c_id int not null,c_sub int not null,primary key(c_id,c_sub))")
				m0Exec(t, ctx, db, "alter table "+schema+"."+matching+" add column x int")
				_, err = db.ExecContext(ctx, "load table "+schema+"."+matching+" from '"+dump+"'")
				require.ErrorContains(t, err, "M0: physical table LOAD cannot join an evolution contract")
				require.Zero(t, m0Count(t, ctx, db, "select count(*) from "+schema+"."+matching))
				require.Empty(t, m0ObjectFingerprints(t, ctx, db, schema+"."+matching))
			})
		}
		t.Run("target_version_advanced_without_layout_change", func(t *testing.T) {
			const dst = "dst_versioned"
			m0Exec(t, ctx, db, "create table "+schema+"."+dst+" (c_id int not null,c_sub int not null,x int,primary key(c_id,c_sub))")
			targetID, before := m0TableGeneration(t, ctx, db, schema, dst)
			m0Exec(t, ctx, db, "alter table "+schema+"."+dst+" comment='' ")
			gotID, after := m0TableGeneration(t, ctx, db, schema, dst)
			require.Equal(t, targetID, gotID)
			require.Equal(t, before+1, after)
			dump := filepath.Join(t.TempDir(), "dump")
			m0Exec(t, ctx, db, "dump table "+schema+".src to '"+dump+"' metadata only")
			_, err := db.ExecContext(ctx, "load table "+schema+"."+dst+" from '"+dump+"'")
			require.Error(t, err, "non-append version changes must not bypass the LOAD layout boundary")
			checkDB := openM0SQL(t, cn)
			defer checkDB.Close()
			require.Zero(t, m0Count(t, ctx, checkDB, "select count(*) from "+schema+"."+dst))
			require.Empty(t, m0ObjectFingerprints(t, ctx, checkDB, schema+"."+dst))
			// A non-append version change alone is not a reason to reject an
			// ordinary source whose present physical columns match the target.
			m0Exec(t, ctx, db, "create table "+schema+".ordinary (c_id int not null,c_sub int not null,x int,primary key(c_id,c_sub))")
			m0Exec(t, ctx, db, "alter table "+schema+".ordinary comment='' ")
			_, ordinaryVersion := m0TableGeneration(t, ctx, db, schema, "ordinary")
			require.Equal(t, after, ordinaryVersion)
			m0Exec(t, ctx, db, "insert into "+schema+".ordinary values(3,4,7)")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".ordinary')")
			ordinaryDump := filepath.Join(t.TempDir(), "ordinary")
			m0Exec(t, ctx, db, "dump table "+schema+".ordinary to '"+ordinaryDump+"' metadata only")
			const omission = "fj/m0/evolution/omit-create"
			defer m0RemoveFaultPointQuiet(context.Background(), db, omission)
			m0AddFaultPoint(t, ctx, db, omission, "echo", "")
			m0Exec(t, ctx, db, "create table "+schema+".ordinary_target (c_id int not null,c_sub int not null,x int,primary key(c_id,c_sub))")
			m0RemoveFaultPoint(t, ctx, db, omission)
			m0Exec(t, ctx, db, "alter table "+schema+".ordinary_target comment='' ")
			m0Exec(t, ctx, db, "load table "+schema+".ordinary_target from '"+ordinaryDump+"'")
			require.Equal(t, 1, m0Count(t, ctx, checkDB, "select count(*) from "+schema+".ordinary_target where c_id=3 and c_sub=4 and x=7"))
		})
		t.Run("target_version_same_oid_collision", func(t *testing.T) {
			m0Exec(t, ctx, db, "create table "+schema+".varchar_src (c_id int not null,c_sub int not null,primary key(c_id,c_sub))")
			m0Exec(t, ctx, db, "insert into "+schema+".varchar_src values(5,6)")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".varchar_src')")
			require.NotEmpty(t, m0ObjectFingerprints(t, ctx, db, schema+".varchar_src"))
			m0Exec(t, ctx, db, "alter table "+schema+".varchar_src add column x varchar(32)")
			m0Exec(t, ctx, db, "create table "+schema+".varchar_dst (c_id int not null,c_sub int not null,x varchar(32),primary key(c_id,c_sub))")
			_, before := m0TableGeneration(t, ctx, db, schema, "varchar_dst")
			m0Exec(t, ctx, db, "alter table "+schema+".varchar_dst comment='' ")
			_, after := m0TableGeneration(t, ctx, db, schema, "varchar_dst")
			require.Equal(t, before+1, after)
			dump := filepath.Join(t.TempDir(), "same_oid")
			m0Exec(t, ctx, db, "dump table "+schema+".varchar_src to '"+dump+"' metadata only")
			_, err := db.ExecContext(ctx, "load table "+schema+".varchar_dst from '"+dump+"'")
			require.Error(t, err, "the target key must not be inferred from a same-OID tail column")
			checkDB := openM0SQL(t, cn)
			defer checkDB.Close()
			require.Zero(t, m0Count(t, ctx, checkDB, "select count(*) from "+schema+".varchar_dst"))
			require.Empty(t, m0ObjectFingerprints(t, ctx, checkDB, schema+".varchar_dst"))
		})
		t.Run("full_width_same_oid_collision", func(t *testing.T) {
			m0Exec(t, ctx, db, "create table "+schema+".full_src (c_id int not null,c_sub int not null,primary key(c_id,c_sub))")
			m0Exec(t, ctx, db, "alter table "+schema+".full_src add column x varchar(32)")
			m0Exec(t, ctx, db, "insert into "+schema+".full_src values(7,8,'expected')")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".full_src')")
			require.NotEmpty(t, m0ObjectFingerprints(t, ctx, db, schema+".full_src"))
			m0Exec(t, ctx, db, "create table "+schema+".full_dst (c_id int not null,c_sub int not null,x varchar(32),primary key(c_id,c_sub))")
			m0Exec(t, ctx, db, "alter table "+schema+".full_dst comment='' ")
			_, sourceVersion := m0TableGeneration(t, ctx, db, schema, "full_src")
			_, targetVersion := m0TableGeneration(t, ctx, db, schema, "full_dst")
			require.Equal(t, sourceVersion, targetVersion)
			require.NotEqual(t, m0StoredColumnLayout(t, ctx, db, schema, "full_src"), m0StoredColumnLayout(t, ctx, db, schema, "full_dst"))
			var sourceKeySeq, targetKeySeq uint16
			for table, target := range map[string]*uint16{"full_src": &sourceKeySeq, "full_dst": &targetKeySeq} {
				require.NoError(t, db.QueryRowContext(ctx, "select attr_seqnum from mo_catalog.mo_columns where att_database=? and att_relname=? and attname=?", schema, table, pkgcatalog.CPrimaryKeyColName).Scan(target))
			}
			require.NotEqual(t, sourceKeySeq, targetKeySeq)
			var locationText string
			require.NoError(t, db.QueryRowContext(ctx, "select obj_loc from metadata_scan(?, 'c_id') g limit 1", schema+".full_src").Scan(&locationText))
			location, err := objectio.StringToLocation(locationText)
			require.NoError(t, err)
			service := cn.RawService().(cnservice.Service)
			engine, ok := service.GetEngine().(*disttae.Engine)
			require.True(t, ok)
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), engine.FS())
			require.NoError(t, err)
			header, err := reader.ReadHeader(ctx, mp)
			require.NoError(t, err)
			objectVersion, err := objectio.ObjectSchemaVersion(header)
			require.NoError(t, err)
			require.Equal(t, targetVersion, objectVersion)
			metadata, err := reader.ReadAllMeta(ctx, mp)
			require.NoError(t, err)
			data := metadata.MustDataMeta()
			require.Equal(t, sourceKeySeq, data.BlockHeader().SortKey())
			block := data.GetBlockMeta(0)
			require.Equal(t, uint8(types.T_varchar), block.ColumnMeta(sourceKeySeq).DataType())
			require.Equal(t, uint8(types.T_varchar), block.ColumnMeta(targetKeySeq).DataType())
			dump := filepath.Join(t.TempDir(), "full_width")
			m0Exec(t, ctx, db, "dump table "+schema+".full_src to '"+dump+"' metadata only")
			_, err = db.ExecContext(ctx, "load table "+schema+".full_dst from '"+dump+"'")
			require.ErrorContains(t, err, "M0: physical table LOAD cannot join an evolution contract")
			checkDB := openM0SQL(t, cn)
			defer checkDB.Close()
			require.Zero(t, m0Count(t, ctx, checkDB, "select count(*) from "+schema+".full_dst"))
			require.Empty(t, m0ObjectFingerprints(t, ctx, checkDB, schema+".full_dst"))
		})
	})
}
