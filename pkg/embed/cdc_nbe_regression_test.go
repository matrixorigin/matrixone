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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestCDCSinkSessionStripsNBEAndPreservesTextJSONBit executes the real
// makeMysqlConfig/OpenDbConn/Executor/CDCStatementBuilder pipeline against a
// live MatrixOne server whose GLOBAL sql_mode enables NO_BACKSLASH_ESCAPES.
// It independently verifies the resulting @@SESSION.sql_mode, byte-for-byte
// text/JSON/BIT writes (INSERT/ODKU), and BIT primary-key DELETEs.
func TestCDCSinkSessionStripsNBEAndPreservesTextJSONBit(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := int(cn.GetServiceConfig().CN.Frontend.Port)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()

		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()

		var origGlobalMode string
		require.NoError(t, admin.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_mode").Scan(&origGlobalMode))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			escaped := strings.ReplaceAll(origGlobalMode, "'", "''")
			_, restoreErr := admin.ExecContext(cleanupCtx, fmt.Sprintf("SET GLOBAL sql_mode = '%s'", escaped))
			require.NoError(t, restoreErr)
		}()

		modeCases := []struct {
			name       string
			globalMode string
			wantMode   string
		}{
			{
				name:       "nbe_first",
				globalMode: "NO_BACKSLASH_ESCAPES,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
				wantMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
			},
			{
				name:       "nbe_middle",
				globalMode: "IGNORE_SPACE,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES",
				wantMode:   "IGNORE_SPACE,STRICT_TRANS_TABLES",
			},
			{
				name:       "nbe_last",
				globalMode: "ALLOW_INVALID_DATES,IGNORE_SPACE,NO_BACKSLASH_ESCAPES",
				wantMode:   "ALLOW_INVALID_DATES,IGNORE_SPACE",
			},
			{
				name:       "nbe_only",
				globalMode: "NO_BACKSLASH_ESCAPES",
				wantMode:   "",
			},
			{
				name:       "without_nbe",
				globalMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
				wantMode:   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
			},
		}

		for _, tc := range modeCases {
			t.Run(tc.name, func(t *testing.T) {
				_, execErr := admin.ExecContext(ctx, fmt.Sprintf("SET GLOBAL sql_mode = '%s'", tc.globalMode))
				require.NoError(t, execErr)

				// Control: an unconfigured connection actually receives GLOBAL sql_mode.
				raw, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
				require.NoError(t, openErr)
				var rawMode string
				require.NoError(t, raw.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&rawMode))
				require.NoError(t, raw.Close())
				require.Equal(t, tc.globalMode, rawMode)

				// Target: CDC sink connection strips NO_BACKSLASH_ESCAPES and preserves everything else.
				sinkDB, openErr := cdc.OpenDbConn(ctx, "dump", "111", "127.0.0.1", port, "10s")
				require.NoError(t, openErr)
				defer sinkDB.Close()

				var sinkMode string
				require.NoError(t, sinkDB.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&sinkMode))
				require.Equal(t, tc.wantMode, sinkMode)
			})
		}

		// End-to-end DML verification under NO_BACKSLASH_ESCAPES + STRICT_TRANS_TABLES.
		_, err = admin.ExecContext(ctx, "SET GLOBAL sql_mode = 'NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,ONLY_FULL_GROUP_BY'")
		require.NoError(t, err)

		_, err = admin.ExecContext(ctx, "CREATE DATABASE cdc_nbe_e2e")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, dropErr := admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS cdc_nbe_e2e")
			require.NoError(t, dropErr)
		}()
		_, err = admin.ExecContext(ctx, `
			CREATE TABLE cdc_nbe_e2e.t_single (
				id BIT(16) PRIMARY KEY,
				b1 BIT(1),
				b5 BIT(5),
				b8 BIT(8),
				b64 BIT(64),
				k_txt VARCHAR(100),
				v TEXT,
				j JSON
			)`)
		require.NoError(t, err)
		_, err = admin.ExecContext(ctx, `
			CREATE TABLE cdc_nbe_e2e.t_comp (
				bpk BIT(16),
				spk VARCHAR(100),
				v TEXT,
				PRIMARY KEY (bpk, spk)
			)`)
		require.NoError(t, err)

		exec, err := cdc.NewExecutor(ctx, "dump", "111", "127.0.0.1", port, 0, 0, "10s", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, exec.Close())
		}()

		mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
		require.NoError(t, err)
		defer func() {
			require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB())
			mpool.DeleteMPool(mp)
		}()

		singleDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_bit), Width: 16}},
				{Name: "b1", Typ: plan.Type{Id: int32(types.T_bit), Width: 1}},
				{Name: "b5", Typ: plan.Type{Id: int32(types.T_bit), Width: 5}},
				{Name: "b8", Typ: plan.Type{Id: int32(types.T_bit), Width: 8}},
				{Name: "b64", Typ: plan.Type{Id: int32(types.T_bit), Width: 64}},
				{Name: "k_txt", Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Name: "v", Typ: plan.Type{Id: int32(types.T_text)}},
				{Name: "j", Typ: plan.Type{Id: int32(types.T_json)}},
			},
			Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{
				"id": 0, "b1": 1, "b5": 2, "b8": 3, "b64": 4, "k_txt": 5, "v": 6, "j": 7,
			},
		}
		singleBuilder, err := cdc.NewCDCStatementBuilder("cdc_nbe_e2e", "t_single", singleDef, 1<<20, false)
		require.NoError(t, err)

		type singleRow struct {
			id   uint64
			b1   uint64
			b5   uint64
			b8   uint64
			b64  uint64
			kTxt string
			v    string
			jRaw string
		}
		wantSingle := []singleRow{
			{
				id:   0x005c,
				b1:   0x0,
				b5:   0x01,
				b8:   0x27,
				b64:  0x0000000000000001,
				kTxt: `a'b`,
				v:    `quote'value`,
				jRaw: `{"a": "quote'val"}`,
			},
			{
				id:   0x5c6e,
				b1:   0x1,
				b5:   0x1f,
				b8:   0x5c,
				b64:  0x5c27000affffffff,
				kTxt: `back\slash`,
				v:    `path\to\dir 'q' 中文`,
				jRaw: `{"k": "x\"y\\z'w"}`,
			},
		}

		insBat := batch.NewWithSize(8)
		insBat.Vecs[0] = vector.NewVec(types.New(types.T_bit, 16, 0), mp)
		insBat.Vecs[1] = vector.NewVec(types.New(types.T_bit, 1, 0), mp)
		insBat.Vecs[2] = vector.NewVec(types.New(types.T_bit, 5, 0), mp)
		insBat.Vecs[3] = vector.NewVec(types.New(types.T_bit, 8, 0), mp)
		insBat.Vecs[4] = vector.NewVec(types.New(types.T_bit, 64, 0), mp)
		insBat.Vecs[5] = vector.NewVec(types.T_varchar.ToType(), mp)
		insBat.Vecs[6] = vector.NewVec(types.T_text.ToType(), mp)
		insBat.Vecs[7] = vector.NewVec(types.T_json.ToType(), mp)
		func() {
			defer insBat.Clean(mp)
			for _, r := range wantSingle {
				require.NoError(t, vector.AppendFixed(insBat.Vecs[0], r.id, false, mp))
				require.NoError(t, vector.AppendFixed(insBat.Vecs[1], r.b1, false, mp))
				require.NoError(t, vector.AppendFixed(insBat.Vecs[2], r.b5, false, mp))
				require.NoError(t, vector.AppendFixed(insBat.Vecs[3], r.b8, false, mp))
				require.NoError(t, vector.AppendFixed(insBat.Vecs[4], r.b64, false, mp))
				require.NoError(t, vector.AppendBytes(insBat.Vecs[5], []byte(r.kTxt), false, mp))
				require.NoError(t, vector.AppendBytes(insBat.Vecs[6], []byte(r.v), false, mp))
				bj, jErr := types.ParseStringToByteJson(r.jRaw)
				require.NoError(t, jErr)
				jb, jErr := bj.Marshal()
				require.NoError(t, jErr)
				require.NoError(t, vector.AppendBytes(insBat.Vecs[7], jb, false, mp))
			}
			insBat.SetRowCount(len(wantSingle))

			fromTS := types.BuildTS(100, 0)
			toTS := types.BuildTS(200, 0)
			sqls, buildErr := singleBuilder.BuildInsertSQL(ctx, insBat, fromTS, toTS)
			require.NoError(t, buildErr)
			require.NotEmpty(t, sqls)
			for _, s := range sqls {
				require.NoError(t, exec.ExecSQL(ctx, nil, s, false))
			}
		}()

		// Read back from independent admin connection.
		func() {
			rows, queryErr := admin.QueryContext(ctx, "SELECT id, b1, b5, b8, b64, k_txt, v, CAST(j AS CHAR) FROM cdc_nbe_e2e.t_single ORDER BY id")
			require.NoError(t, queryErr)
			defer rows.Close()
			var gotIdx int
			for rows.Next() {
				var id, b1, b5, b8, b64 []byte
				var kTxt, v, jStr string
				require.NoError(t, rows.Scan(&id, &b1, &b5, &b8, &b64, &kTxt, &v, &jStr))
				w := wantSingle[gotIdx]
				require.Equal(t, fmt.Sprintf("%04x", w.id), fmt.Sprintf("%x", id))
				require.Equal(t, w.b1, decodeBigEndianBit(b1))
				require.Equal(t, w.b5, decodeBigEndianBit(b5))
				require.Equal(t, w.b8, decodeBigEndianBit(b8))
				require.Equal(t, w.b64, decodeBigEndianBit(b64))
				require.Equal(t, w.kTxt, kTxt)
				require.Equal(t, w.v, v)
				wantBJ, jErr := types.ParseStringToByteJson(w.jRaw)
				require.NoError(t, jErr)
				gotBJ, jErr := types.ParseStringToByteJson(jStr)
				require.NoError(t, jErr)
				require.Equal(t, wantBJ.String(), gotBJ.String())
				gotIdx++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, len(wantSingle), gotIdx)
		}()

		// Delete single BIT PK rows through CDCStatementBuilder + Executor.
		func() {
			delBat := batch.NewWithSize(2)
			delBat.Vecs[0] = vector.NewVec(types.New(types.T_bit, 16, 0), mp)
			delBat.Vecs[1] = vector.NewVec(types.T_TS.ToType(), mp)
			owned := true
			defer func() {
				if owned {
					delBat.Clean(mp)
				}
			}()
			ts := types.BuildTS(300, 0)
			for _, r := range wantSingle {
				require.NoError(t, vector.AppendFixed(delBat.Vecs[0], r.id, false, mp))
				require.NoError(t, vector.AppendFixed(delBat.Vecs[1], ts, false, mp))
			}
			delBat.SetRowCount(len(wantSingle))
			atomic := cdc.NewAtomicBatch(mp)
			defer atomic.Close()
			packer := types.NewPacker()
			defer packer.Close()
			atomic.Append(packer, delBat, 1, 0)
			owned = false

			delSQLs, buildErr := singleBuilder.BuildDeleteSQL(ctx, atomic, ts, types.BuildTS(400, 0))
			require.NoError(t, buildErr)
			for _, s := range delSQLs {
				require.NoError(t, exec.ExecSQL(ctx, nil, s, false))
			}
		}()

		var remaining int
		require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_nbe_e2e.t_single").Scan(&remaining))
		require.Zero(t, remaining)

		// Composite (BIT(16), VARCHAR) PK INSERT + DELETE.
		compDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "bpk", Typ: plan.Type{Id: int32(types.T_bit), Width: 16}},
				{Name: "spk", Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Name: "v", Typ: plan.Type{Id: int32(types.T_text)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"bpk", "spk"}},
			Name2ColIndex: map[string]int32{"bpk": 0, "spk": 1, "v": 2},
		}
		compBuilder, err := cdc.NewCDCStatementBuilder("cdc_nbe_e2e", "t_comp", compDef, 1<<20, false)
		require.NoError(t, err)

		compRows := []struct {
			bpk uint64
			spk string
			v   string
		}{
			{bpk: 0x005c, spk: `back\slash`, v: `v\1`},
			{bpk: 0x5c6e, spk: `a'b`, v: `v'2`},
		}
		func() {
			insComp := batch.NewWithSize(3)
			insComp.Vecs[0] = vector.NewVec(types.New(types.T_bit, 16, 0), mp)
			insComp.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
			insComp.Vecs[2] = vector.NewVec(types.T_text.ToType(), mp)
			defer insComp.Clean(mp)
			for _, r := range compRows {
				require.NoError(t, vector.AppendFixed(insComp.Vecs[0], r.bpk, false, mp))
				require.NoError(t, vector.AppendBytes(insComp.Vecs[1], []byte(r.spk), false, mp))
				require.NoError(t, vector.AppendBytes(insComp.Vecs[2], []byte(r.v), false, mp))
			}
			insComp.SetRowCount(len(compRows))
			sqls, buildErr := compBuilder.BuildInsertSQL(ctx, insComp, types.BuildTS(500, 0), types.BuildTS(600, 0))
			require.NoError(t, buildErr)
			for _, s := range sqls {
				require.NoError(t, exec.ExecSQL(ctx, nil, s, false))
			}
		}()

		var compCount int
		require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_nbe_e2e.t_comp").Scan(&compCount))
		require.Equal(t, len(compRows), compCount)

		func() {
			delComp := batch.NewWithSize(2)
			delComp.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
			delComp.Vecs[1] = vector.NewVec(types.T_TS.ToType(), mp)
			owned := true
			defer func() {
				if owned {
					delComp.Clean(mp)
				}
			}()
			packer := types.NewPacker()
			defer packer.Close()
			ts := types.BuildTS(700, 0)
			for _, r := range compRows {
				packer.Reset()
				packer.EncodeBit(r.bpk)
				packer.EncodeStringType([]byte(r.spk))
				require.NoError(t, vector.AppendBytes(delComp.Vecs[0], packer.GetBuf(), false, mp))
				require.NoError(t, vector.AppendFixed(delComp.Vecs[1], ts, false, mp))
			}
			delComp.SetRowCount(len(compRows))
			atomic := cdc.NewAtomicBatch(mp)
			defer atomic.Close()
			atomic.Append(packer, delComp, 1, 0)
			owned = false

			delSQLs, buildErr := compBuilder.BuildDeleteSQL(ctx, atomic, ts, types.BuildTS(800, 0))
			require.NoError(t, buildErr)
			for _, s := range delSQLs {
				require.NoError(t, exec.ExecSQL(ctx, nil, s, false))
			}
		}()

		require.NoError(t, admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc_nbe_e2e.t_comp").Scan(&compCount))
		require.Zero(t, compCount)
	})
}

func decodeBigEndianBit(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = (v << 8) | uint64(x)
	}
	return v
}
