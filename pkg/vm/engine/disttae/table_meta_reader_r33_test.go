// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

// TestIssue25590UnboundMaterializingReadersSkipNilOption pins the
// round-33 pre-PR review's finding 1 against design r11's recorded
// forward-compat stance: a TableMetaReader built WITHOUT a sinker
// option - the exported constructor's defaulted form, or an explicitly
// nil option a workspace without the owner surface resolves to - keeps
// its pre-fix HOOKLESS materializing writers. The r32 plumbing passed
// r.sinkerOption to BOTH writer families unconditionally, so a nil
// option rode into ioutil.NewSinker's option loop, which calls every
// element without a nil check: an unbound reader panicked entering
// EITHER reader phase, before any row knew whether it needed
// materialization - so a clone whose proc carries no txn operator (a
// workspace without the SinkerOwner surface) could not read at all.
// The regression constructs its readers through the ACTUAL exported
// constructor - the omitted-option entry the two-argument call leaves
// defaulted, and an explicit nil alike - drives BOTH phases (the data
// family of collectDataOfAObjsAndInMem and the tombstone family of
// collectTombstoneOfAObjsAndInMem), and asserts the hookless contract:
// no panic, each phase materializes exactly its fresh object, and the
// admission ledger is never touched. A bound control constructed the
// same way but carrying the recording transaction's option retains the
// bound-writer assertions: its drive reserves the fresh name before
// durability and the record choke converts it.

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// r33RecoverGuarded runs one reader-phase drive and converts a panic
// anywhere inside it - the pre-fix nil-option call inside
// ioutil.NewSinker's option loop panics at writer construction - into
// a hard test failure, so the red run reports the discriminant instead
// of crashing the test binary.
func r33RecoverGuarded(drive func() error, where string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(
				"%s panicked (the nil sinker option reached the sinker's option loop): %v",
				where, r)
		}
	}()
	return drive()
}

func TestIssue25590UnboundMaterializingReadersSkipNilOption(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()

	// The spill-at-Sync fault keeps every drive's materialization
	// deterministic: exactly one fresh object per phase drive.
	disarmSmallObjs := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarmSmallObjs()

	// One real readable source object, written by an UNHOOKED writer -
	// the committed source state owns it.
	srcRows := batch.NewWithSize(3)
	srcRows.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
	srcRowids := make([]types.Rowid, 16)
	srcPKs := make([]int64, 16)
	for i := range srcRowids {
		srcRowids[i] = types.RandomRowid()
		srcPKs[i] = int64(i + 1)
	}
	rv := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rv, srcRowids, nil, mp))
	av := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(av, srcPKs, nil, mp))
	vv := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vv, srcPKs, nil, mp))
	srcRows.Vecs[0] = rv
	srcRows.Vecs[1] = av
	srcRows.Vecs[2] = vv
	srcRows.SetRowCount(16)
	srcWriter := colexec.NewCNS3DataWriterForService(
		txn.proc.GetService(), mp, failFS, tbl.tableDef, -1, false)
	require.NoError(t, srcWriter.Write(ctx, srcRows))
	srcStatsList, err := srcWriter.Sync(ctx)
	require.NoError(t, err)
	srcWriter.Close()
	srcRows.Clean(mp)
	require.Len(t, srcStatsList, 1, "the source materializes exactly one object")
	srcStats := srcStatsList[0]
	require.Equal(t, 1, len(failFS.persistedSnapshot()),
		"before the drives only the source object exists")

	// One workspace DELETE entry below every reader's boundary, so the
	// tombstone phase materializes it exactly the production clone
	// path does.
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	delRowids := make([]types.Rowid, 16)
	delPKs := make([]int64, 16)
	for i := range delRowids {
		delRowids[i] = types.RandomRowid()
		delPKs[i] = int64(i + 1)
	}
	drv := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(drv, delRowids, nil, mp))
	dpv := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(dpv, delPKs, nil, mp))
	delBat.Vecs[0] = drv
	delBat.Vecs[1] = dpv
	delBat.SetRowCount(16)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          DELETE,
		accountId:    1,
		databaseId:   tbl.db.databaseId,
		tableId:      tbl.tableId,
		databaseName: "db",
		tableName:    tbl.tableName,
		bat:          delBat,
	})

	// The ACTUAL exported constructor - entering here, not a struct
	// literal - resolves the reader's partition state through the
	// created-in-txn fast path, so register the fixture table exactly
	// the production CREATE TABLE flow does and give the bare fixture
	// engine its partitions map.
	txn.tableOps.addCreatedInTxn(tbl.tableId, 0)
	if e.partitions == nil {
		e.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	}

	// The ACTUAL exported constructor: omitted options and an explicit
	// nil alike build the reader whose sinkerOption stays nil.
	actualReader := func(opts ...ioutil.SinkerOption) *TableMetaReader {
		t.Helper()
		raw, cerr := NewTableMetaReader(ctx, tbl, opts...)
		require.NoError(t, cerr,
			"the exported constructor must succeed on the fixture source table")
		rd, ok := raw.(*TableMetaReader)
		require.True(t, ok, "NewTableMetaReader returns the concrete reader")
		return rd
	}

	seqnums, colTypes, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
	objRelData := readutil.ObjListRelData{}
	objRelData.AppendObj(&srcStats)

	persistedBefore := len(failFS.persistedSnapshot())
	variants := []struct {
		name  string
		build func() *TableMetaReader
	}{
		{
			name:  "constructor-with-omitted-option",
			build: func() *TableMetaReader { return actualReader() },
		},
		{
			name:  "constructor-with-explicit-nil-option",
			build: func() *TableMetaReader { return actualReader(nil) },
		},
	}
	for _, variant := range variants {
		reader := variant.build()

		// The data phase drives the data-materializing family.
		dataOut := colexec.AllocCNS3ResultBat(false)
		driveErr := r33RecoverGuarded(func() error {
			_, derr := reader.collectDataOfAObjsAndInMem(
				ctx, mp, dataOut, seqnums, attrs, colTypes, objRelData)
			return derr
		}, variant.name+"/data-phase")
		require.NoError(t, driveErr,
			"%s: the data phase must keep its pre-fix HOOKLESS writer - a nil option may never reach the sinker's option loop",
			variant.name)
		require.Equal(t, persistedBefore+1, len(failFS.persistedSnapshot()),
			"%s: the hookless data family still materialized exactly one fresh object",
			variant.name)
		matStats := r32FirstOutBatchStats(t, dataOut)
		matName := matStats.ObjectName().String()
		require.NotEqual(t, srcStats.ObjectName().String(), matName,
			"%s: the materialized object is a FRESH object, not the source's name",
			variant.name)
		dataOut.Clean(mp)

		// The tombstone phase drives the tombstone-materializing family.
		tombOut := colexec.AllocCNS3ResultBat(true)
		tombErr := r33RecoverGuarded(func() error {
			_, terr := reader.collectTombstoneOfAObjsAndInMem(
				ctx, mp, tombOut,
				[]uint16{0, 1},
				objectio.TombstoneAttrs_CN_Created,
				[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()},
				readutil.ObjListRelData{},
			)
			return terr
		}, variant.name+"/tombstone-phase")
		require.NoError(t, tombErr,
			"%s: the tombstone phase must keep its pre-fix HOOKLESS writer - a nil option may never reach the sinker's option loop",
			variant.name)
		require.Equal(t, persistedBefore+2, len(failFS.persistedSnapshot()),
			"%s: the hookless tombstone family still materialized exactly one fresh object",
			variant.name)
		tombOut.Clean(mp)

		// The hookless contract: the drives never touched the admission
		// ledger nor the transaction's ownership surfaces.
		pending, reserved := txn.UnsettledCleanupOwnershipForTest()
		require.Empty(t, pending, "%s: nothing joined pending cleanup", variant.name)
		require.Empty(t, reserved,
			"%s: the hookless writers reserved nothing - the unbound reader keeps the pre-fix behavior",
			variant.name)
		queued, live, inFlight := e.InspectUnpublishedPendingForTest()
		require.Empty(t, queued, "%s: the engine queue holds nothing", variant.name)
		require.Zero(t, live, "%s: nothing entered live ownership", variant.name)
		require.Zero(t, inFlight, "%s: no in-flight unit was booked", variant.name)

		persistedBefore = len(failFS.persistedSnapshot())
	}

	// The bound control - the SAME actual constructor carrying the
	// recording transaction's option - retains the bound-writer
	// assertions: the drive reserves the fresh name BEFORE durability
	// and the record choke converts it.
	boundReader := actualReader(txn.SinkerCapacityOption())
	boundOut := colexec.AllocCNS3ResultBat(false)
	_, err = boundReader.collectDataOfAObjsAndInMem(
		ctx, mp, boundOut, seqnums, attrs, colTypes, objRelData)
	require.NoError(t, err,
		"the bound reader's data family still materializes under the recording transaction's option")
	matStats := r32FirstOutBatchStats(t, boundOut)
	boundName := matStats.ObjectName().String()
	boundOut.Clean(mp)
	require.Equal(t, persistedBefore+1, len(failFS.persistedSnapshot()),
		"the bound data family materialized exactly one fresh object")
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, pending)
	require.Equal(t, []string{boundName}, reserved,
		"the bound family reserved its materialized name BEFORE the object went durable")
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, live)
	require.Equal(t, 1, inFlight,
		"the bound family's materialization holds exactly one in-flight unit")

	// The record choke converts the bound reservation, settling the
	// ledger exactly the production destination Write does.
	recBat := colexec.AllocCNS3ResultBat(false)
	defer recBat.Clean(mp)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		mp, false, recBat, false, matStats))
	require.NoError(t, txn.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		boundName, recBat, txn.tnStores[0]))
	_, reserved = txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, reserved, "the record converted the bound reservation")
	_, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Zero(t, inFlight)
	require.Equal(t, 1, live,
		"the converted reservation became the recorded object's owned membership")
}
