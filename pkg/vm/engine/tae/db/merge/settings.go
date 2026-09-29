// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/docker/go-units"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

const (
	MergeSettingsVersion_Curr = 0
)

var mergeSettingsDecoders = map[uint32]func(bs []byte) (*MergeSettings, error){
	MergeSettingsVersion_Curr: func(bs []byte) (*MergeSettings, error) {
		var settings MergeSettings
		byteJson := types.DecodeJson(bs)
		normal, err := byteJson.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(normal, &settings); err != nil {
			return nil, err
		}
		if settings.LNMinPointDepthPerCluster == 0 {
			return nil, moerr.NewInternalErrorNoCtxf("probable corrupted merge settings")
		}
		return &settings, nil
	},
}

func DecodeMergeSettings(version uint32, bs []byte) (*MergeSettings, error) {
	decoder, ok := mergeSettingsDecoders[version]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("unknown merge settings version: %d", version)
	}
	return decoder(bs)
}

func DecodeMergeSettingsBatchAnd(
	bat *batch.Batch,
	fn func(tid uint64, setting *MergeSettings),
) {
	// 0 account id
	// 1 table id
	// 2 version
	// 3 settings
	// 4 extra_info
	tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[1])
	versions := vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[2])

	for i, tid := range tids {
		bs := bat.Vecs[3].GetBytesAt(i)
		version := versions[i]
		setting, err := DecodeMergeSettings(version, bs)
		if err != nil {
			logutil.Error("MergeExecutorEvent",
				zap.String("event", "decode settings"),
				zap.String("table", fmt.Sprintf("%d", tid)),
				zap.Error(err),
				zap.String("settings", string(bs)),
			)
			// The row still supersedes an older committed setting. Deliver a
			// nil trigger so a late callback cannot restore stale config.
			fn(tid, nil)
			continue
		}
		fn(tid, setting)
	}
}

type MergeSettings struct {
	VacuumTopK               int    `json:"vacuum-top-k"`
	VacuumScoreStart         int    `json:"vacuum-score-start"`
	VacuumScoreEnd           int    `json:"vacuum-score-end"`
	VacuumScoreDecayDuration string `json:"vacuum-score-decay-duration"`

	L0MaxCountStart         int       `json:"l0-max-count-start"`
	L0MaxCountEnd           int       `json:"l0-max-count-end"`
	L0MaxCountDecayDuration string    `json:"l0-max-count-decay-duration"`
	L0MaxCountDecayControl  []float64 `json:"l0-max-count-decay-control"`

	LNStartLv                 int `json:"ln-start-lv"`
	LNEndLv                   int `json:"ln-end-lv"`
	LNMinPointDepthPerCluster int `json:"ln-min-point-depth-per-cluster"`

	TombstoneL1Size  string `json:"tombstone-l1-size"`
	TombstoneL1Count int    `json:"tombstone-l1-count"`
	TombstoneL2Count int    `json:"tombstone-l2-count"`
}

var DefaultMergeSettings = &MergeSettings{
	VacuumTopK:               DefaultVacuumOpts.HollowTopK,
	VacuumScoreStart:         DefaultVacuumOpts.StartScore,
	VacuumScoreEnd:           DefaultVacuumOpts.EndScore,
	VacuumScoreDecayDuration: DefaultVacuumOpts.Duration.Round(time.Second).String(),

	L0MaxCountStart:         DefaultLayerZeroOpts.Start,
	L0MaxCountEnd:           DefaultLayerZeroOpts.End,
	L0MaxCountDecayDuration: DefaultLayerZeroOpts.Duration.Round(time.Second).String(),
	L0MaxCountDecayControl:  slices.Clone(DefaultLayerZeroOpts.CPoints[:]),

	LNStartLv:                 1,
	LNEndLv:                   7,
	LNMinPointDepthPerCluster: DefaultOverlapOpts.MinPointDepthPerCluster,

	TombstoneL1Size:  units.BytesSize(float64(DefaultTombstoneOpts.L1Size)),
	TombstoneL1Count: DefaultTombstoneOpts.L1Count,
	TombstoneL2Count: DefaultTombstoneOpts.L2Count,
}

func (s *MergeSettings) Clone() *MergeSettings {
	bs, _ := json.Marshal(s)
	var settings MergeSettings
	_ = json.Unmarshal(bs, &settings)
	return &settings
}

func (s *MergeSettings) String() string {
	bs, _ := json.Marshal(s)
	return string(bs)
}

func (s *MergeSettings) ToMMsgTaskTrigger() (*MMsgTaskTrigger, error) {
	if len(s.L0MaxCountDecayControl) != 4 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"merge settings l0 decay control needs 4 points, got %d",
			len(s.L0MaxCountDecayControl))
	}
	l0MaxCountDecayDuration, err := time.ParseDuration(s.L0MaxCountDecayDuration)
	if err != nil {
		return nil, err
	}
	vacuumScoreDecayDuration, err := time.ParseDuration(s.VacuumScoreDecayDuration)
	if err != nil {
		return nil, err
	}
	tombstoneL1Size, err := units.RAMInBytes(s.TombstoneL1Size)
	if err != nil {
		return nil, err
	}
	return NewMMsgTaskTrigger(nil).
		WithL0(NewLayerZeroOpts().WithToleranceDegressionCurve(
			s.L0MaxCountStart, s.L0MaxCountEnd, l0MaxCountDecayDuration,
			[4]float64{
				s.L0MaxCountDecayControl[0],
				s.L0MaxCountDecayControl[1],
				s.L0MaxCountDecayControl[2],
				s.L0MaxCountDecayControl[3],
			})).
		WithLn(s.LNStartLv, s.LNEndLv, NewOverlapOptions().
			WithMinPointDepthPerCluster(s.LNMinPointDepthPerCluster)).
		WithTombstone(NewTombstoneOpts().
			WithL1(int(tombstoneL1Size), s.TombstoneL1Count).
			WithL2Count(s.TombstoneL2Count)).
		WithVacuumCheck(NewVacuumOpts().
			WithHollowTopK(s.VacuumTopK).
			WithStartScore(s.VacuumScoreStart).
			WithEndScore(s.VacuumScoreEnd).
			WithDuration(vacuumScoreDecayDuration)), nil
}

type TNCatalogEventSource struct {
	*catalog.Catalog
	*txnbase.TxnManager
}

func (c *TNCatalogEventSource) GetMergeSettingsBatchFn() func() (*batch.Batch, func()) {
	db, err := c.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return nil
	}
	ts := c.Now()
	if maxCommitted := c.MaxCommittedTS.Load(); ts.LE(maxCommitted) {
		ts = maxCommitted.Next()
	}
	txn := c.OpenOfflineTxn(ts)
	entry, err := db.GetTableEntryByName(0, pkgcatalog.MO_MERGE_SETTINGS, txn)
	if err != nil {
		return nil
	}
	return func() (*batch.Batch, func()) {
		bat := tables.ReadMergeSettingsBatch(context.Background(), entry, txn)
		if bat == nil {
			return nil, nil
		}
		cnBat := containers.ToCNBatch(bat)
		return cnBat, func() {
			bat.Close()
		}
	}
}

// ReadPromotionSettings is used only after replay has joined. It does not
// change the normal startup bootstrap path or turn a failed scan into defaults.
func (c *TNCatalogEventSource) ReadPromotionSettings(
	ctx context.Context,
) (settings map[uint64]*MMsgTaskTrigger, err error) {
	db, err := c.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return nil, err
	}
	ts := c.Now()
	if maxCommitted := c.MaxCommittedTS.Load(); ts.LE(maxCommitted) {
		ts = maxCommitted.Next()
	}
	txn := c.OpenOfflineTxn(ts)
	defer func() { err = errors.Join(err, txn.GetStore().Close()) }()
	entry, err := db.GetTableEntryByName(0, pkgcatalog.MO_MERGE_SETTINGS, txn)
	if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		return map[uint64]*MMsgTaskTrigger{}, nil
	}
	if err != nil {
		return nil, err
	}
	bat, err := tables.ReadMergeSettingsBatchForPromotion(ctx, entry, txn)
	if err != nil {
		return nil, err
	}
	if bat == nil {
		return map[uint64]*MMsgTaskTrigger{}, nil
	}
	defer bat.Close()
	cnBat := containers.ToCNBatch(bat)
	tids := vector.MustFixedColNoTypeCheck[uint64](cnBat.Vecs[1])
	versions := vector.MustFixedColNoTypeCheck[uint32](cnBat.Vecs[2])
	settings = make(map[uint64]*MMsgTaskTrigger, len(tids))
	for i, tid := range tids {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := settings[tid]; exists {
			return nil, moerr.NewInternalErrorNoCtxf("duplicate merge settings for table %d", tid)
		}
		setting, decodeErr := DecodeMergeSettings(versions[i], cnBat.Vecs[3].GetBytesAt(i))
		if decodeErr != nil {
			return nil, decodeErr
		}
		trigger, decodeErr := setting.toPromotionTrigger()
		if decodeErr != nil {
			return nil, decodeErr
		}
		settings[tid] = trigger
	}
	return settings, nil
}

// Each tombstone count becomes a pointer-slice capacity on every gather. Two
// slices at this limit preallocate at most 1 MiB together on 64-bit targets.
const maxPromotionTombstoneCount = 1 << 16

// toPromotionTrigger validates the domains consumed by scheduling before the
// one-way promotion starts workers. Normal startup/config parsing is unchanged.
func (s *MergeSettings) toPromotionTrigger() (*MMsgTaskTrigger, error) {
	if len(s.L0MaxCountDecayControl) != 4 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid merge settings decay points: %d", len(s.L0MaxCountDecayControl))
	}
	if s.TombstoneL1Count <= 0 || s.TombstoneL1Count > maxPromotionTombstoneCount ||
		s.TombstoneL2Count <= 0 || s.TombstoneL2Count > maxPromotionTombstoneCount ||
		s.LNMinPointDepthPerCluster <= 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid merge settings counts: tombstone L1=%d L2=%d, overlap depth=%d",
			s.TombstoneL1Count, s.TombstoneL2Count, s.LNMinPointDepthPerCluster)
	}
	trigger, err := s.ToMMsgTaskTrigger()
	if err != nil {
		return nil, err
	}
	if trigger.vacuum.Duration <= 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid merge settings vacuum duration: %s", s.VacuumScoreDecayDuration)
	}
	return trigger, nil
}
