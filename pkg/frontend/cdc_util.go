// Copyright 2023 Matrix Origin
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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func WithBackgroundExec(
	ctx context.Context,
	ses *Session,
	fn func(context.Context, *Session, BackgroundExec) error,
) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	return fn(ctx, ses, bh)
}

var checkPitr = func(ctx context.Context, bh BackgroundExec, accName string, pts *cdc.PatternTuples) error {
	// TODO min length
	minPitrLen := int64(2)
	checkPitrByLevel := func(level, dbName, tblName string) (bool, error) {
		length, unit, ok, err := getPitrLengthAndUnit(ctx, bh, level, accName, dbName, tblName)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		return !(unit == "h" && length < minPitrLen), nil
	}

	if ok, err := checkPitrByLevel(cdc.CDCPitrGranularity_Cluster, "", ""); err != nil {
		return err
	} else if ok {
		// covered by cluster level pitr
		return nil
	}

	for _, pt := range pts.Pts {
		dbName := pt.Source.Database
		tblName := pt.Source.Table
		level := cdc.CDCPitrGranularity_Table
		if dbName == cdc.CDCPitrGranularity_All && tblName == cdc.CDCPitrGranularity_All { // account level
			level = cdc.CDCPitrGranularity_Account
		} else if tblName == cdc.CDCPitrGranularity_All { // db level
			level = cdc.CDCPitrGranularity_DB
		}

		if ok, err := checkPitrByLevel(cdc.CDCPitrGranularity_Account, dbName, tblName); err != nil {
			return err
		} else if ok {
			// covered by account level pitr
			continue
		}

		if level == cdc.CDCPitrGranularity_DB || level == cdc.CDCPitrGranularity_Table {
			if ok, err := checkPitrByLevel(cdc.CDCPitrGranularity_DB, dbName, tblName); err != nil {
				return err
			} else if ok {
				// covered by db level pitr
				continue
			}
		}

		if level == cdc.CDCPitrGranularity_Table {
			if ok, err := checkPitrByLevel(cdc.CDCPitrGranularity_Table, dbName, tblName); err != nil {
				return err
			} else if ok {
				// covered by table level pitr
				continue
			}
		}

		return moerr.NewInternalErrorf(ctx, "no account/db/table level pitr with enough length found for pattern: %s, min pitr length: %d h", pt.OriginString, minPitrLen)
	}
	return nil
}
