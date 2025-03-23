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

// PitrConfig represents the PITR configuration for a specific level
type PitrConfig struct {
	Level  string
	Length int64
	Unit   string
	Exists bool
}

// NewPitrConfig creates a new PitrConfig instance
func NewPitrConfig(level string) *PitrConfig {
	return &PitrConfig{
		Level: level,
	}
}

// IsValid checks if the PITR configuration meets the minimum requirements
func (pc *PitrConfig) IsValid(minLength int64) bool {
	if !pc.Exists {
		return false
	}
	return !(pc.Unit == "h" && pc.Length < minLength)
}

// CDCCheckPitrGranularity checks if the PITR (Point-in-Time Recovery) granularity settings
// meet the minimum requirements for CDC tasks at different levels (cluster/account/db/table)
// It verifies the PITR configuration in descending order of priority (cluster > account > database > table)
// to ensure that at least one level satisfies the minimum time requirement (2 hours).
//
// Parameters:
// - ctx: Context for managing the lifecycle of the function.
// - bh: Background handler for executing database queries.
// - accName: The account name associated with the CDC task.
// - pts: Pattern tuples representing the database and table patterns to be checked.
//
// Returns:
// - error: Returns an error if no PITR configuration meets the minimum requirement, otherwise nil.
var CDCCheckPitrGranularity = func(
	ctx context.Context,
	bh BackgroundExec,
	accName string,
	pts *cdc.PatternTuples,
) error {
	const minPitrLen = int64(2)

	// Helper function to get PITR config for a specific level
	getPitrConfig := func(level, dbName, tblName string) (*PitrConfig, error) {
		config := NewPitrConfig(level)
		length, unit, ok, err := getPitrLengthAndUnit(ctx, bh, level, accName, dbName, tblName)
		if err != nil {
			return nil, err
		}
		config.Length = length
		config.Unit = unit
		config.Exists = ok
		return config, nil
	}

	// Check cluster level first
	if config, err := getPitrConfig(cdc.CDCPitrGranularity_Cluster, "", ""); err != nil {
		return err
	} else if config.IsValid(minPitrLen) {
		return nil
	}

	// Check other levels for each pattern tuple
	for _, pt := range pts.Pts {
		dbName := pt.Source.Database
		tblName := pt.Source.Table

		// Determine the level based on pattern
		level := cdc.CDCPitrGranularity_Table
		if dbName == cdc.CDCPitrGranularity_All && tblName == cdc.CDCPitrGranularity_All {
			level = cdc.CDCPitrGranularity_Account
		} else if tblName == cdc.CDCPitrGranularity_All {
			level = cdc.CDCPitrGranularity_DB
		}

		// Check account level
		if config, err := getPitrConfig(cdc.CDCPitrGranularity_Account, dbName, tblName); err != nil {
			return err
		} else if config.IsValid(minPitrLen) {
			continue
		}

		// Check DB level if needed
		if level == cdc.CDCPitrGranularity_DB || level == cdc.CDCPitrGranularity_Table {
			if config, err := getPitrConfig(cdc.CDCPitrGranularity_DB, dbName, tblName); err != nil {
				return err
			} else if config.IsValid(minPitrLen) {
				continue
			}
		}

		// Check table level if needed
		if level == cdc.CDCPitrGranularity_Table {
			if config, err := getPitrConfig(cdc.CDCPitrGranularity_Table, dbName, tblName); err != nil {
				return err
			} else if config.IsValid(minPitrLen) {
				continue
			}
		}

		return moerr.NewInternalErrorf(ctx,
			"no valid PITR configuration found for pattern: %s, minimum required length: %d hours",
			pt.OriginString, minPitrLen)
	}
	return nil
}
