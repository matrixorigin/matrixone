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

package interactive

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	objectinteractive "github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
)

// Run starts the interactive checkpoint viewer
func Run(reader *checkpointtool.CheckpointReader) error {
	m := NewUnifiedModel(reader)

	for {
		p := tea.NewProgram(m, tea.WithAltScreen())
		finalModel, err := p.Run()
		if err != nil {
			return err
		}

		um, ok := finalModel.(*UnifiedModel)
		if !ok {
			return nil
		}

		// Check if we need to open an object file
		if um.GetObjectToOpen() != "" && um.GetRangeToOpen() != nil {
			rng := um.GetRangeToOpen()
			opts := &objectinteractive.ViewOptions{
				StartRow: int64(rng.Start.GetRowOffset()),
				EndRow:   int64(rng.End.GetRowOffset()),
				ColumnNames: map[uint16]string{
					0:  ckputil.TableObjectsAttr_Accout,
					1:  ckputil.TableObjectsAttr_DB,
					2:  ckputil.TableObjectsAttr_Table,
					3:  ckputil.TableObjectsAttr_ObjectType,
					4:  "object_name",
					5:  "flags",
					6:  "rows",
					7:  "osize",
					8:  "csize",
					9:  ckputil.TableObjectsAttr_CreateTS,
					10: ckputil.TableObjectsAttr_DeleteTS,
					11: ckputil.TableObjectsAttr_Cluster,
				},
				ColumnExpander: &objectinteractive.ColumnExpander{
					SourceCol: 4, // id column
					NewCols:   []string{"object_name", "flags", "rows", "osize", "csize"},
					NewTypes: []types.Type{
						types.T_varchar.ToType(),
						types.T_varchar.ToType(),
						types.T_uint32.ToType(),
						types.T_varchar.ToType(),
						types.T_varchar.ToType(),
					},
					ExpandFunc: expandObjectStats,
				},
				ObjectNameCol: 4,                    // object_name column after expansion
				BaseDir:       m.state.reader.Dir(), // Base directory for nested objects
			}
			if err := objectinteractive.RunUnified(context.Background(), um.GetObjectToOpen(), opts); err != nil {
				return err
			}
			// Clear the object to open flag and continue with current state
			um.ClearObjectToOpen()
			continue
		}

		return nil
	}
}

// expandObjectStats expands ObjectStats bytes into multiple values
func expandObjectStats(value any) []any {
	data, ok := value.([]byte)
	if !ok || len(data) != objectio.ObjectStatsLen {
		return []any{"", "", "", "", ""}
	}

	var stats objectio.ObjectStats
	stats.UnMarshal(data)

	// Format flags: A=Appendable, S=Sorted, C=CNCreated
	flags := ""
	if stats.GetAppendable() {
		flags += "A"
	}
	if stats.GetSorted() {
		flags += "S"
	}
	if stats.GetCNCreated() {
		flags += "C"
	}
	if flags == "" {
		flags = "-"
	}

	return []any{
		stats.ObjectName().String(),
		flags,
		stats.Rows(),
		formatSize(stats.OriginSize()),
		formatSize(stats.Size()),
	}
}
