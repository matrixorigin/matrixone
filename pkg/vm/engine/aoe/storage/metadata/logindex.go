package md

import (
	"fmt"
)

func (idx *LogIndex) String() string {
	return fmt.Sprintf("(%d,%d,%d,%d)", idx.ID, idx.Start, idx.Count, idx.Capacity)
}

func (idx *LogIndex) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}
