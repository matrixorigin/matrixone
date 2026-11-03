package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Exercise every ordered deletion set, including empty, prefix, suffix, and all rows.
func TestShrinkNegateOrderedSubsets(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for n := 0; n <= 7; n++ {
		for mask := 0; mask < 1<<n; mask++ {
			func() {
				v := NewVec(types.T_int64.ToType())
				defer v.Free(mp)
				values := make([]int64, n)
				sels := make([]int64, 0, n)
				want := make([]int64, 0, n)
				for i := range values {
					values[i] = int64(i + 10)
					if mask&(1<<i) != 0 {
						sels = append(sels, int64(i))
					} else {
						want = append(want, values[i])
					}
				}
				require.NoError(t, AppendFixedList(v, values, nil, mp), "n=%d mask=%d", n, mask)
				require.NoError(t, v.Shrink(sels, true, mp), "n=%d mask=%d", n, mask)
				got := MustFixedColNoTypeCheck[int64](v)
				require.Equal(t, len(want), len(got), "n=%d mask=%d", n, mask)
				for i := range want {
					require.Equal(t, want[i], got[i], "n=%d mask=%d index=%d", n, mask, i)
				}
			}()
		}
	}
	require.Zero(t, mp.CurrNB())
}
