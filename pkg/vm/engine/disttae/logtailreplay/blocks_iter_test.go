// Copyright 2024 Matrix Origin
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

package logtailreplay

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestPartitionState_CollectObjectsBetweenInProgress(t *testing.T) {
	pState := NewPartitionState("", false, 0x3fff, false)

	//       t1			t2		   t3		  t4           t4         t5		 t5			t6		   t6
	// ---- obj1 ----- obj2 ----- obj3 ----- d-obj1 ----- obj4 ---- d-obj2 ---- obj5 ---- d-obj3 ---- obj6
	ts := types.TimestampToTS(timestamp.Timestamp{
		PhysicalTime: 10,
		LogicalTime:  00,
	})

	var (
		stats objectio.ObjectStats
		t1    = ts
		t2    = ts.Next()
		t3    = t2.Next()
		t4    = t3.Next()
		t5    = t4.Next()
		t6    = t5.Next()

		obj1, obj2, obj3, obj4, obj5, obj6 objectio.ObjectEntry
	)

	// t1: insert obj1
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj1"))
		obj1 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t1,
		}

		pState.dataObjectsNameIndex.Set(obj1)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj1.CreateTime,
			ShortObjName: *obj1.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t2: insert obj2
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj2"))
		obj2 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t2,
		}

		pState.dataObjectsNameIndex.Set(obj2)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj2.CreateTime,
			ShortObjName: *obj2.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t3: insert obj3
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj3"))
		obj3 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t3,
		}

		pState.dataObjectsNameIndex.Set(obj3)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj3.CreateTime,
			ShortObjName: *obj3.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t4: delete obj1, insert obj4
	{
		obj1.DeleteTime = t4
		pState.dataObjectsNameIndex.Set(obj1)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj1.DeleteTime,
			ShortObjName: *obj1.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj4"))
		obj4 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t4,
		}

		pState.dataObjectsNameIndex.Set(obj4)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj4.CreateTime,
			ShortObjName: *obj4.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// check 1
	{
		inserted, deleted := pState.CollectObjectsBetween(t1, t3)
		require.Nil(t, deleted)

		require.Equal(t, inserted,
			[]objectio.ObjectStats{
				obj1.ObjectStats,
				obj2.ObjectStats,
				obj3.ObjectStats,
			})
	}

	// check 2
	{
		inserted, deleted := pState.CollectObjectsBetween(t1, t4)
		require.Nil(t, deleted)
		require.Equal(t, inserted,
			[]objectio.ObjectStats{obj2.ObjectStats, obj3.ObjectStats, obj4.ObjectStats})
	}

	// check 3
	{
		inserted, deleted := pState.CollectObjectsBetween(t2, t4)
		require.Equal(t, deleted, []objectio.ObjectStats{obj1.ObjectStats})
		require.Equal(t, inserted,
			[]objectio.ObjectStats{obj2.ObjectStats, obj3.ObjectStats, obj4.ObjectStats})
	}

	// t5: delete obj2, insert obj5
	{
		obj2.DeleteTime = t5
		pState.dataObjectsNameIndex.Set(obj2)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj2.DeleteTime,
			ShortObjName: *obj2.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj5"))
		obj5 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t5,
		}

		pState.dataObjectsNameIndex.Set(obj5)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj5.CreateTime,
			ShortObjName: *obj5.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t6: delete obj3, insert obj6
	{
		obj3.DeleteTime = t6
		pState.dataObjectsNameIndex.Set(obj3)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj3.DeleteTime,
			ShortObjName: *obj3.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj6"))
		obj6 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t6,
		}

		pState.dataObjectsNameIndex.Set(obj6)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj6.CreateTime,
			ShortObjName: *obj6.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// random check
	{
		//name := []string{"t1", "t2", "t3", "t4", "t5", "t6"}
		tss := []types.TS{t1, t2, t3, t4, t5, t6}
		for i := 0; i < 100000; i++ {
			x := rand.Int() % (len(tss) - 1)
			y := rand.Int()%(len(tss)-x) + x

			tx := tss[x]
			ty := tss[y]

			require.True(t, ty.GE(&tx))

			inserted, deleted := pState.CollectObjectsBetween(tx, ty)
			for _, ss := range inserted {
				obj, ok := pState.GetObject(*ss.ObjectShortName())
				require.True(t, ok)

				if obj.DeleteTime.IsEmpty() {
					ok = obj.CreateTime.GE(&tx) && obj.CreateTime.LE(&ty)
					require.True(t, ok)
				} else {
					ok = obj.CreateTime.GE(&tx) && obj.CreateTime.LE(&ty) && obj.DeleteTime.GT(&ty)
					require.True(t, ok)
				}
			}

			for _, ss := range deleted {
				obj, ok := pState.GetObject(*ss.ObjectShortName())
				require.True(t, ok)

				require.False(t, obj.DeleteTime.IsEmpty())

				ok = obj.CreateTime.LT(&tx) && obj.DeleteTime.GE(&tx) && obj.DeleteTime.LE(&ty)
				require.True(t, ok)
			}

			//fmt.Println(name[x], name[y], len(inserted), len(deleted))

		}
	}
}
