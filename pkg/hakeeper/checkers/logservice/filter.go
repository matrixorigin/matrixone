package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"sort"
)

type excludedFilter struct {
	excluded map[string]struct{}
}

func newExcludedFilter(stores ...string) *excludedFilter {
	e := excludedFilter{map[string]struct{}{}}
	for _, store := range stores {
		e.excluded[store] = struct{}{}
	}
	return &e
}

func (e *excludedFilter) Filter(store *util.Store) bool {
	if _, ok := e.excluded[string(store.ID)]; ok {
		return true
	}
	return false
}

func selector(shardInfo logservice.LogShardInfo, stores *util.ClusterStores) util.StoreID {
	workingStores := stores.WorkingStores()
	excluded := make([]string, 0)
	for _, storeID := range shardInfo.Replicas {
		excluded = append(excluded, storeID)
	}

	candidates := util.FilterStore(workingStores, []util.IFilter{newExcludedFilter(excluded...)})
	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})

	return candidates[0].ID
}
