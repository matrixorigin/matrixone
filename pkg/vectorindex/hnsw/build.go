// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type HnswBuild[T types.RealNumbers] struct {
	uid      string
	cfg      vectorindex.IndexConfig
	tblcfg   vectorindex.IndexTableConfig
	indexes  []*HnswModel[T]
	nthread  int
	add_chan chan AddItem[T]
	err_chan chan error
	wg       sync.WaitGroup
	once     sync.Once
	mutex    sync.Mutex
	count    atomic.Int64
}

type AddItem[T types.RealNumbers] struct {
	key int64
	vec []T
}

// create HsnwBuild struct
func NewHnswBuild[T types.RealNumbers](sqlproc *sqlexec.SqlProcess, uid string, nworker int32,
	cfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (info *HnswBuild[T], err error) {

	// estimate the number of worker threads
	nthread := 0
	if nworker <= 1 {
		// single database thread and set nthread to ThreadsBuild
		nthread = int(vectorindex.GetConcurrency(tblcfg.ThreadsBuild))
	} else {
		// multiple database worker threads
		threadsbuild := vectorindex.GetConcurrencyForBuild(tblcfg.ThreadsBuild)
		nthread = int(float64(threadsbuild) / float64(nworker))
	}
	if nthread < 1 {
		nthread = 1
	}

	info = &HnswBuild[T]{
		uid:     uid,
		cfg:     cfg,
		tblcfg:  tblcfg,
		indexes: make([]*HnswModel[T], 0, 16),
		nthread: int(nthread),
	}

	if nthread > 1 {
		info.add_chan = make(chan AddItem[T], nthread*4)
		info.err_chan = make(chan error, nthread)

		// create multi-threads worker for add
		for i := 0; i < info.nthread; i++ {

			info.wg.Add(1)
			go func() {
				defer info.wg.Done()
				var err0 error
				closed := false
				for !closed {
					closed, err0 = info.addFromChannel(sqlproc)
					if err0 != nil {
						info.err_chan <- err0
						return
					}
				}
			}()
		}

	}
	return info, nil
}

func (h *HnswBuild[T]) addFromChannel(sqlproc *sqlexec.SqlProcess) (stream_closed bool, err error) {
	var res AddItem[T]
	var ok bool

	procCtx := sqlproc.GetContext()
	select {
	case res, ok = <-h.add_chan:
		if !ok {
			return true, nil
		}
	case <-procCtx.Done():
		return false, moerr.NewInternalError(procCtx, "context cancelled")
	}

	// add
	err = h.addVectorSync(res.key, res.vec)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (h *HnswBuild[T]) CloseAndWait() {
	if h.nthread > 1 {
		h.once.Do(func() {
			close(h.add_chan)
			h.wg.Wait()
		})
	}
}

// destroy
func (h *HnswBuild[T]) Destroy() error {

	var errs error

	h.CloseAndWait()

	for _, idx := range h.indexes {
		err := idx.Destroy()
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	h.indexes = nil
	return errs
}

func (h *HnswBuild[T]) Add(key int64, vec []T) error {
	if h.nthread > 1 {

		select {
		case err := <-h.err_chan:
			return err
		default:
		}
		// copy the []float32 slice.
		h.add_chan <- AddItem[T]{key, append(make([]T, 0, len(vec)), vec...)}
		return nil
	} else {
		return h.addVector(key, vec)
	}
}

func (h *HnswBuild[T]) createIndexUniqueKey(id int64) string {
	return fmt.Sprintf("%s:%d", h.uid, id)
}

func (h *HnswBuild[T]) getIndexForAddSync() (idx *HnswModel[T], save_idx *HnswModel[T], err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.getIndexForAdd()
}

func (h *HnswBuild[T]) getIndexForAdd() (idx *HnswModel[T], save_idx *HnswModel[T], err error) {

	save_idx = nil
	nidx := int64(len(h.indexes))
	if nidx == 0 {
		idx, err = NewHnswModelForBuild[T](h.createIndexUniqueKey(nidx), h.cfg, h.nthread, uint(h.tblcfg.IndexCapacity))
		if err != nil {
			return nil, nil, err
		}
		h.indexes = append(h.indexes, idx)
	} else {
		// get last index
		idx = h.indexes[nidx-1]

		cnt := h.count.Load()
		if uint(cnt) >= idx.MaxCapacity {
			// assign save_idx to save out of mutex
			save_idx = idx

			// create new index
			idx, err = NewHnswModelForBuild[T](h.createIndexUniqueKey(nidx), h.cfg, h.nthread, uint(h.tblcfg.IndexCapacity))
			if err != nil {
				return nil, nil, err
			}
			h.indexes = append(h.indexes, idx)
			// reset count for next index
			h.count.Store(0)
		}
	}
	h.count.Add(1)

	return idx, save_idx, nil
}

// add vector to the build
// it will check the current index is full and add the vector to available index
// sync version for multi-thread
func (h *HnswBuild[T]) addVectorSync(key int64, vec []T) error {
	var err error
	var idx *HnswModel[T]
	var save_idx *HnswModel[T]

	idx, save_idx, err = h.getIndexForAddSync()
	if err != nil {
		return err
	}

	if save_idx != nil {
		// save the current index to file
		err = save_idx.SaveToFile()
		if err != nil {
			return err
		}
	}

	return idx.Add(key, vec)
}

// add vector to the build
// it will check the current index is full and add the vector to available index
// single-threaded version.
func (h *HnswBuild[T]) addVector(key int64, vec []T) error {
	var err error
	var idx *HnswModel[T]
	var save_idx *HnswModel[T]

	h.mutex.Lock()
	defer h.mutex.Unlock()
	idx, save_idx, err = h.getIndexForAdd()
	if err != nil {
		return err
	}

	if save_idx != nil {
		// save the current index to file
		err = save_idx.SaveToFile()
		if err != nil {
			return err
		}
	}

	return idx.Add(key, vec)
}

// generate SQL to update the secondary index tables
// 1. sync the metadata table
// 2. sync the index file to index table
func (h *HnswBuild[T]) ToInsertSql(ts int64) ([]string, error) {

	h.CloseAndWait()

	if len(h.indexes) == 0 {
		return []string{}, nil
	}

	sqls := make([]string, 0, len(h.indexes)+1)

	metas := make([]string, 0, len(h.indexes))
	for _, idx := range h.indexes {
		indexsqls, err := idx.ToSql(h.tblcfg)
		if err != nil {
			return nil, err
		}

		sqls = append(sqls, indexsqls...)

		//os.Stderr.WriteString(fmt.Sprintf("Sql: %s\n", sql))
		chksum, err := vectorindex.CheckSum(idx.Path)
		if err != nil {
			return nil, err
		}

		finfo, err := os.Stat(idx.Path)
		if err != nil {
			return nil, err
		}
		fs := finfo.Size()

		metas = append(metas, fmt.Sprintf("('%s', '%s', %d, %d)", idx.Id, chksum, ts, fs))
	}

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", h.tblcfg.DbName, h.tblcfg.MetadataTable, strings.Join(metas, ", "))

	sqls = append(sqls, metasql)
	return sqls, nil
}

func (h *HnswBuild[T]) GetIndexes() []*HnswModel[T] {
	return h.indexes
}
