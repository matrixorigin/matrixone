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
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

// Hnsw Build index implementation
type HnswBuildIndex struct {
	Id          string
	Index       *usearch.Index
	Path        string
	Saved       bool
	Size        int64
	MaxCapacity uint
}

type HnswBuild struct {
	uid      string
	cfg      vectorindex.IndexConfig
	tblcfg   vectorindex.IndexTableConfig
	indexes  []*HnswBuildIndex
	nthread  int
	add_chan chan AddItem
	err_chan chan error
	wg       sync.WaitGroup
	once     sync.Once
	mutex    sync.Mutex
	count    atomic.Int64
}

type AddItem struct {
	key int64
	vec []float32
}

// New HnswBuildIndex struct
func NewHnswBuildIndex(id string, cfg vectorindex.IndexConfig, nthread int, max_capacity uint) (*HnswBuildIndex, error) {
	var err error
	idx := &HnswBuildIndex{}

	idx.Id = id

	idx.Index, err = usearch.NewIndex(cfg.Usearch)
	if err != nil {
		return nil, err
	}

	idx.MaxCapacity = max_capacity

	err = idx.Index.Reserve(idx.MaxCapacity)
	if err != nil {
		return nil, err
	}

	err = idx.Index.ChangeThreadsAdd(uint(nthread))
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Destroy the struct
func (idx *HnswBuildIndex) Destroy() error {
	if idx.Index != nil {
		err := idx.Index.Destroy()
		if err != nil {
			return err
		}
		idx.Index = nil
	}

	if idx.Saved && len(idx.Path) > 0 {
		// remove the file
		err := os.Remove(idx.Path)
		if err != nil {
			return err
		}
	}
	return nil
}

// Save the index to file
func (idx *HnswBuildIndex) SaveToFile() error {
	if idx.Saved {
		return nil
	}

	f, err := os.CreateTemp("", "hnsw")
	if err != nil {
		return err
	}

	err = idx.Index.Save(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	// free memory
	err = idx.Index.Destroy()
	if err != nil {
		return err
	}
	idx.Index = nil

	idx.Saved = true
	idx.Path = f.Name()
	return nil
}

const InsertIndexBatchSize = 2000

// Generate the SQL to update the secondary index tables.
// 1. store the index file into the index table
func (idx *HnswBuildIndex) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {

	err := idx.SaveToFile()
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(idx.Path)
	if err != nil {
		return nil, err
	}

	filesz := fi.Size()
	offset := int64(0)
	chunksz := int64(0)
	chunkid := int64(0)

	idx.Size = filesz

	sqls := make([]string, 0, 5)

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", cfg.DbName, cfg.IndexTable)
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(vectorindex.MaxChunkSize))))
	n := 0
	for offset = 0; offset < filesz; {
		if offset+vectorindex.MaxChunkSize < filesz {
			chunksz = vectorindex.MaxChunkSize

		} else {
			chunksz = filesz - offset
		}

		url := fmt.Sprintf("file://%s?offset=%d&size=%d", idx.Path, offset, chunksz)
		tuple := fmt.Sprintf("('%s', %d, load_file(cast('%s' as datalink)), 0)", idx.Id, chunkid, url)
		values = append(values, tuple)

		// offset and chunksz
		offset += chunksz
		chunkid++

		n++
		if n == InsertIndexBatchSize {
			newsql := sql + strings.Join(values, ", ")
			sqls = append(sqls, newsql)
			values = values[:0]
			n = 0
		}
	}

	if len(values) > 0 {
		newsql := sql + strings.Join(values, ", ")
		sqls = append(sqls, newsql)
	}

	//sql += strings.Join(values, ", ")
	//return []string{sql}, nil
	return sqls, nil
}

// is the index empty
func (idx *HnswBuildIndex) Empty() (bool, error) {
	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}
	return (sz == 0), nil
}

// check the index is full, i.e. 10K vectors
func (idx *HnswBuildIndex) Full() (bool, error) {
	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}
	return (sz == idx.MaxCapacity), nil
}

// add vector to the index
func (idx *HnswBuildIndex) Add(key int64, vec []float32) error {
	return idx.Index.Add(uint64(key), vec)
}

// create HsnwBuild struct
func NewHnswBuild(proc *process.Process, uid string, nworker int32,
	cfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (info *HnswBuild, err error) {

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

	info = &HnswBuild{
		uid:     uid,
		cfg:     cfg,
		tblcfg:  tblcfg,
		indexes: make([]*HnswBuildIndex, 0, 16),
		nthread: int(nthread),
	}

	if nthread > 1 {
		info.add_chan = make(chan AddItem, nthread*4)
		info.err_chan = make(chan error, nthread)

		// create multi-threads worker for add
		for i := 0; i < info.nthread; i++ {

			info.wg.Add(1)
			go func() {
				defer info.wg.Done()
				var err0 error
				closed := false
				for !closed {
					closed, err0 = info.addFromChannel(proc)
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

func (h *HnswBuild) addFromChannel(proc *process.Process) (stream_closed bool, err error) {
	var res AddItem
	var ok bool

	select {
	case res, ok = <-h.add_chan:
		if !ok {
			return true, nil
		}
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	}

	// add
	err = h.addVectorSync(res.key, res.vec)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (h *HnswBuild) CloseAndWait() {
	if h.nthread > 1 {
		h.once.Do(func() {
			close(h.add_chan)
			h.wg.Wait()
		})
	}
}

// destroy
func (h *HnswBuild) Destroy() error {

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

func (h *HnswBuild) Add(key int64, vec []float32) error {
	if h.nthread > 1 {

		select {
		case err := <-h.err_chan:
			return err
		default:
		}
		// copy the []float32 slice.
		h.add_chan <- AddItem{key, append(make([]float32, 0, len(vec)), vec...)}
		return nil
	} else {
		return h.addVector(key, vec)
	}
}

func (h *HnswBuild) createIndexUniqueKey(id int64) string {
	return fmt.Sprintf("%s:%d", h.uid, id)
}

func (h *HnswBuild) getIndexForAddSync() (idx *HnswBuildIndex, save_idx *HnswBuildIndex, err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.getIndexForAdd()
}

func (h *HnswBuild) getIndexForAdd() (idx *HnswBuildIndex, save_idx *HnswBuildIndex, err error) {

	save_idx = nil
	nidx := int64(len(h.indexes))
	if nidx == 0 {
		idx, err = NewHnswBuildIndex(h.createIndexUniqueKey(nidx), h.cfg, h.nthread, uint(h.tblcfg.IndexCapacity))
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
			idx, err = NewHnswBuildIndex(h.createIndexUniqueKey(nidx), h.cfg, h.nthread, uint(h.tblcfg.IndexCapacity))
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
func (h *HnswBuild) addVectorSync(key int64, vec []float32) error {
	var err error
	var idx *HnswBuildIndex
	var save_idx *HnswBuildIndex

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
func (h *HnswBuild) addVector(key int64, vec []float32) error {
	var err error
	var idx *HnswBuildIndex
	var save_idx *HnswBuildIndex

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
func (h *HnswBuild) ToInsertSql(ts int64) ([]string, error) {

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

func (h *HnswBuild) GetIndexes() []*HnswBuildIndex {
	return h.indexes
}
