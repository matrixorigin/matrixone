// Copyright 2023 Matrix Origin
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
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	defaultBasicConfig = &BasicPolicyConfig{
		MergeMaxOneRun:    common.DefaultMaxMergeObjN,
		MaxOsizeMergedObj: common.DefaultMaxOsizeObjMB * common.Const1MBytes,
		ObjectMinOsize:    common.DefaultMinOsizeQualifiedMB * common.Const1MBytes,
		MinCNMergeSize:    common.DefaultMinCNMergeSize * common.Const1MBytes,
	}
)

/// TODO(aptend): codes related storing and fetching configs are too annoying!

type BasicPolicyConfig struct {
	MergeMaxOneRun    int
	ObjectMinOsize    uint32
	MaxOsizeMergedObj uint32
	MinCNMergeSize    uint64
	FromUser          bool
	MergeHints        []api.MergeHint
}

func (c *BasicPolicyConfig) String() string {
	return fmt.Sprintf(
		"minOsizeObj:%v, maxOneRun:%v, maxOsizeMergedObj: %v, offloadToCNSize:%v, hints: %v",
		common.HumanReadableBytes(int(c.ObjectMinOsize)),
		c.MergeMaxOneRun,
		common.HumanReadableBytes(int(c.MaxOsizeMergedObj)),
		common.HumanReadableBytes(int(c.MinCNMergeSize)),
		c.MergeHints,
	)
}

type customConfigProvider struct {
	sync.Mutex
	configs map[uint64]*BasicPolicyConfig // works like a cache
}

func newCustomConfigProvider() *customConfigProvider {
	return &customConfigProvider{
		configs: make(map[uint64]*BasicPolicyConfig),
	}
}

func (o *customConfigProvider) GetConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	o.Lock()
	defer o.Unlock()
	p, ok := o.configs[tbl.ID]
	if !ok {
		// load from an atomic value
		extra := tbl.GetLastestSchemaLocked(false).Extra
		if extra.MaxObjOnerun == 0 && extra.MinOsizeQuailifed == 0 {
			p = defaultBasicConfig
			o.configs[tbl.ID] = p
		} else {
			// compatible with old version
			cnSize := extra.MinCnMergeSize
			if cnSize == 0 {
				cnSize = common.DefaultMinCNMergeSize * common.Const1MBytes
			}
			// compatible codes: remap old rows -> default bytes size
			minOsize := extra.MinOsizeQuailifed
			if minOsize < 80*8192 {
				minOsize = common.DefaultMinOsizeQualifiedMB * common.Const1MBytes
			}
			maxOsize := extra.MaxOsizeMergedObj
			if maxOsize < 500*8192 {
				maxOsize = common.DefaultMaxOsizeObjMB * common.Const1MBytes
			}
			p = &BasicPolicyConfig{
				ObjectMinOsize:    minOsize,
				MergeMaxOneRun:    int(extra.MaxObjOnerun),
				MaxOsizeMergedObj: maxOsize,
				MinCNMergeSize:    cnSize,
				FromUser:          true,
				MergeHints:        extra.Hints,
			}
			o.configs[tbl.ID] = p
		}
	}
	return p
}

func (o *customConfigProvider) invalidCache(tbl *catalog.TableEntry) {
	o.Lock()
	defer o.Unlock()
	delete(o.configs, tbl.ID)
}

func (o *customConfigProvider) SetCache(tbl *catalog.TableEntry, cfg *BasicPolicyConfig) {
	o.Lock()
	defer o.Unlock()
	o.configs[tbl.ID] = cfg
}

func (o *customConfigProvider) String() string {
	o.Lock()
	defer o.Unlock()
	keys := make([]uint64, 0, len(o.configs))
	for k := range o.configs {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b uint64) int { return cmp.Compare(a, b) })
	buf := bytes.Buffer{}
	buf.WriteString("customConfigProvider: ")
	for _, k := range keys {
		c := o.configs[k]
		buf.WriteString(fmt.Sprintf("%d:%v,%v | ", k, c.ObjectMinOsize, c.MergeMaxOneRun))
	}
	return buf.String()
}

func (o *customConfigProvider) ResetConfig() {
	o.Lock()
	defer o.Unlock()
	o.configs = make(map[uint64]*BasicPolicyConfig)
}
