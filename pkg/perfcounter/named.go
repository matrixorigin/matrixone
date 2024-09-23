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

package perfcounter

import (
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var Named sync.Map

func NameForNode(nodeType, uuid string) string {
	return strings.Join([]string{
		"node",
		nodeType,
		uuid,
	}, ":")
}

func NameForFileService(nodeType string, uuid string, fsName string) string {
	return strings.Join([]string{
		"fs",
		nodeType,
		uuid,
		fsName,
	}, ":")
}

func decodeName(name string) (string, string) {
	s := strings.Split(name, ":")
	if len(s) != 3 {
		return "", ""
	}
	return strings.TrimSpace(s[1]), strings.TrimSpace(s[2])
}

func LogNodeCacheStats(uuid string) {
	v, ok := Named.Load(NameForNode("", uuid))
	if !ok {
		return
	}
	counter := v.(*CounterSet)
	logutil.Debug("cache stats",
		zap.Any("node", uuid),
		zap.Any("type", "memory"),
		zap.Any("hit ratio", float64(counter.FileService.Cache.Memory.Hit.Load())/
			float64(counter.FileService.Cache.Memory.Read.Load())),
	)
	logutil.Debug("cache stats",
		zap.Any("node", uuid),
		zap.Any("type", "disk"),
		zap.Any("hit ratio", float64(counter.FileService.Cache.Disk.Hit.Load())/
			float64(counter.FileService.Cache.Disk.Read.Load())),
	)
}

// GetCacheStats returns the cache stats for nodes.
func GetCacheStats(callback func(info []*query.CacheInfo)) {
	var ok = false
	var ptr *CounterSet
	var name string
	Named.Range(func(k, v interface{}) bool {
		if callback != nil {
			name = ""
			ptr = nil

			if name, ok = k.(string); !ok {
				return true
			}
			if strings.Contains(strings.ToLower(name), "global") {
				return true
			}
			if ptr, ok = v.(*CounterSet); !ok {
				return true
			}
			if ptr == nil {
				return true
			}

			nodeType, nodeId := decodeName(name)
			if len(nodeType) == 0 || len(nodeId) == 0 {
				return true
			}

			//memory
			read1 := ptr.FileService.Cache.Memory.Read.Load()
			if read1 <= 0 {
				read1 = 1
			}
			ci1 := &query.CacheInfo{
				NodeType:  nodeType,
				NodeId:    nodeId,
				CacheType: "memory",
				HitRatio:  float32(ptr.FileService.Cache.Memory.Hit.Load()) / float32(read1),
			}

			read2 := ptr.FileService.Cache.Disk.Read.Load()
			if read2 <= 0 {
				read2 = 1
			}
			ci2 := &query.CacheInfo{
				NodeType:  nodeType,
				NodeId:    nodeId,
				CacheType: "disk",
				Used:      0,
				Free:      0,
				HitRatio:  float32(ptr.FileService.Cache.Disk.Hit.Load()) / float32(read2),
			}

			callback([]*query.CacheInfo{ci1, ci2})
		}

		return true
	})
}
