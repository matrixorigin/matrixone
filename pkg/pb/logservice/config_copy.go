// Copyright 2021 - 2022 Matrix Origin
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

package logservice

// DeepCopy implements the existing deepcopy hook used by HAKeeper snapshots.
// Configuration contains immutable strings; only maps, items and unknown bytes
// need independent storage. Avoid reflecting over every configuration field on
// each health check while keeping the returned snapshot detached from live state.
func (m *ConfigData) DeepCopy() interface{} {
	if m == nil {
		return (*ConfigData)(nil)
	}
	result := *m
	if m.XXX_unrecognized != nil {
		result.XXX_unrecognized = make([]byte, len(m.XXX_unrecognized), cap(m.XXX_unrecognized))
		copy(result.XXX_unrecognized, m.XXX_unrecognized)
	}
	if m.Content != nil {
		result.Content = make(map[string]*ConfigItem, len(m.Content))
		for key, item := range m.Content {
			if item == nil {
				result.Content[key] = nil
				continue
			}
			copied := *item
			if item.XXX_unrecognized != nil {
				copied.XXX_unrecognized = make([]byte, len(item.XXX_unrecognized), cap(item.XXX_unrecognized))
				copy(copied.XXX_unrecognized, item.XXX_unrecognized)
			}
			result.Content[key] = &copied
		}
	}
	return &result
}
