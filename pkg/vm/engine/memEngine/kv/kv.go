// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import "bytes"

func New() *KV {
	return &KV{mp: make(map[string][]byte)}
}

func (a *KV) Close() error {
	return nil
}

func (a *KV) Del(k string) error {
	a.Lock()
	defer a.Unlock()
	delete(a.mp, k)
	return nil
}

func (a *KV) Set(k string, v []byte) error {
	a.Lock()
	defer a.Unlock()
	a.mp[k] = v
	return nil
}

func (a *KV) Get(k string, buf *bytes.Buffer) ([]byte, error) {
	a.Lock()
	defer a.Unlock()
	v, ok := a.mp[k]
	if !ok {
		return nil, ErrNotExist
	}
	buf.Reset()
	if len(v) > buf.Cap() {
		buf.Grow(len(v))
	}
	data := buf.Bytes()[:len(v)]
	copy(data, v)
	return data, nil
}

func (a *KV) Range() ([]string, [][]byte) {
	a.Lock()
	defer a.Unlock()
	names := make([]string, 0, len(a.mp))
	datas := make([][]byte, 0, len(a.mp))
	for k, v := range a.mp {
		names = append(names, k)
		datas = append(datas, v)
	}
	return names, datas
}
