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

package memEngine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/meta"
)

// MemEngine standalone memory engine
type MemEngine struct {
	db *kv.KV
	n  engine.Node
}

type database struct {
	db *kv.KV
	n  engine.Node
}

type relation struct {
	id string
	db *kv.KV
	n  engine.Node
	md meta.Metadata
}

type reader struct {
	zs    []int64
	db    *kv.KV
	segs  []string
	cds   []*bytes.Buffer
	dds   []*bytes.Buffer
	attrs map[string]engine.Attribute
}
