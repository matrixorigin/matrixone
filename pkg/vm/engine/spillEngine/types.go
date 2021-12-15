// Copyright 2021 Matrix Origin
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

package spillEngine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/spillEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/spillEngine/meta"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
)

type spillEngine struct {
	path string
	cdb  *kv.KV    // column store
	db   engine.DB // kv store
}

type database struct {
	path string
	cdb  *kv.KV    // column store
	db   engine.DB // kv store
}

type relation struct {
	id string
	db *kv.KV
	md meta.Metadata
	mp map[string]metadata.Attribute
}
