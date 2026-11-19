// Copyright 2026 Matrix Origin
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

package search

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type keyReader struct {
	engine.Reader
	keys *keycodec.KeyPayload
	once sync.Once
	err  error
}

// RetainReaderKeys gives each lazy reader its own key reference. Construction
// borrows the caller's reference; Close releases keys after the reader closes.
func RetainReaderKeys(readers []engine.Reader, keys *keycodec.KeyPayload) ([]engine.Reader, error) {
	if keys == nil {
		return readers, nil
	}
	for i, reader := range readers {
		if reader == nil || !keys.Retain() {
			for _, opened := range readers {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return nil, moerr.NewInvalidStateNoCtx("runtime keys were released before reader construction")
		}
		readers[i] = &keyReader{Reader: reader, keys: keys}
	}
	return readers, nil
}

func (r *keyReader) TakeExplainDiagnostics() []*plan.Query {
	if source, ok := r.Reader.(engine.ExplainDiagnosticReader); ok {
		return source.TakeExplainDiagnostics()
	}
	return nil
}

func (r *keyReader) Close() error {
	r.once.Do(func() { defer r.keys.Release(); r.err = r.Reader.Close() })
	return r.err
}
