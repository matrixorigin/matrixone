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

package plan

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/gogo/protobuf/proto"
)

func TestPlanFileDescriptorIsGzipEncoded(t *testing.T) {
	b := proto.FileDescriptor("plan.proto")
	if len(b) < 2 {
		t.Fatalf("plan descriptor is empty")
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("plan descriptor is not valid gzip: %v", err)
	}
	defer r.Close()
	buf := make([]byte, 1)
	if _, err = r.Read(buf); err != nil {
		t.Fatalf("plan descriptor payload cannot be decoded: %v", err)
	}
}
