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

package dnservice

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testDNStoreAddr = "unix:///tmp/test-dnstore.sock"
)

func TestNewStore(t *testing.T) {

}

func newTestStore(t *testing.T, uuid string, options ...Option) *store {
	assert.NoError(t, os.RemoveAll(testDNStoreAddr[7:]))
	c := &Config{
		UUID:          uuid,
		ListenAddress: testDNStoreAddr,
	}
	c.Txn.Clock.Backend = localClockBackend
	c.Txn.Storage.Backend = memStorageBackend
	c.FileService.Backend = memFileServiceBackend

	s, err := New(c, options...)
	assert.NoError(t, err)
	return s.(*store)
}
