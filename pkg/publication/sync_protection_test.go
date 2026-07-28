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

package publication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMoCtlResponse_Valid(t *testing.T) {
	input := `{"method":"test","result":[{"ReturnStr":"{\"status\":\"ok\"}"}]}`
	ret, err := parseMoCtlResponse(input)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, ret)
}

func TestParseMoCtlResponse_EmptyResult(t *testing.T) {
	input := `{"method":"test","result":[]}`
	_, err := parseMoCtlResponse(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no result")
}

func TestParseMoCtlResponse_InvalidJSON(t *testing.T) {
	_, err := parseMoCtlResponse("not json")
	assert.Error(t, err)
}

func TestIsGCRunningError(t *testing.T) {
	assert.False(t, IsGCRunningError(nil))
	assert.True(t, IsGCRunningError(errors.New("GC is running")))
	assert.False(t, IsGCRunningError(errors.New("something else")))
}

func TestIsSyncProtectionNotFoundError(t *testing.T) {
	assert.False(t, IsSyncProtectionNotFoundError(nil))
	assert.True(t, IsSyncProtectionNotFoundError(errors.New("sync protection not found")))
	assert.False(t, IsSyncProtectionNotFoundError(errors.New("other")))
}

func TestIsSyncProtectionExistsError(t *testing.T) {
	assert.False(t, IsSyncProtectionExistsError(nil))
	assert.True(t, IsSyncProtectionExistsError(errors.New("sync protection already exists")))
	assert.False(t, IsSyncProtectionExistsError(errors.New("other")))
}

func TestIsSyncProtectionMaxCountError(t *testing.T) {
	assert.False(t, IsSyncProtectionMaxCountError(nil))
	assert.True(t, IsSyncProtectionMaxCountError(errors.New("sync protection max count reached")))
	assert.False(t, IsSyncProtectionMaxCountError(errors.New("other")))
}

func TestIsSyncProtectionSoftDeleteError(t *testing.T) {
	assert.False(t, IsSyncProtectionSoftDeleteError(nil))
	assert.True(t, IsSyncProtectionSoftDeleteError(errors.New("sync protection is soft deleted")))
	assert.False(t, IsSyncProtectionSoftDeleteError(errors.New("other")))
}

func TestIsSyncProtectionInvalidError(t *testing.T) {
	assert.False(t, IsSyncProtectionInvalidError(nil))
	assert.True(t, IsSyncProtectionInvalidError(errors.New("invalid sync protection request")))
	assert.False(t, IsSyncProtectionInvalidError(errors.New("other")))
}

func TestBuildBloomFilterFromObjectMap_Empty(t *testing.T) {
	result, err := BuildBloomFilterFromObjectMap(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", result)
}
