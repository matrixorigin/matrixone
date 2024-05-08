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

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileServices(t *testing.T) {
	ctx := context.Background()
	c, err := NewCluster(
		ctx,
		t,
		DefaultOptions().
			WithTNServiceNum(3).
			WithCNServiceNum(2))
	require.NoError(t, err)
	fs := c.(*testCluster).buildFileServices(ctx)
	require.NotNil(t, fs.getTNLocalFileService(0))
	require.NotNil(t, fs.getTNLocalFileService(1))
	require.NotNil(t, fs.getTNLocalFileService(2))
	require.Nil(t, fs.getTNLocalFileService(3))
}
