// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func Test_CleanUpUselessFiles(t *testing.T) {
	tDir := os.TempDir()
	dir := path.Join(tDir, "/local")
	assert.NoError(t, os.RemoveAll(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	c := fileservice.Config{
		Name:    defines.ETLFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer fs.Close()

	ent := &api.MergeCommitEntry{
		BookingLoc: []string{"abc"},
	}

	CleanUpUselessFiles(ent, fs)
}
