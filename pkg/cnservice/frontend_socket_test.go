// Copyright 2021 - 2026 Matrix Origin
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

package cnservice

import (
	"context"
	"errors"
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestCNFrontendUnixSocketOverride(t *testing.T) {
	defaults := config.FrontendParameters{}
	defaults.SetDefaultValues()
	for _, tc := range []struct {
		name string
		opts []Option
		want string
	}{
		{"default", nil, defaults.UnixSocketAddress},
		{"TCP-only", []Option{WithFrontendUnixSocketAddress("")}, ""},
		{"explicit", []Option{WithFrontendUnixSocketAddress("explicit.sock")}, "explicit.sock"},
		{"last-option", []Option{WithFrontendUnixSocketAddress("explicit.sock"), WithFrontendUnixSocketAddress("")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serviceID := strings.ReplaceAll(t.Name(), "/", "-")
			moruntime.RunTest(serviceID, func(rt moruntime.Runtime) {
				local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				etl, err := fileservice.NewMemoryFS(defines.ETLFileServiceName, fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				fs, err := fileservice.NewFileServices(defines.LocalFileServiceName, local, etl)
				require.NoError(t, err)
				t.Cleanup(func() { fs.Close(context.Background()) })
				var owner Service
				defer func() {
					if owner != nil {
						require.NoError(t, owner.Close())
					}
				}()
				var observed config.FrontendParameters
				sentinel := errors.New("constructor observed")
				opts := append([]Option(nil), tc.opts...)
				opts = append(opts, func(s *service) {
					observed = s.cfg.Frontend
					panic(sentinel) // Stop before allocating network/service resources.
				})
				cfg := &Config{UUID: serviceID, Frontend: config.FrontendParameters{Port: 54321}}
				require.PanicsWithValue(t, sentinel, func() {
					_, _ = NewService(cfg, t.Context(), fs, nil, func(s Service) { owner = s }, opts...)
				})
				require.NotNil(t, owner)
				require.Equal(t, tc.want, observed.UnixSocketAddress)
				require.Equal(t, int64(54321), observed.Port)
				require.Positive(t, observed.ConnectTimeout.Duration)
			})
		})
	}
}
