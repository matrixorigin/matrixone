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

package fz

import (
	"os/user"
	"strconv"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
)

type SetupFuse func(
	mountPoint string,
) (
	err error,
	end func() error,
)

func (_ Def) SetupFuse(
	logger Logger,
) SetupFuse {
	return func(
		mountPoint string,
	) (
		err error,
		end func() error,
	) {
		defer he(&err)

		u, err := user.Current()
		ce(err)
		intUid, err := strconv.Atoi(u.Uid)
		ce(err)
		intGid, err := strconv.Atoi(u.Gid)
		ce(err)

		server, err := fs.Mount(
			mountPoint,
			new(inode),
			&fs.Options{
				MountOptions: fuse.MountOptions{
					//Debug:         true,
					Name: "motest",
					//DisableXAttrs: true,
				},
				UID:    uint32(intUid),
				GID:    uint32(intGid),
				Logger: zap.NewStdLog(logger),
			},
		)
		ce(err)

		go func() {
			server.Wait()
		}()
		ce(server.WaitMount())

		end = func() error {
			return server.Unmount()
		}

		return
	}
}
