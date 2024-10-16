// Copyright 2024 Matrix Origin
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

//go:build linux
// +build linux

package system

import (
	"context"
	"path/filepath"
	"unsafe"

	"github.com/containerd/cgroups/v3"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	cgroupv2MountPoint = "/sys/fs/cgroup"
	cgroupv2CPULimit   = "cpu.max"
	cgroupv2MemLimit   = "memory.max"
)

// runWatchCgroupConfig do watch cpu.max and memory.max to upgrade resource.
// pls, this function is run-in-container logic.
func runWatchCgroupConfig(stopper *stopper.Stopper) {
	// there are three cases while using cgroup, see more in cgroups.FromCgroup()
	// ignore cgroups.Legacy case.
	if mode := cgroups.Mode(); mode == cgroups.Legacy {
		logutil.Warnf("ignore watch cgroup config in cgroupv1.")
		return
	}

	// cgroup v2 root
	cgDir := cgroupv2MountPoint

	fd, err := unix.InotifyInit()
	if err != nil {
		logutil.Errorf("unable to init inotify: %v", err)
		return
	}
	// watch cpu.max modified
	cpuFd, err := unix.InotifyAddWatch(fd, filepath.Join(cgDir, cgroupv2CPULimit), unix.IN_MODIFY)
	if err != nil {
		unix.Close(fd)
		logutil.Errorf("unable to add inotify watch cpu.max: %v", err)
		return
	}
	// watch memory.max modified
	memFd, err := unix.InotifyAddWatch(fd, filepath.Join(cgDir, cgroupv2MemLimit), unix.IN_MODIFY)
	if err != nil {
		unix.Close(fd)
		logutil.Errorf("unable to add inotify watch memory.max: %v", err)
		return
	}

	if err := stopper.RunNamedTask("cgroup config watcher", func(ctx context.Context) {
		var (
			buffer [unix.SizeofInotifyEvent + unix.PathMax + 1]byte
			offset uint32
		)
		defer func() {
			unix.Close(fd)
			logutil.Info("exit cgroup config watcher")
		}()

		for {
			n, err := unix.Read(fd, buffer[:])
			if err != nil {
				logutil.Error("unable to read event data from inotify", zap.Error(err))
				return
			}
			if n < unix.SizeofInotifyEvent {
				logutil.Warnf("we should read at least %d bytes from inotify, but got %d bytes.", unix.SizeofInotifyEvent, n)
				return
			}
			offset = 0
			for offset <= uint32(n-unix.SizeofInotifyEvent) {
				rawEvent := (*unix.InotifyEvent)(unsafe.Pointer(&buffer[offset]))
				offset += unix.SizeofInotifyEvent + rawEvent.Len
				if rawEvent.Mask&unix.IN_MODIFY != unix.IN_MODIFY {
					continue
				}
				switch int(rawEvent.Wd) {
				case cpuFd:
					logutil.Info("got cpu.max changed")
					refreshQuotaConfig()
				case memFd:
					logutil.Info("got memory.max changed")
					refreshQuotaConfig()
				}
			}
		}
	}); err != nil {
		logutil.Errorf("failed to start cgroup config watcher: %v", err)
	}
}
