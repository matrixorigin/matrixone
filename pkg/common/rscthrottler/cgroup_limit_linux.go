// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package rscthrottler

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/containerd/cgroups/v3"
	"golang.org/x/sys/unix"
)

// The legacy provider can return ErrNoLimit for an unreadable or malformed
// limit file. Confirm its no-limit sentinel before publishing a host ceiling.
func resolvePressureCgroup(limit uint64, err error) (uint64, error) {
	// The legacy v2 reader returns the leaf limit only. Use the same
	// hierarchy-aware ceiling for successful and unsuccessful reads.
	if cgroups.Mode() == cgroups.Unified {
		return pressureCgroupV2Limit()
	}
	if cgroups.Mode() == cgroups.Hybrid || cgroups.Mode() == cgroups.Legacy {
		controllers, _, parseErr := cgroups.ParseCgroupFileUnified("/proc/self/cgroup")
		if parseErr != nil {
			return 0, parseErr
		}
		if path, exists := controllers["memory"]; exists {
			return pressureCgroupV1Limit(path)
		}
		if cgroups.Mode() == cgroups.Hybrid {
			return pressureCgroupV2Limit()
		}
		return 0, memlimit.ErrNoCgroup
	}
	if errors.Is(err, memlimit.ErrNoCgroup) {
		var st unix.Statfs_t
		if statErr := unix.Statfs("/sys/fs/cgroup", &st); statErr == nil {
			return 0, fmt.Errorf("cgroup mode unavailable despite an accessible mount")
		} else if !errors.Is(statErr, unix.ENOENT) {
			return 0, statErr
		}
		return limit, err
	}
	return limit, err
}

func pressureCgroupV2Limit() (uint64, error) {
	_, group, err := cgroups.ParseCgroupFileUnified("/proc/self/cgroup")
	if err != nil {
		return 0, err
	}
	mount, dir, err := pressureCgroupMount(group, "cgroup2")
	if err != nil {
		return 0, err
	}
	return pressureCgroupV2Directory(mount, dir)
}

func pressureCgroupV2Directory(mount, dir string) (uint64, error) {
	mount, dir = filepath.Clean(mount), filepath.Clean(dir)
	rel, err := filepath.Rel(mount, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return 0, fmt.Errorf("cgroup path outside mount")
	}
	var minimum uint64
	for {
		if _, err := os.Stat(filepath.Join(dir, "cgroup.controllers")); err != nil {
			return 0, err
		}
		data, err := os.ReadFile(filepath.Join(dir, "memory.max"))
		if errors.Is(err, os.ErrNotExist) {
			if dir != mount {
				controllers, readErr := os.ReadFile(filepath.Join(filepath.Dir(dir), "cgroup.subtree_control"))
				if readErr != nil {
					return 0, readErr
				}
				for _, controller := range strings.Fields(string(controllers)) {
					if strings.TrimPrefix(controller, "+") == "memory" {
						return 0, err
					}
				}
			}
		} else if err != nil {
			return 0, err
		} else if value := strings.TrimSpace(string(data)); value != "max" {
			n, parseErr := strconv.ParseUint(value, 10, 63)
			if parseErr != nil || n == 0 {
				return 0, fmt.Errorf("invalid cgroup v2 memory limit %q", value)
			}
			if minimum == 0 || n < minimum {
				minimum = n
			}
		}
		if dir == mount {
			break
		}
		dir = filepath.Dir(dir)
	}
	if minimum == 0 {
		return 0, memlimit.ErrNoLimit
	}
	return minimum, nil
}

func pressureCgroupV1Limit(group string) (uint64, error) {
	_, dir, err := pressureCgroupMount(group, "cgroup")
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return 0, err
	}
	return parseCgroupV1Limit(data, uint64(os.Getpagesize()))
}

// pressureCgroupMount resolves a process membership relative to its actual
// mount root, which may be a container's bind-mounted subtree.
func pressureCgroupMount(group, fsType string) (string, string, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", "", err
	}
	return parsePressureCgroupMount(data, group, fsType)
}

func parsePressureCgroupMount(data []byte, group, fsType string) (string, string, error) {
	var bestRoot, bestMount, bestDir string
	unescape := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\")
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		before, after := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(before) < 5 || len(after) < 3 || after[0] != fsType {
			continue
		}
		if fsType == "cgroup" && !strings.Contains(","+after[2]+",", ",memory,") {
			continue
		}
		root, mount := filepath.Clean(unescape.Replace(before[3])), filepath.Clean(unescape.Replace(before[4]))
		rel, err := filepath.Rel(root, filepath.Clean(group))
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			continue
		}
		if bestMount == "" || len(root) < len(bestRoot) {
			bestRoot, bestMount, bestDir = root, mount, filepath.Join(mount, rel)
		}
	}
	if bestMount == "" {
		return "", "", fmt.Errorf("memory cgroup mount not found for %q", group)
	}
	return bestMount, bestDir, nil
}

func parseCgroupV1Limit(data []byte, page uint64) (uint64, error) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "hierarchical_memory_limit" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			if page != 0 && value == uint64(^uint64(0)>>1)/page*page {
				return 0, memlimit.ErrNoLimit
			}
			if value == 0 || value > uint64(^uint64(0)>>1) {
				return 0, fmt.Errorf("invalid cgroup v1 memory limit")
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("cgroup hierarchical memory limit missing")
}
