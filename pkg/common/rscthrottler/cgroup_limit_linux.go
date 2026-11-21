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
	"cmp"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/containerd/cgroups/v3"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/sys/unix"
)

// Resolve the pressure ceiling from the process membership. A mount at the
// conventional cgroup path need not be the view exposing that membership.
func resolvePressureCgroup(_ uint64, legacyErr error, publishRoot func(string)) (rssPressureLimit, error) {
	controllers, unified, err := cgroups.ParseCgroupFileUnified("/proc/self/cgroup")
	if err == nil {
		if group, ok := controllers["memory"]; ok {
			limit, err := pressureCgroupV1Limit(group)
			return rssPressureLimit{bytes: limit}, err
		}
		if unified != "" {
			return pressureCgroupV2Limit(unified, publishRoot)
		}
		if len(controllers) != 0 {
			return rssPressureLimit{}, memlimit.ErrNoCgroup
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return rssPressureLimit{}, err
	}
	// A genuine lack of cgroups permits host fallback. An empty or missing
	// membership with an accessible mount is an incomplete observation.
	if errors.Is(legacyErr, memlimit.ErrNoCgroup) {
		var st unix.Statfs_t
		if statErr := unix.Statfs("/sys/fs/cgroup", &st); errors.Is(statErr, unix.ENOENT) {
			return rssPressureLimit{}, legacyErr
		} else if statErr != nil {
			return rssPressureLimit{}, statErr
		}
	}
	if err != nil {
		return rssPressureLimit{}, err
	}
	return rssPressureLimit{}, moerr.NewInternalError(moerr.NoReportContext(), "cgroup membership unavailable")
}

func pressureCgroupV2Limit(group string, publishRoot func(string)) (rssPressureLimit, error) {
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return rssPressureLimit{}, err
	}
	return resolvePressureCgroupV2(group, mountinfo, publishRoot)
}

func resolvePressureCgroupV2(group string, mountinfo []byte, publishRoot func(string)) (rssPressureLimit, error) {
	views, knownRoot, err := parsePressureCgroupViews(mountinfo, group, "cgroup2")
	ceiling := rssPressureLimit{knownRoot: knownRoot}
	if knownRoot != "" {
		// Mount topology is independent of RSS order and already available
		// even if the subsequent hierarchy reads stall or fail.
		publishRoot(knownRoot)
	}
	if err != nil {
		return ceiling, err
	}
	ceiling.bytes, err = pressureCgroupV2Hierarchy(filepath.Clean(group), views)
	if err == nil || errors.Is(err, memlimit.ErrNoLimit) {
		ceiling.readRoot = views[0].root
	} else {
		ceiling.incomplete = true
	}
	return ceiling, err
}

// Read each visible ancestor once successfully. Alternate mount views can
// expose a masked descendant without hiding a stricter ancestor in the wider
// view. Each finite limit bounds the process independently. A read error
// leaves that bound usable for pressure but rules out recovery.
func pressureCgroupV2Hierarchy(group string, views []pressureCgroupView) (uint64, error) {
	root := views[0].root
	var minimum uint64
	var firstErr error
	for level := group; ; level = filepath.Dir(level) {
		limit, err := pressureCgroupV2Level(level, root, views)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else if limit != 0 && (minimum == 0 || limit < minimum) {
			minimum = limit
		}
		if level == root {
			break
		}
	}
	if firstErr != nil {
		return minimum, firstErr
	}
	if minimum == 0 {
		return 0, memlimit.ErrNoLimit
	}
	return minimum, nil
}

func pressureCgroupV2Level(level, root string, views []pressureCgroupView) (uint64, error) {
	var firstErr error
	var noLimit bool
	for _, view := range views {
		dir, ok := view.directory(level)
		if !ok {
			continue
		}
		limit, err := readPressureCgroupV2Level(view, dir, level, root, views)
		if err == nil {
			return limit, nil
		}
		if errors.Is(err, memlimit.ErrNoLimit) {
			// Prefer an explicit memory.max from another view to inferred absence.
			noLimit = true
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if noLimit {
		return 0, nil
	}
	return 0, firstErr
}

func readPressureCgroupV2Level(view pressureCgroupView, dir, level, root string, views []pressureCgroupView) (uint64, error) {
	data, err := view.read(dir, "memory.max")
	if errors.Is(err, os.ErrNotExist) {
		if statErr := view.stat(dir, "cgroup.controllers"); statErr != nil {
			return 0, statErr
		}
		var enabled bool
		var readErr error
		if level == root {
			enabled, readErr = pressureRootMemoryEnabled(view, dir, level)
		} else {
			enabled, readErr = pressureParentMemoryEnabled(filepath.Dir(level), views)
		}
		if readErr != nil {
			return 0, readErr
		}
		if !enabled {
			return 0, memlimit.ErrNoLimit
		}
	}
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(string(data))
	if value == "max" {
		return 0, nil
	}
	limit, err := strconv.ParseUint(value, 10, 63)
	if err != nil || limit == 0 {
		return 0, moerr.NewInternalErrorf(moerr.NoReportContext(), "invalid cgroup v2 memory limit %q", value)
	}
	return limit, nil
}

// At a bind or namespace root, memory.max still exists if memory is
// enabled. Only the global root lacks both memory.max and cgroup.type.
func pressureRootMemoryEnabled(view pressureCgroupView, dir, level string) (bool, error) {
	data, err := view.read(dir, "cgroup.controllers")
	if err != nil {
		return false, err
	}
	if !hasMemoryController(data) {
		return false, nil
	}
	if level == "/" {
		err := view.stat(dir, "cgroup.type")
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func pressureParentMemoryEnabled(level string, views []pressureCgroupView) (bool, error) {
	var firstErr error
	for _, view := range views {
		dir, ok := view.directory(level)
		if !ok {
			continue
		}
		data, err := view.read(dir, "cgroup.subtree_control")
		if err == nil {
			return hasMemoryController(data), nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return false, firstErr
}

func hasMemoryController(data []byte) bool {
	for _, controller := range strings.Fields(string(data)) {
		if strings.TrimPrefix(controller, "+") == "memory" {
			return true
		}
	}
	return false
}

func pressureCgroupV1Limit(group string) (uint64, error) {
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return 0, err
	}
	views, _, err := parsePressureCgroupViews(mountinfo, group, "cgroup")
	if err != nil {
		return 0, err
	}
	return pressureCgroupV1Hierarchy(filepath.Clean(group), views)
}

func pressureCgroupV1Hierarchy(group string, views []pressureCgroupView) (uint64, error) {
	var firstErr error
	for _, view := range views {
		dir, _ := view.directory(group)
		data, err := view.read(dir, "memory.stat")
		if err == nil {
			var limit uint64
			limit, err = parseCgroupV1Limit(data, uint64(os.Getpagesize()))
			if err == nil || errors.Is(err, memlimit.ErrNoLimit) {
				return limit, err
			}
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return 0, firstErr
}

type pressureMount struct {
	id, parent, point string
}

type pressureCgroupView struct {
	root, mount string
	overmounts  []pressureMount
}

func (v pressureCgroupView) file(dir, name string) (string, error) {
	path := filepath.Join(dir, name)
	if slices.ContainsFunc(v.overmounts, func(m pressureMount) bool {
		return cgroupPathContains(m.point, path)
	}) {
		return "", moerr.NewInternalErrorf(moerr.NoReportContext(), "cgroup path %q is covered by another mount", path)
	}
	return path, nil
}

func (v pressureCgroupView) read(dir, name string) ([]byte, error) {
	path, err := v.file(dir, name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func (v pressureCgroupView) stat(dir, name string) error {
	path, err := v.file(dir, name)
	if err != nil {
		return err
	}
	_, err = os.Stat(path)
	return err
}

// A sibling mounted on an ancestor path can hide an existing attachment.
// Stacks at the same path are parent/child and are checked at view selection.
func pressureMountHidden(m pressureMount, mounts map[string]pressureMount, children map[string][]pressureMount) bool {
	for {
		for _, sibling := range children[m.parent] {
			if sibling.point != m.point && cgroupPathContains(sibling.point, m.point) {
				return true
			}
		}
		parent, ok := mounts[m.parent]
		if !ok || parent.id == m.id {
			return false
		}
		m = parent
	}
}

func (v pressureCgroupView) directory(group string) (string, bool) {
	rel, err := filepath.Rel(v.root, group)
	if err != nil || !filepath.IsLocal(rel) {
		return "", false
	}
	return filepath.Join(v.mount, rel), true
}

// A mount can expose just a subtree of the same process hierarchy. Keep all
// reachable views; start at the widest root and preserve order for equal roots.
// The second result is the widest matching root, including covered views.
func parsePressureCgroupViews(data []byte, group, fsType string) ([]pressureCgroupView, string, error) {
	if !inCgroupNamespace(group) {
		return nil, "", moerr.NewInternalErrorf(moerr.NoReportContext(), "cgroup membership %q is outside the visible namespace", group)
	}
	group = filepath.Clean(group)
	mounts := make(map[string]pressureMount)
	children := make(map[string][]pressureMount)
	type candidate struct {
		mount pressureMount
		root  string
	}
	var matched []candidate
	widestRoot := group
	unescape := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\")
	for _, line := range strings.Split(string(data), "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		before, after := strings.Fields(left), strings.Fields(right)
		if len(before) < 5 || len(after) < 3 {
			continue
		}
		mount := pressureMount{
			id:     before[0],
			parent: before[1],
			point:  filepath.Clean(unescape.Replace(before[4])),
		}
		mounts[mount.id] = mount
		if mount.id != mount.parent {
			children[mount.parent] = append(children[mount.parent], mount)
		}
		if after[0] != fsType || fsType == "cgroup" && !slices.Contains(strings.Split(after[2], ","), "memory") {
			continue
		}
		root := unescape.Replace(before[3])
		// An inherited mount may be rooted at /.. in this cgroup namespace.
		// Cleaning it would alias a different group to the namespace root.
		if !inCgroupNamespace(root) {
			continue
		}
		root = filepath.Clean(root)
		if cgroupPathContains(root, group) {
			if len(root) < len(widestRoot) {
				widestRoot = root
			}
			matched = append(matched, candidate{mount: mount, root: root})
		}
	}
	if len(matched) == 0 {
		return nil, "", moerr.NewInternalErrorf(moerr.NoReportContext(), "memory cgroup mount not found for %q", group)
	}
	views := make([]pressureCgroupView, 0, len(matched))
	for _, candidate := range matched {
		m := candidate.mount
		overmounts := children[m.id]
		// A mount stacked at the candidate's mountpoint hides its entire view.
		// Mounts below it can leave ancestors visible and are checked per file.
		if pressureMountHidden(m, mounts, children) || slices.ContainsFunc(overmounts, func(child pressureMount) bool {
			return child.point == m.point
		}) {
			continue
		}
		views = append(views, pressureCgroupView{
			root:       candidate.root,
			mount:      m.point,
			overmounts: overmounts,
		})
	}
	if len(views) == 0 {
		return nil, widestRoot, moerr.NewInternalErrorf(moerr.NoReportContext(), "all memory cgroup mounts for %q are covered by another mount", group)
	}
	slices.SortStableFunc(views, func(a, b pressureCgroupView) int {
		return cmp.Compare(len(a.root), len(b.root))
	})
	return views, widestRoot, nil
}

func inCgroupNamespace(path string) bool {
	return filepath.IsAbs(path) && !slices.Contains(strings.Split(path, "/"), "..")
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
				return 0, moerr.NewInternalError(moerr.NoReportContext(), "invalid cgroup v1 memory limit")
			}
			return value, nil
		}
	}
	return 0, moerr.NewInternalError(moerr.NoReportContext(), "cgroup hierarchical memory limit missing")
}
