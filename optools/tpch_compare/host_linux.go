//go:build linux

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

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func availableDisk(path string) (int64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0, err
	}
	return int64(s.Bavail) * s.Bsize, nil
}
func checkPersistentFS(path string) error {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return err
	}
	if s.Type == 0x01021994 {
		return fmt.Errorf("%s uses tmpfs; benchmark data must use persistent NVMe", path)
	}
	return nil
}

func validateTopology(app, controller []int) error {
	cores := map[string]bool{}
	cache := ""
	for _, cpu := range app {
		base := fmt.Sprintf("/sys/devices/system/cpu/cpu%d", cpu)
		core, err := os.ReadFile(filepath.Join(base, "topology", "core_id"))
		if err != nil {
			return err
		}
		socket, err := os.ReadFile(filepath.Join(base, "topology", "physical_package_id"))
		if err != nil {
			return err
		}
		id := string(socket) + ":" + string(core)
		if cores[id] {
			return errors.New("application CPU set contains SMT siblings")
		}
		cores[id] = true
		shared, err := os.ReadFile(filepath.Join(base, "cache", "index3", "shared_cpu_list"))
		if err != nil {
			return err
		}
		if cache != "" && cache != string(shared) {
			return errors.New("application CPUs must share one CCD/L3")
		}
		cache = string(shared)
		siblings, err := os.ReadFile(filepath.Join(base, "topology", "thread_siblings_list"))
		if err != nil {
			return err
		}
		list, err := parseCPUSet(string(siblings))
		if err != nil {
			return err
		}
		for _, c := range controller {
			if slices.Contains(list, c) {
				return errors.New("controller uses an application's SMT sibling")
			}
		}
	}
	return nil
}

type cgroupManager struct{ root, application string }
type appGroup struct {
	path       string
	peak       *os.File
	windowPeak bool
}

func setupCgroups(c Campaign) (*cgroupManager, error) {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	line := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(line, "0::/") {
		return nil, errors.New("unified cgroup v2 membership is required")
	}
	root := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
	if !strings.HasPrefix(filepath.Base(root), "tpch-compare-") {
		return nil, errors.New("run inside systemd-run --user --scope --unit=tpch-compare-NAME -p Delegate=yes")
	}
	controller := filepath.Join(root, "controller")
	if err := os.Mkdir(controller, 0750); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(controller, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+cpu +memory +cpuset +io"), 0600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(controller, "cpuset.cpus"), []byte(cpuSet(c.ControllerCPUs)), 0600); err != nil {
		return nil, err
	}
	app := filepath.Join(root, "application")
	if err := os.Mkdir(app, 0750); err != nil {
		return nil, err
	}
	for key, value := range map[string]string{"cpuset.cpus": cpuSet(c.CPUs), "memory.max": strconv.FormatInt(c.MemoryBytes, 10), "memory.swap.max": "0", "cgroup.subtree_control": "+cpu +memory +cpuset +io"} {
		if err := os.WriteFile(filepath.Join(app, key), []byte(value), 0600); err != nil {
			return nil, err
		}
	}
	return &cgroupManager{root, app}, nil
}

func (m *cgroupManager) group(t Trial, engine string, cpus []int) (*appGroup, error) {
	name := fmt.Sprintf("%s-sf%s-c%d-m%d", engine, t.Scale, t.Cores, t.MemoryBytes>>30)
	path := filepath.Join(m.application, name)
	if err := os.Mkdir(path, 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	procs, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(procs)) != "" {
		return nil, errors.New("previous application generation has not joined")
	}
	for key, value := range map[string]string{"cpuset.cpus": cpuSet(cpus[:t.Cores]), "memory.max": strconv.FormatInt(t.MemoryBytes, 10), "memory.swap.max": "0", "memory.oom.group": "1"} {
		if err := os.WriteFile(filepath.Join(path, key), []byte(value), 0600); err != nil {
			return nil, err
		}
	}
	peak, _ := os.OpenFile(filepath.Join(path, "memory.peak"), os.O_RDWR, 0)
	return &appGroup{path: path, peak: peak}, nil
}
func (g *appGroup) close() error {
	if g.peak != nil {
		return g.peak.Close()
	}
	return nil
}
func (g *appGroup) kill() error {
	return os.WriteFile(filepath.Join(g.path, "cgroup.kill"), []byte("1"), 0600)
}

// Quiescence requires every managed Wait/log owner to finish as well as empty
// cgroup membership. An empty group cannot substitute for a failed process join.
func (g *appGroup) quiescent(processes ...*managedProcess) error {
	for _, p := range processes {
		if p == nil {
			continue
		}
		select {
		case <-p.done:
		default:
			return errors.New("managed process has not joined")
		}
	}
	procs, err := os.ReadFile(filepath.Join(g.path, "cgroup.procs"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(procs)) != "" {
		return errors.New("application generation still owns processes")
	}
	return nil
}

type Usage struct {
	CPUUsec     uint64            `json:"cpu_usec"`
	UserUsec    uint64            `json:"user_usec"`
	SystemUsec  uint64            `json:"system_usec"`
	MemoryBytes uint64            `json:"memory_current_bytes"`
	PeakBytes   uint64            `json:"memory_peak_bytes"`
	PeakQuality string            `json:"peak_quality"`
	ReadBytes   uint64            `json:"read_bytes"`
	WriteBytes  uint64            `json:"write_bytes"`
	OOMKills    uint64            `json:"oom_kills"`
	Raw         map[string]string `json:"raw"`
}

func numericFields(raw string) map[string]uint64 {
	out := map[string]uint64{}
	for _, line := range strings.Split(raw, "\n") {
		p := strings.Fields(line)
		if len(p) == 2 {
			n, err := strconv.ParseUint(p[1], 10, 64)
			if err == nil {
				out[p[0]] = n
			}
		}
	}
	return out
}
func (g *appGroup) snapshot(reset bool) (Usage, error) {
	u := Usage{Raw: map[string]string{}, PeakQuality: "lifetime"}
	for _, key := range []string{"cpu.stat", "memory.current", "memory.events", "io.stat", "memory.stat", "cpu.pressure", "memory.pressure", "io.pressure", "cpuset.cpus.effective", "memory.max", "memory.swap.max"} {
		raw, err := os.ReadFile(filepath.Join(g.path, key))
		if err != nil {
			return u, err
		}
		u.Raw[key] = string(raw)
	}
	cpu := numericFields(u.Raw["cpu.stat"])
	for _, key := range []string{"usage_usec", "user_usec", "system_usec"} {
		if _, ok := cpu[key]; !ok {
			return u, fmt.Errorf("missing/malformed cpu.stat %s", key)
		}
	}
	u.CPUUsec = cpu["usage_usec"]
	u.UserUsec = cpu["user_usec"]
	u.SystemUsec = cpu["system_usec"]
	u.OOMKills = numericFields(u.Raw["memory.events"])["oom_kill"]
	var err error
	u.MemoryBytes, err = strconv.ParseUint(strings.TrimSpace(u.Raw["memory.current"]), 10, 64)
	if err != nil {
		return u, err
	}
	if g.peak != nil {
		if reset {
			if _, err := g.peak.WriteAt([]byte("0"), 0); err == nil {
				g.windowPeak = true
			}
		}
		b := make([]byte, 64)
		n, err := g.peak.ReadAt(b, 0)
		if err == nil || n > 0 {
			u.PeakBytes, err = strconv.ParseUint(strings.TrimSpace(string(b[:n])), 10, 64)
			if err != nil {
				return u, err
			}
		}
		if g.windowPeak {
			u.PeakQuality = "window"
		}
	}
	for _, line := range strings.Split(u.Raw["io.stat"], "\n") {
		for _, field := range strings.Fields(line) {
			p := strings.SplitN(field, "=", 2)
			if len(p) != 2 {
				continue
			}
			n, _ := strconv.ParseUint(p[1], 10, 64)
			switch p[0] {
			case "rbytes":
				u.ReadBytes += n
			case "wbytes":
				u.WriteBytes += n
			}
		}
	}
	return u, nil
}

type managedProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	waitErr  error
	log      *os.File
	stopOnce sync.Once
}

func startManaged(ctx context.Context, argv []string, dir string, env []string, group *appGroup, logPath string, extra []*os.File) (*managedProcess, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty managed command")
	}
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = dir
	baseEnv := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GOMEMLIMIT=") {
			baseEnv = append(baseEnv, entry)
		}
	}
	cmd.Env = append(baseEnv, env...)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.ExtraFiles = extra
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	var cg *os.File
	if group != nil {
		cg, err = os.Open(group.path)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		defer cg.Close()
		cmd.SysProcAttr.UseCgroupFD = true
		cmd.SysProcAttr.CgroupFD = int(cg.Fd())
	}
	if err := ctx.Err(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	p := &managedProcess{cmd: cmd, done: make(chan struct{}), log: f}
	go func() { p.waitErr = cmd.Wait(); p.waitErr = errors.Join(p.waitErr, f.Close()); close(p.done) }()
	return p, nil
}

func (p *managedProcess) wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.waitErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (p *managedProcess) stop(group *appGroup, grace time.Duration) error {
	p.stopOnce.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-p.done:
			return
		case <-timer.C:
			if group != nil {
				_ = group.kill()
			} else {
				_ = p.cmd.Process.Kill()
			}
		}
	})
	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Second, errors.New("process did not join after termination"))
	defer cancel()
	err := p.wait(ctx)
	if ctx.Err() != nil {
		return err
	}
	return nil
}

func diskUsage(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		s, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("allocated disk accounting unavailable")
		}
		total += s.Blocks * 512
		return nil
	})
	return total, err
}

// evictTree operates only on a task-owned stopped deployment. No host-wide
// cache drop or unrelated process termination is used.
func evictTree(root string) error {
	return filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := f.Sync(); err != nil {
			return err
		}
		_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, 4, 0, 0)
		if errno != 0 {
			return errno
		}
		return nil
	})
}

func residentDataPages(ctx context.Context, root string) (resident, total uint64, retErr error) {
	// fincore uses mincore and includes per-file residency. It does not read
	// data bytes into the page cache. Missing support makes cold trials N/A.
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		probe, cancel := context.WithTimeoutCause(ctx, 10*time.Second, errors.New("cache residency probe deadline"))
		defer cancel()
		raw, err := commandOutput(probe, "", nil, "fincore", "--bytes", "--noheadings", "--output", "RES,SIZE", path)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(raw)))
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) != 2 {
				return errors.New("unrecognized fincore result")
			}
			r, err := strconv.ParseUint(fields[0], 10, 64)
			if err != nil {
				return err
			}
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
			resident += r
			total += n
		}
		return scanner.Err()
	})
	return resident, total, err
}
