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
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Campaign struct {
	Version        int               `json:"version"`
	Root           string            `json:"root"`
	Mode           string            `json:"mode"` // full or sf1-smoke
	Datasets       map[string]string `json:"datasets"`
	MOSource       string            `json:"mo_source"`
	MOBinary       string            `json:"mo_binary"`
	MORevision     string            `json:"mo_revision"`
	MOBuild        string            `json:"mo_build"`
	CPUs           []int             `json:"application_cpus"`
	ControllerCPUs []int             `json:"controller_cpus"`
	MemoryBytes    int64             `json:"memory_bytes"`
	DiskBytes      int64             `json:"disk_bytes_per_engine"`
	ReserveBytes   int64             `json:"disk_reserve_bytes"`
	ProfileBytes   int64             `json:"profile_quota_bytes"`
	PortBase       int               `json:"port_base"`
	Seed           uint64            `json:"seed"`
	ExclusiveHost  bool              `json:"exclusive_host"`
}

func (c Campaign) validate() error {
	if c.Version != formatVersion || !filepath.IsAbs(c.Root) || !filepath.IsAbs(c.MOSource) || !filepath.IsAbs(c.MOBinary) || c.MORevision == "" || c.MOBuild == "" {
		return errors.New("campaign needs absolute paths and explicit MO source/build identity")
	}
	if c.Mode != "full" && c.Mode != "sf1-smoke" {
		return errors.New("mode must be full or sf1-smoke")
	}
	if len(c.CPUs) != 8 || len(c.ControllerCPUs) == 0 || c.MemoryBytes != 32<<30 || c.DiskBytes <= 0 || c.ReserveBytes < 8<<30 || c.ProfileBytes <= 0 || c.PortBase < 10000 || c.PortBase > 64000 {
		return errors.New("invalid frozen resource envelope")
	}
	seen := map[int]bool{}
	for _, list := range [][]int{c.CPUs, c.ControllerCPUs} {
		for _, cpu := range list {
			if cpu < 0 || cpu > 4095 || seen[cpu] {
				return errors.New("CPU sets must be nonempty and disjoint")
			}
			seen[cpu] = true
		}
	}
	if !c.ExclusiveHost {
		return errors.New("an exclusive benchmark window must be declared before admission")
	}
	needed := []string{"1"}
	if c.Mode == "full" {
		needed = []string{"1", "10", "100"}
	}
	if len(c.Datasets) != len(needed) {
		return errors.New("dataset selection differs from declared campaign mode")
	}
	for _, sf := range needed {
		path, ok := c.Datasets[sf]
		if !ok || !filepath.IsAbs(path) {
			return fmt.Errorf("SF%s prepared dataset is required", sf)
		}
	}
	return nil
}

type Trial struct {
	ID          string   `json:"id"`
	Experiment  string   `json:"experiment"`
	Scale       string   `json:"scale"`
	Cores       int      `json:"cores"`
	MemoryBytes int64    `json:"memory_bytes"`
	Clients     int      `json:"clients"`
	Block       int      `json:"block"`
	Queries     []int    `json:"queries"`
	Arms        []string `json:"arms"`
}

func allQueries() []int {
	out := make([]int, 22)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

func schedule(c Campaign) []Trial {
	rng := rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15))
	var out []Trial
	add := func(experiment, sf string, cores int, memory int64, clients, reps int, queries []int) {
		order := make([]bool, reps)
		for i := range order {
			order[i] = i%2 == 0
		}
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for block, moFirst := range order {
			arms := []string{"mo", "duckdb"}
			if !moFirst {
				slices.Reverse(arms)
			}
			id := fmt.Sprintf("%s-sf%s-c%d-m%d-n%d-b%02d", experiment, sf, cores, memory>>30, clients, block)
			if len(queries) == 1 {
				id += fmt.Sprintf("-q%02d", queries[0])
			}
			out = append(out, Trial{id, experiment, sf, cores, memory, clients, block, append([]int(nil), queries...), arms})
		}
	}
	if c.Mode == "sf1-smoke" {
		add("smoke", "1", 8, c.MemoryBytes, 1, 1, allQueries())
		return out
	}
	for _, sf := range []string{"1", "10", "100"} {
		add("warm", sf, 8, c.MemoryBytes, 1, 10, allQueries())
	}
	for _, cores := range []int{1, 2, 4} {
		add("cpu", "10", cores, c.MemoryBytes, 1, 5, allQueries())
	}
	for _, memory := range []int64{8 << 30, 16 << 30} {
		add("memory", "100", 8, memory, 1, 5, allQueries())
	}
	for _, clients := range []int{1, 2, 4, 8} {
		add("concurrency", "10", 8, c.MemoryBytes, clients, 5, allQueries())
	}
	for _, cache := range []string{"process-warm-os", "process-cold-os"} {
		for q := 1; q <= 22; q++ {
			add(cache, "10", 8, c.MemoryBytes, 1, 5, []int{q})
		}
	}
	return out
}

type Pin struct {
	Path string `json:"path"`
	File FileID `json:"file"`
}
type Preflight struct {
	Version    int               `json:"version"`
	Config     FileID            `json:"config"`
	Worker     FileID            `json:"worker"`
	MO         FileID            `json:"mo"`
	DatasetIDs map[string]FileID `json:"datasets"`
	Templates  []Pin             `json:"templates"`
	Hardware   map[string]string `json:"hardware"`
	FreeBytes  int64             `json:"free_bytes"`
	Created    time.Time         `json:"created"`
	Errors     []string          `json:"errors,omitempty"`
}

func preflight(ctx context.Context, path string) (p Preflight, retErr error) {
	c, err := readJSON[Campaign](path)
	if err != nil {
		return p, err
	}
	if err := c.validate(); err != nil {
		return p, err
	}
	p = Preflight{Version: formatVersion, DatasetIDs: map[string]FileID{}, Hardware: map[string]string{}, Created: time.Now().UTC()}
	if err := os.MkdirAll(filepath.Dir(c.Root), 0750); err != nil {
		return p, err
	}
	if err := os.Mkdir(c.Root, 0750); err != nil {
		return p, err
	}
	if _, err := os.Stat(filepath.Join(c.Root, "owner.json")); errors.Is(err, os.ErrNotExist) {
		if err := publishJSON(filepath.Join(c.Root, "owner.json"), map[string]any{"purpose": "tpch-compare campaign", "version": formatVersion}); err != nil {
			return p, err
		}
	}
	p.Config, err = identify(path)
	if err != nil {
		return p, err
	}
	exe, err := os.Executable()
	if err != nil {
		return p, err
	}
	p.Worker, err = identify(exe)
	if err != nil {
		return p, err
	}
	p.MO, err = identify(c.MOBinary)
	if err != nil {
		return p, err
	}
	for sf, root := range c.Datasets {
		d, err := loadDataset(root, false)
		if err != nil {
			return p, err
		}
		if d.Scale != sf {
			return p, errors.New("dataset scale differs from campaign selection")
		}
		id, err := identify(filepath.Join(root, "dataset.json"))
		if err != nil {
			return p, err
		}
		p.DatasetIDs[sf] = id
	}
	for _, name := range []string{"cn", "tn", "log"} {
		path := filepath.Join(c.MOSource, "etc", "launch", name+".toml")
		id, err := identify(path)
		if err != nil {
			return p, err
		}
		p.Templates = append(p.Templates, Pin{path, id})
	}
	if raw, err := commandOutput(ctx, c.MOSource, nil, "git", "rev-parse", "HEAD"); err == nil {
		p.Hardware["mo_checkout_head"] = strings.TrimSpace(string(raw))
	}
	for _, file := range []string{"/proc/cpuinfo", "/proc/meminfo", "/proc/sys/kernel/perf_event_paranoid", "/proc/version"} {
		if b, err := os.ReadFile(file); err == nil {
			p.Hardware[file] = string(b)
		}
	}
	if err := validateTopology(c.CPUs, c.ControllerCPUs); err != nil {
		p.Errors = append(p.Errors, err.Error())
	}
	free, err := availableDisk(c.Root)
	if err != nil {
		return p, err
	}
	p.FreeBytes = free
	if free < 2*c.DiskBytes+c.ReserveBytes+c.ProfileBytes {
		p.Errors = append(p.Errors, "insufficient space for two engine allowances, profile quota and reserve")
	}
	if err := checkPersistentFS(c.Root); err != nil {
		p.Errors = append(p.Errors, err.Error())
	}
	for _, root := range c.Datasets {
		if err := checkPersistentFS(root); err != nil {
			p.Errors = append(p.Errors, err.Error())
		}
	}
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		p.Errors = append(p.Errors, "cgroup v2 is required")
	}
	if err := publishJSON(filepath.Join(c.Root, "preflight.json"), p); err != nil {
		return p, err
	}
	if len(p.Errors) > 0 {
		return p, errors.New(strings.Join(p.Errors, "; "))
	}
	return p, nil
}

func campaignPin(c Campaign, p Preflight) error {
	if p.Version != formatVersion || len(p.Errors) > 0 {
		return errors.New("preflight did not qualify")
	}
	for _, pin := range []FileID{p.MO, p.Worker, p.Config} {
		got, err := identify(pin.Path)
		if err != nil {
			return err
		}
		if got.SHA256 != pin.SHA256 {
			return fmt.Errorf("pinned artifact changed: %s", pin.Path)
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	actual, err := identify(exe)
	if err != nil {
		return err
	}
	if actual.SHA256 != p.Worker.SHA256 {
		return errors.New("controller differs from frozen worker executable")
	}
	for _, pin := range p.Templates {
		actual, err := identify(pin.Path)
		if err != nil {
			return err
		}
		if actual.SHA256 != pin.File.SHA256 {
			return errors.New("MO launch template changed after preflight")
		}
	}
	for sf, pin := range p.DatasetIDs {
		got, err := identify(filepath.Join(c.Datasets[sf], "dataset.json"))
		if err != nil {
			return err
		}
		if got.SHA256 != pin.SHA256 {
			return errors.New("dataset manifest changed after preflight")
		}
	}
	return nil
}

func parseCPUSet(raw string) ([]int, error) {
	var out []int
	for _, group := range strings.Split(strings.TrimSpace(raw), ",") {
		bounds := strings.Split(group, "-")
		start, err := strconv.Atoi(bounds[0])
		if err != nil || start < 0 {
			return nil, errors.New("invalid CPU set")
		}
		end := start
		if len(bounds) > 2 {
			return nil, errors.New("invalid CPU range")
		}
		if len(bounds) == 2 {
			end, err = strconv.Atoi(bounds[1])
			if err != nil || end < start || end-start > 4096 {
				return nil, errors.New("invalid CPU range")
			}
		}
		for cpu := start; cpu <= end; cpu++ {
			out = append(out, cpu)
		}
	}
	return out, nil
}
func cpuSet(cpus []int) string {
	parts := make([]string, len(cpus))
	for i, cpu := range cpus {
		parts[i] = strconv.Itoa(cpu)
	}
	return strings.Join(parts, ",")
}
