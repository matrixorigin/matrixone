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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type controllerGate struct {
	gate, event *os.File
	reader      *bufio.Reader
	child       []*os.File
}

func newControllerGate() (*controllerGate, error) {
	gr, gw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	er, ew, err := os.Pipe()
	if err != nil {
		_ = gr.Close()
		_ = gw.Close()
		return nil, err
	}
	return &controllerGate{gw, er, bufio.NewReaderSize(er, 256), []*os.File{gr, ew}}, nil
}
func (g *controllerGate) closeChild() {
	for _, f := range g.child {
		_ = f.Close()
	}
	g.child = nil
}
func (g *controllerGate) close() { g.closeChild(); _ = g.gate.Close(); _ = g.event.Close() }
func (g *controllerGate) wait(ctx context.Context, event string) error {
	readerCtx, cancel := context.WithCancel(ctx)
	type message struct {
		line string
		err  error
	}
	messages := make(chan message, 32)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		for {
			line, err := g.reader.ReadSlice('\n')
			m := message{string(line), err}
			select {
			case messages <- m:
			case <-readerCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		_ = g.event.SetReadDeadline(time.Now())
		<-joined
		_ = g.event.SetReadDeadline(time.Time{})
	}()
	active := map[int]activeQuery{}
	timer := time.NewTimer(10 * time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			return errors.New("independent controller query/startup deadline expired")
		case m := <-messages:
			if m.err != nil {
				return m.err
			}
			if m.line == event+"\n" {
				if len(active) != 0 {
					return errors.New("worker crossed a gate with active queries")
				}
				return nil
			}
			now := time.Now()
			if err := advanceProgress(active, strings.TrimSpace(m.line), now); err != nil {
				return err
			}
			next := nextProgressDeadline(active, now)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(max(time.Until(next), time.Nanosecond))
		}
	}
}

type activeQuery struct {
	number   int
	deadline time.Time
}

func nextProgressDeadline(active map[int]activeQuery, now time.Time) time.Time {
	if len(active) == 0 {
		return now.Add(10 * time.Minute)
	}
	var next time.Time
	for _, q := range active {
		if next.IsZero() || q.deadline.Before(next) {
			next = q.deadline
		}
	}
	return next
}

func advanceProgress(active map[int]activeQuery, line string, now time.Time) error {
	fields := strings.Fields(line)
	if len(fields) != 4 {
		return errors.New("invalid worker progress event")
	}
	stream, err := strconv.Atoi(fields[1])
	if err != nil || stream < 0 || stream > 7 {
		return errors.New("invalid progress stream")
	}
	query, err := strconv.Atoi(fields[2])
	if err != nil || query < 1 || query > 22 {
		return errors.New("invalid progress query")
	}
	limit, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return err
	}
	switch fields[0] {
	case "start":
		if _, ok := active[stream]; ok || limit <= 0 || limit > int64(30*time.Minute) {
			return errors.New("duplicate/unbounded query admission")
		}
		active[stream] = activeQuery{query, now.Add(time.Duration(limit) + 30*time.Second)}
	case "end":
		q, ok := active[stream]
		if !ok || q.number != query {
			return errors.New("unmatched query completion")
		}
		delete(active, stream)
	default:
		return errors.New("unknown progress event")
	}
	return nil
}
func (g *controllerGate) release() error { _, err := g.gate.Write([]byte{'g'}); return err }

type deployment struct {
	Engine    Engine `json:"engine"`
	Root      string `json:"root"`
	Launch    string `json:"launch,omitempty"`
	SQLPort   int    `json:"sql_port,omitempty"`
	DebugPort int    `json:"debug_port,omitempty"`
}

func loadDeployment(c Campaign, scale, engine string, rep int) deployment {
	root := filepath.Join(c.Root, "engines", "sf"+scale, engine, fmt.Sprintf("load-%d", rep))
	e := Engine{Name: engine, Threads: 8, MemoryBytes: c.MemoryBytes}
	d := deployment{Engine: e, Root: root}
	if engine == "duckdb" {
		d.Engine.Database = filepath.Join(root, "duckdb.db")
		return d
	}
	h := sha256.Sum256([]byte(root))
	d.Engine.Database = "tpch_compare_" + hex.EncodeToString(h[:8])
	d.Engine.DSNEnv = "TPCH_COMPARE_OWNED_MO_DSN"
	d.SQLPort = c.PortBase + 300
	d.DebugPort = c.PortBase + 301
	d.Launch = filepath.Join(root, "launch.toml")
	return d
}

func prepareDeployment(c Campaign, scale, engine string, rep int) (deployment, error) {
	d := loadDeployment(c, scale, engine, rep)
	root := d.Root
	if err := os.MkdirAll(filepath.Dir(root), 0750); err != nil {
		return deployment{}, err
	}
	if err := os.Mkdir(root, 0750); err != nil {
		return deployment{}, err
	}
	if err := publishJSON(filepath.Join(root, "owner.json"), map[string]any{"purpose": "tpch-compare deployment", "engine": engine, "scale": scale}); err != nil {
		return deployment{}, err
	}
	if engine == "duckdb" {
		return d, nil
	}
	data := filepath.Join(root, "data")
	for _, name := range []string{"cn", "tn", "log"} {
		input, err := os.ReadFile(filepath.Join(c.MOSource, "etc", "launch", name+".toml"))
		if err != nil {
			return d, err
		}
		text := strings.ReplaceAll(string(input), "./mo-data", data)
		text = strings.ReplaceAll(text, "status-port = 7001", fmt.Sprintf("status-port = %d", c.PortBase+302))
		port := c.PortBase
		if name == "tn" {
			port += 100
		}
		if name == "log" {
			port += 200
		}
		text = regexp.MustCompile(`port-base\s*=\s*[0-9]+`).ReplaceAllString(text, fmt.Sprintf("port-base = %d", port))
		text += fmt.Sprintf("\n[hakeeper-client]\nservice-addresses = [\"127.0.0.1:%d\"]\n", c.PortBase+201)
		if name == "cn" {
			text += fmt.Sprintf("\n[cn.frontend]\nport = %d\nhost = \"127.0.0.1\"\nunix-socket = \"mysql.sock\"\n", d.SQLPort)
		}
		if name == "log" {
			text += fmt.Sprintf("\n[logservice]\nservice-host = \"127.0.0.1\"\nlogservice-port = %d\nraft-port = %d\ngossip-port = %d\ngossip-seed-addresses = [\"127.0.0.1:%d\"]\n", c.PortBase+201, c.PortBase+200, c.PortBase+202, c.PortBase+202)
		}
		if err := os.WriteFile(filepath.Join(root, name+".toml"), []byte(text), 0600); err != nil {
			return d, err
		}
	}
	text := fmt.Sprintf("logservices = [%q]\ntnservices = [%q]\ncnservices = [%q]\n", filepath.Join(root, "log.toml"), filepath.Join(root, "tn.toml"), filepath.Join(root, "cn.toml"))
	return d, os.WriteFile(d.Launch, []byte(text), 0600)
}

func (d deployment) environment(cores int) []string {
	env := []string{fmt.Sprintf("GOMAXPROCS=%d", cores), "GOGC=100", "TZ=UTC", "LC_ALL=C"}
	if d.Engine.Name == "mo" {
		cfg := mysql.NewConfig()
		cfg.User = "root"
		cfg.Passwd = "111"
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", d.SQLPort)
		env = append(env, d.Engine.DSNEnv+"="+cfg.FormatDSN())
	}
	return env
}

func waitMO(ctx context.Context, p *managedProcess, d deployment) error {
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Passwd = "111"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", d.SQLPort)
	cfg.Timeout = time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-p.done:
			return errors.Join(errors.New("MO exited before readiness"), p.waitErr)
		case <-tick.C:
			probe, cancel := context.WithTimeoutCause(ctx, time.Second, errors.New("MO readiness probe deadline"))
			err := db.PingContext(probe)
			cancel()
			if err == nil {
				return nil
			}
		}
	}
}

type ArmRecord struct {
	Version            int            `json:"version"`
	Trial              Trial          `json:"trial"`
	Engine             string         `json:"engine"`
	Request            FileID         `json:"request"`
	Worker             FileID         `json:"worker_receipt"`
	Started            time.Time      `json:"started"`
	StartupNS          int64          `json:"startup_ns"`
	StartupToResultNS  int64          `json:"startup_to_result_ns"`
	WaveNS             int64          `json:"wave_ns"`
	Before             Usage          `json:"before"`
	After              Usage          `json:"after"`
	Trailing           Usage          `json:"after_cleanup"`
	DiskBytes          int64          `json:"allocated_disk_bytes"`
	CacheResidentBytes uint64         `json:"cache_resident_bytes,omitempty"`
	CacheTotalBytes    uint64         `json:"cache_total_bytes,omitempty"`
	Status             string         `json:"status"`
	Error              string         `json:"error,omitempty"`
	Cleanup            string         `json:"cleanup"`
	Profile            *ProfileRecord `json:"profile,omitempty"`
}

func runArm(ctx context.Context, c Campaign, p Preflight, manager *cgroupManager, d deployment, t Trial, root string, request WorkerRequest) (record ArmRecord, retErr error) {
	budget := 2 * time.Hour
	if request.Operation == "load" || request.Operation == "verify" {
		budget = 4 * time.Hour
	}
	armCtx, cancel := context.WithTimeoutCause(ctx, budget, errors.New("arm control deadline"))
	defer cancel()
	ctx = armCtx
	record = ArmRecord{Version: formatVersion, Trial: t, Engine: d.Engine.Name, Started: time.Now().UTC(), Status: "failed", Cleanup: "failed"}
	if err := os.MkdirAll(root, 0750); err != nil {
		return record, err
	}
	defer func() {
		if retErr != nil {
			record.Status = "failed"
			record.Error = retErr.Error()
		}
		if err := publishJSON(filepath.Join(root, "arm.json"), record); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	group, err := manager.group(t, d.Engine.Name, c.CPUs)
	if err != nil {
		return record, err
	}
	var server, worker *managedProcess
	defer func() { retErr = errors.Join(retErr, group.close()) }()
	defer func() {
		record.Trailing, err = group.snapshot(false)
		retErr = errors.Join(retErr, err)
		if cleanupErr := group.quiescent(server, worker); cleanupErr == nil {
			record.Cleanup = "complete"
		} else {
			record.Cleanup = "failed"
			retErr = errors.Join(retErr, cleanupErr)
		}
	}()
	watchCtx, stopWatch := context.WithCancelCause(ctx)
	watchDone := make(chan struct{})
	defer func() {
		stopWatch(nil)
		<-watchDone
		if cause := context.Cause(watchCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			retErr = errors.Join(retErr, cause)
		}
	}()
	go func() {
		defer close(watchDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-tick.C:
				used, e := diskUsage(d.Root)
				if e == nil {
					var outBytes int64
					outBytes, e = diskUsage(root)
					used += outBytes
				}
				free, freeErr := availableDisk(c.Root)
				if e != nil && !errors.Is(e, os.ErrNotExist) {
					stopWatch(e)
					cancel()
					_ = group.kill()
					return
				}
				if freeErr != nil || used > c.DiskBytes || free < c.ReserveBytes {
					stopWatch(errors.New("disk allowance or reserve exhausted"))
					cancel()
					_ = group.kill()
					return
				}
			}
		}
	}()
	if t.Experiment == "process-cold-os" {
		if err := evictTree(d.Root); err != nil {
			return record, err
		}
		record.CacheResidentBytes, record.CacheTotalBytes, err = residentDataPages(ctx, d.Root)
		if err != nil {
			return record, fmt.Errorf("cold-cache verification unavailable: %w", err)
		}
		if record.CacheResidentBytes != 0 {
			return record, errors.New("owned data pages remain resident before cold startup")
		}
	}
	armStart := time.Now()
	if d.Engine.Name == "mo" {
		if err := requireFreePorts(c); err != nil {
			return record, err
		}
		args := []string{c.MOBinary, "-launch", d.Launch, "-debug-http", fmt.Sprintf("127.0.0.1:%d", d.DebugPort)}
		if request.Profile != nil && request.Profile.Kind == "wait" {
			args = append(args, "-block-profile-rate", "1", "-mutex-profile-fraction", "1")
		}
		server, err = startManaged(ctx, args, d.Root, d.environment(t.Cores), group, filepath.Join(root, "service.log"), nil)
		if err != nil {
			return record, err
		}
		defer func() {
			stopErr := server.stop(group, 30*time.Second)
			if stopErr != nil {
				retErr = errors.Join(retErr, stopErr)
			}
		}()
		ready, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("MO startup deadline"))
		err = waitMO(ready, server, d)
		cancel()
		if err != nil {
			return record, err
		}
	}
	request.Engine = d.Engine
	request.Engine.Threads = t.Cores
	request.Engine.MemoryBytes = t.MemoryBytes
	request.DatasetRoot = c.Datasets[t.Scale]
	request.OutputRoot = filepath.Join(root, "output")
	request.Version = formatVersion
	requestPath := filepath.Join(root, "request.json")
	if err := publishJSON(requestPath, request); err != nil {
		return record, err
	}
	record.Request, err = identify(requestPath)
	if err != nil {
		return record, err
	}
	record.Request.Path = "request.json"
	argv := []string{p.Worker.Path, "worker", "--request", requestPath, "--response", filepath.Join(request.OutputRoot, "receipt.json")}
	var gate *controllerGate
	var extra []*os.File
	if request.Operation == "queries" {
		gate, err = newControllerGate()
		if err != nil {
			return record, err
		}
		defer gate.close()
		extra = gate.child
		argv = append(argv, "--controlled")
	}
	worker, err = startManaged(ctx, argv, d.Root, d.environment(t.Cores), group, filepath.Join(root, "worker.log"), extra)
	if err != nil {
		return record, err
	}
	defer func() {
		if err := worker.stop(group, 30*time.Second); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	if gate != nil {
		gate.closeChild()
		if err := gate.wait(ctx, "ready"); err != nil {
			return record, err
		}
		var profiler *parentProfile
		if request.Profile != nil {
			pids := []int{worker.cmd.Process.Pid}
			if server != nil {
				pids = append(pids, server.cmd.Process.Pid)
			}
			profiler, err = startParentProfile(ctx, *request.Profile, root, pids, d)
			if err != nil {
				return record, err
			}
			defer func() {
				if profiler != nil {
					profile := profiler.stop()
					record.Profile = &profile
				}
			}()
		}
		record.StartupNS = time.Since(armStart).Nanoseconds()
		record.Before, err = group.snapshot(true)
		if err != nil {
			return record, err
		}
		if err := gate.release(); err != nil {
			return record, err
		}
		windowCtx := ctx
		windowCancel := func() {}
		if request.MaxWindowNS > 0 {
			windowCtx, windowCancel = context.WithTimeoutCause(ctx, time.Duration(request.MaxWindowNS), errors.New("profile window cap exceeded"))
		}
		defer windowCancel()
		if err := gate.wait(windowCtx, "done"); err != nil {
			return record, err
		}
		record.StartupToResultNS = time.Since(armStart).Nanoseconds()
		record.After, err = group.snapshot(false)
		if err != nil {
			return record, err
		}
		if profiler != nil {
			_ = profiler.stop()
			profiler.afterSnapshots()
			record.Profile = &profiler.record
			profiler = nil
		}
		if err := gate.release(); err != nil {
			return record, err
		}
	}
	if err := worker.wait(ctx); err != nil {
		return record, err
	}
	workerReceipt, err := readJSON[WorkerReceipt](filepath.Join(request.OutputRoot, "receipt.json"))
	if err != nil {
		return record, err
	}
	record.WaveNS = workerReceipt.WaveNS
	if workerReceipt.Error != "" || workerReceipt.Cleanup != "complete" || workerReceipt.Binary.SHA256 != p.Worker.SHA256 || workerReceipt.Dataset.SHA256 != p.DatasetIDs[t.Scale].SHA256 {
		return record, errors.New("worker failed qualification, provenance or cleanup")
	}
	record.Worker, err = identify(filepath.Join(request.OutputRoot, "receipt.json"))
	if err != nil {
		return record, err
	}
	record.Worker.Path = filepath.Join("output", "receipt.json")
	if server != nil {
		if err := server.stop(group, 30*time.Second); err != nil {
			return record, err
		}
		if server.waitErr != nil {
			return record, fmt.Errorf("MO service shutdown: %w", server.waitErr)
		}
	}
	// Disk/cache bookkeeping assumes the owned deployment has stopped. The
	// deferred check also covers errors and partial initialization after teardown.
	if err := group.quiescent(server, worker); err != nil {
		return record, err
	}
	record.Trailing, err = group.snapshot(false)
	if err != nil {
		return record, err
	}
	if record.Trailing.OOMKills > record.Before.OOMKills {
		return record, errors.New("application cgroup reported an OOM kill")
	}
	record.DiskBytes, err = diskUsage(d.Root)
	if err != nil {
		return record, err
	}
	if record.DiskBytes > c.DiskBytes {
		return record, errors.New("engine exceeded its disk allowance")
	}
	if t.Experiment != "warm-prime" {
		if err := evictTree(d.Root); err != nil {
			return record, err
		}
	}
	if request.Operation == "load" || request.Operation == "verify" {
		if err := evictTree(request.DatasetRoot); err != nil {
			return record, err
		}
	}
	record.Status = "pass"
	return record, nil
}
