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
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ProfileRecord struct {
	Kind       string         `json:"kind"`
	Status     string         `json:"status"`
	Scope      string         `json:"scope"`
	Artifacts  []FileID       `json:"artifacts,omitempty"`
	Errors     []string       `json:"errors,omitempty"`
	PIDs       []int          `json:"pids,omitempty"`
	Collectors []int          `json:"collector_pids,omitempty"`
	Events     []string       `json:"events,omitempty"`
	Roles      map[int]string `json:"process_roles,omitempty"`
	Cleanup    string         `json:"cleanup"`
	Retained   string         `json:"retained_temporary_directory,omitempty"`
}

type perfCapture struct {
	process      *managedProcess
	control, ack *os.File
	reader       *bufio.Reader
	path         string
}

func startPerf(ctx context.Context, kind, events, tmp, root string, pids []int) (*perfCapture, error) {
	ctlRead, ctlWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	ackRead, ackWrite, err := os.Pipe()
	if err != nil {
		_ = ctlRead.Close()
		_ = ctlWrite.Close()
		return nil, err
	}
	closeAll := func() { _ = ctlRead.Close(); _ = ctlWrite.Close(); _ = ackRead.Close(); _ = ackWrite.Close() }
	ids := make([]string, len(pids))
	for i, pid := range pids {
		ids[i] = strconv.Itoa(pid)
	}
	path := filepath.Join(tmp, kind)
	args := []string{"perf", kind, "-p", strings.Join(ids, ","), "--control", "fd:3,4", "-D", "-1", "-o", path}
	if kind == "record" {
		args = append(args, "-e", "cpu-clock:u", "-F", "99", "--user-callchains", "--call-graph", "dwarf,16384", "--switch-events", "--clockid", "mono")
	} else {
		args = append(args, "-x", ",", "-e", events)
	}
	p, err := startManaged(ctx, args, "", nil, nil, filepath.Join(root, "perf-"+kind+".log"), []*os.File{ctlRead, ackWrite})
	_ = ctlRead.Close()
	_ = ackWrite.Close()
	if err != nil {
		closeAll()
		return nil, err
	}
	c := &perfCapture{p, ctlWrite, ackRead, bufio.NewReaderSize(ackRead, 64), path}
	if err := c.command("enable"); err != nil {
		_ = p.stop(nil, 0)
		closeAll()
		detail, _ := os.ReadFile(filepath.Join(root, "perf-"+kind+".log"))
		if len(detail) > 4096 {
			detail = detail[:4096]
		}
		return nil, fmt.Errorf("perf %s unavailable: %w: %s", kind, err, detail)
	}
	return c, nil
}
func (c *perfCapture) command(s string) error {
	if err := c.ack.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(c.control, s); err != nil {
		return err
	}
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	if strings.Trim(string(line), "\x00\r\n ") != "ack" {
		return fmt.Errorf("invalid perf acknowledgment %q", line)
	}
	return nil
}
func (c *perfCapture) stop() error {
	err := c.command("disable")
	_ = c.control.Close()
	_ = c.ack.Close()
	_ = c.process.cmd.Process.Signal(syscall.SIGINT)
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, errors.New("perf flush deadline"))
	defer cancel()
	waitErr := c.process.wait(ctx)
	var exit *exec.ExitError
	if errors.As(waitErr, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGINT {
			waitErr = nil
		}
	}
	if ctx.Err() != nil {
		waitErr = errors.Join(waitErr, c.process.stop(nil, 0))
	}
	return errors.Join(err, waitErr)
}

func (c *perfCapture) joined() bool {
	select {
	case <-c.process.done:
		return true
	default:
		return false
	}
}

type parentProfile struct {
	spec      ProfileSpec
	root, tmp string
	captures  []*perfCapture
	record    ProfileRecord
	server    deployment
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	limitHit  chan error
}

func startParentProfile(ctx context.Context, spec ProfileSpec, root string, pids []int, server deployment) (*parentProfile, error) {
	tmp, err := os.MkdirTemp("/tmp", "tpch-compare-profile-")
	if err != nil {
		return nil, err
	}
	profileCtx, cancel := context.WithCancel(ctx)
	p := &parentProfile{spec: spec, root: root, tmp: tmp, server: server, ctx: ctx, cancel: cancel, done: make(chan struct{}), limitHit: make(chan error, 1), record: ProfileRecord{Kind: spec.Kind, Status: "complete", Scope: "whole application; user-mode PMU/sampling", PIDs: pids}}
	p.record.Roles = map[int]string{}
	for i, pid := range pids {
		role := "MO client"
		if server.Engine.Name == "duckdb" {
			role = "Go client and embedded C++ engine"
		} else if i > 0 {
			role = "MO services"
		}
		p.record.Roles[pid] = role
	}
	fail := func(err error) { p.record.Status = "partial"; p.record.Errors = append(p.record.Errors, err.Error()) }
	if spec.Kind == "cpu" || spec.Kind == "branches" || spec.Kind == "cache" || spec.Kind == "wait" {
		events := "{cycles:u,instructions:u}"
		if spec.Kind == "branches" {
			events = "{branches:u,branch-misses:u}"
		}
		if spec.Kind == "cache" {
			events = "{cache-references:u,cache-misses:u}"
		}
		if spec.Kind != "wait" {
			capture, err := startPerf(ctx, "stat", events, tmp, root, pids)
			if err != nil {
				fail(err)
			} else {
				p.captures = append(p.captures, capture)
				p.record.Collectors = append(p.record.Collectors, capture.process.cmd.Process.Pid)
				p.record.Events = append(p.record.Events, events)
			}
		}
		if spec.Kind == "cpu" || spec.Kind == "wait" {
			capture, err := startPerf(ctx, "record", "", tmp, root, pids)
			if err != nil {
				fail(err)
			} else {
				p.captures = append(p.captures, capture)
				p.record.Collectors = append(p.record.Collectors, capture.process.cmd.Process.Pid)
			}
		}
	}
	if (spec.Kind == "alloc" || spec.Kind == "wait") && server.Engine.Name == "mo" {
		if err := p.moSnapshots("before"); err != nil {
			fail(err)
		}
	}
	go func() {
		defer close(p.done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-profileCtx.Done():
				return
			case <-tick.C:
				for _, capture := range p.captures {
					info, err := os.Stat(capture.path)
					if err == nil && info.Size() > 1<<30 {
						p.limitHit <- errors.New("profiler capture exceeded 1 GiB")
						_ = capture.process.cmd.Process.Signal(syscall.SIGINT)
						return
					}
				}
			}
		}
	}()
	return p, nil
}

func (p *parentProfile) moSnapshots(prefix string) error {
	kinds := []string{"heap", "allocs"}
	if p.spec.Kind == "wait" {
		kinds = []string{"block", "mutex", "goroutine"}
	}
	for _, kind := range kinds {
		ctx, cancel := context.WithTimeoutCause(p.ctx, 15*time.Second, errors.New("MO pprof snapshot deadline"))
		err := downloadProfile(ctx, fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/%s", p.server.DebugPort, kind), filepath.Join(p.root, "mo-"+prefix+"-"+kind+".pb.gz"))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func downloadProfile(ctx context.Context, url, path string) (retErr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("profile endpoint returned %s", response.Status)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	_, err = io.Copy(&limitedWriter{Writer: f, Remaining: 256 << 20}, response.Body)
	return err
}

func (p *parentProfile) stop() (record ProfileRecord) {
	p.cancel()
	<-p.done
	select {
	case err := <-p.limitHit:
		p.record.Status = "partial"
		p.record.Errors = append(p.record.Errors, err.Error())
	default:
	}
	for _, capture := range p.captures {
		if err := capture.stop(); err != nil {
			p.record.Status = "partial"
			p.record.Errors = append(p.record.Errors, err.Error())
		}
		if !capture.joined() {
			// A surviving writer still owns its temporary file. Preserve its
			// location and block arm reuse instead of archiving mutable bytes.
			continue
		}
		name := "perf." + filepath.Base(capture.path)
		dest := filepath.Join(p.root, name)
		if err := copyFile(capture.path, dest, 1<<30); err != nil {
			p.record.Status = "partial"
			p.record.Errors = append(p.record.Errors, err.Error())
			continue
		}
		id, err := identify(dest)
		if err != nil {
			p.record.Status = "partial"
			p.record.Errors = append(p.record.Errors, err.Error())
		} else {
			id.Path = name
			p.record.Artifacts = append(p.record.Artifacts, id)
		}
	}
	p.finishCleanup()
	return p.record
}

func (p *parentProfile) finishCleanup() {
	for _, capture := range p.captures {
		if !capture.joined() {
			p.record.Cleanup = "failed"
			p.record.Status = "partial"
			p.record.Retained = p.tmp
			return
		}
	}
	p.record.Cleanup = "complete"
	if err := os.RemoveAll(p.tmp); err != nil {
		p.record.Cleanup = "failed"
		p.record.Status = "partial"
		p.record.Retained = p.tmp
		p.record.Errors = append(p.record.Errors, err.Error())
	}
}

func (p *parentProfile) afterSnapshots() {
	if (p.spec.Kind == "alloc" || p.spec.Kind == "wait") && p.server.Engine.Name == "mo" {
		if err := p.moSnapshots("after"); err != nil {
			p.record.Status = "partial"
			p.record.Errors = append(p.record.Errors, err.Error())
		}
	}
}
