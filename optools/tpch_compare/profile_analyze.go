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
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type CounterMetric struct {
	Name         string  `json:"name"`
	Value        string  `json:"value"`
	Unit         string  `json:"unit"`
	RunningRatio float64 `json:"running_ratio"`
	Quality      string  `json:"quality"`
}

func parsePerfStat(r io.Reader) ([]CounterMetric, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	reader.Comment = '#'
	var out []CounterMetric
	for {
		fields, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(fields) < 5 {
			continue
		}
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		ratio, err := strconv.ParseFloat(fields[4], 64)
		quality := "measured"
		if err != nil || ratio < 90 || strings.HasPrefix(fields[0], "<") {
			quality = "unavailable_or_multiplexed"
		}
		if _, err := strconv.ParseFloat(fields[0], 64); err != nil {
			quality = "unavailable"
		}
		out = append(out, CounterMetric{fields[2], fields[0], fields[1], ratio / 100, quality})
	}
	if len(out) == 0 {
		return nil, errors.New("no usable perf counter records")
	}
	return out, nil
}

type StackEvidence struct {
	Samples       int64             `json:"samples"`
	WeightNS      uint64            `json:"sample_weight_ns"`
	KnownWeightNS uint64            `json:"known_leaf_weight_ns"`
	KnownRatio    float64           `json:"known_leaf_ratio"`
	Stacks        map[string]uint64 `json:"folded_stacks"`
	Hot           map[string]uint64 `json:"exclusive_leaf_weight_ns"`
	Quality       string            `json:"quality"`
	RawSymbols    map[string]string `json:"raw_symbols,omitempty"`
	CallGraph     string            `json:"callgraph_coverage,omitempty"`
	Limitations   []string          `json:"limitations,omitempty"`
}

var foldedFrameEscaper = strings.NewReplacer("%", "%25", ";", "%3B", "\n", "%0A", "\r", "%0D")

func parsePerfScript(r io.Reader) (out StackEvidence, retErr error) {
	return parsePerfScriptContext(context.Background(), r, 256<<20)
}

func parsePerfScriptContext(ctx context.Context, r io.Reader, limit int64) (out StackEvidence, retErr error) {
	out = StackEvidence{Stacks: map[string]uint64{}, Hot: map[string]uint64{}, Quality: "partial"}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var frames []string
	var weight uint64
	var pid, headerLeaf string
	var retained, pending int64
	flush := func() error {
		if weight == 0 {
			return nil
		}
		if weight > ^uint64(0)-out.WeightNS {
			return errors.New("CPU sample weights overflow")
		}
		out.Samples++
		out.WeightNS += weight
		if len(frames) == 0 {
			if headerLeaf == "" {
				headerLeaf = "[unknown]"
			}
			frames = []string{headerLeaf}
		}
		leaf := frames[0]
		hotKey := "pid=" + pid + " " + leaf
		if _, ok := out.Hot[hotKey]; !ok {
			retained += int64(len(hotKey)) + 128
		}
		out.Hot[hotKey] += weight
		if leaf != "unknown" && !strings.Contains(leaf, "[unknown]") {
			out.KnownWeightNS += weight
		}
		slices.Reverse(frames)
		for i, raw := range frames {
			label := foldedFrameEscaper.Replace(raw)
			if label != raw {
				if out.RawSymbols == nil {
					out.RawSymbols = map[string]string{}
				}
				if _, ok := out.RawSymbols[label]; !ok {
					retained += int64(len(label)+len(raw)) + 128
					out.RawSymbols[label] = raw
				}
			}
			frames[i] = label
			if retained > limit {
				return errors.New("stack analysis exceeds retention bound")
			}
		}
		key := "pid=" + pid + ";" + strings.Join(frames, ";")
		if _, ok := out.Stacks[key]; !ok {
			retained += int64(len(key)) + 128
		}
		out.Stacks[key] += weight
		weight = 0
		frames = nil
		headerLeaf = ""
		pending = 0
		if retained > limit {
			return errors.New("stack analysis exceeds retention bound")
		}
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return out, context.Cause(ctx)
		}
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			if err := flush(); err != nil {
				return out, err
			}
			continue
		}
		event := -1
		for i, field := range fields {
			if strings.HasPrefix(field, "cpu-clock") {
				event = i
				break
			}
		}
		if event >= 0 {
			if err := flush(); err != nil {
				return out, err
			}
			pid = strings.Split(fields[0], "/")[0]
			if _, err := strconv.ParseUint(pid, 10, 64); err != nil {
				return out, errors.New("invalid perf process identity")
			}
			if event < 1 {
				return out, errors.New("CPU sample period missing")
			}
			var err error
			weight, err = strconv.ParseUint(fields[event-1], 10, 64)
			if err != nil || weight == 0 {
				return out, errors.New("invalid CPU sample period")
			}
			// With -G, perf prints IP/symbol on the sample header instead of
			// following callchain lines. Full conversion may also supply both.
			tail := line[strings.Index(line, fields[event])+len(fields[event]):]
			headerLeaf = perfFrame(tail)
			pending = int64(len(headerLeaf))
			if retained+pending > limit {
				return out, errors.New("stack analysis exceeds retention bound")
			}
			continue
		}
		if weight > 0 && len(fields) >= 2 {
			if symbol := perfFrame(line); symbol != "" {
				frames = append(frames, symbol)
				pending += int64(len(symbol)) + 64
				if len(frames) > 512 {
					return out, errors.New("stack depth exceeds bound")
				}
			}
		}
		if retained+pending > limit {
			return out, errors.New("stack analysis exceeds retention bound")
		}
	}
	if err := scanner.Err(); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, context.Cause(ctx)
	}
	if err := flush(); err != nil {
		return out, err
	}
	if out.WeightNS == 0 {
		return out, errors.New("CPU samples/weights unavailable")
	}
	out.KnownRatio = float64(out.KnownWeightNS) / float64(out.WeightNS)
	if out.KnownRatio >= 0.9 {
		out.Quality = "symbolized_leaf"
	}
	return out, nil
}

func perfFrame(line string) string {
	line = strings.TrimSpace(line)
	end := strings.IndexAny(line, " \t")
	if end < 0 {
		return ""
	}
	ip, symbol := line[:end], line[end+1:]
	if _, err := strconv.ParseUint(strings.TrimPrefix(ip, "0x"), 16, 64); err != nil {
		return ""
	}
	symbol = strings.TrimSpace(symbol)
	if at := strings.LastIndex(symbol, " ("); at >= 0 {
		symbol = symbol[:at]
	}
	if at := strings.Index(symbol, "+0x"); at >= 0 {
		symbol = symbol[:at]
	}
	return strings.Clone(strings.TrimSpace(symbol))
}

func commandToFile(ctx context.Context, output string, limit int64, args ...string) (retErr error) {
	if len(args) == 0 {
		return errors.New("empty profile command")
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = &limitedWriter{Writer: f, Remaining: limit}
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Run(); err != nil {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return errors.Join(err, context.Cause(ctx))
	}
	return nil
}

func analyzePerf(ctx context.Context, root string) error {
	return analyzePerfWithCommand(ctx, root, commandToFile)
}

type profileCommand func(context.Context, string, int64, ...string) error

func analyzePerfWithCommand(ctx context.Context, root string, command profileCommand) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	var resultErr error
	if f, err := os.Open(filepath.Join(root, "perf.stat")); err == nil {
		values, parseErr := parsePerfStat(f)
		closeErr := f.Close()
		resultErr = errors.Join(resultErr, parseErr, closeErr)
		if parseErr == nil {
			resultErr = errors.Join(resultErr, publishJSON(filepath.Join(root, "counters.json"), values))
		}
	}
	if _, err := os.Stat(filepath.Join(root, "perf.record")); err == nil {
		bounded, cancel := context.WithTimeoutCause(ctx, time.Minute, errors.New("perf postprocessing deadline"))
		defer cancel()
		path := filepath.Join(root, "perf-script.txt")
		fullCtx, fullCancel := context.WithTimeoutCause(bounded, 30*time.Second, errors.New("full callgraph conversion deadline"))
		args := []string{"perf", "script", "-i", filepath.Join(root, "perf.record"), "-F", "pid,tid,time,period,event,ip,sym,dso"}
		fullErr := command(fullCtx, path, 1<<30, args...)
		fullCancel()
		if fullErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("full callgraph conversion incomplete: %w", fullErr))
			if bounded.Err() != nil {
				return errors.Join(resultErr, context.Cause(bounded))
			}
			path = filepath.Join(root, "perf-script-leaf.txt")
			leafCtx, leafCancel := context.WithTimeoutCause(bounded, 15*time.Second, errors.New("leaf conversion deadline"))
			err := command(leafCtx, path, 1<<30, append(args, "-G")...)
			leafCancel()
			if err != nil {
				return errors.Join(resultErr, err)
			}
		}
		f, err := os.Open(path)
		if err != nil {
			return errors.Join(resultErr, err)
		}
		evidence, err := parsePerfScriptContext(bounded, f, 256<<20)
		err = errors.Join(err, f.Close())
		if err != nil {
			return errors.Join(resultErr, err)
		}
		evidence.CallGraph = "retained"
		if fullErr != nil {
			evidence.CallGraph = "unavailable; leaf-only recovery"
			evidence.Quality = "partial_callgraph"
			evidence.Limitations = []string{fullErr.Error()}
		}
		if err := publishJSON(filepath.Join(root, "stacks.json"), evidence); err != nil {
			return errors.Join(resultErr, err)
		}
		folded, err := os.OpenFile(filepath.Join(root, "stacks.folded"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return errors.Join(resultErr, err)
		}
		writer := bufio.NewWriter(folded)
		keys := make([]string, 0, len(evidence.Stacks))
		for key := range evidence.Stacks {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			if bounded.Err() != nil {
				return errors.Join(resultErr, context.Cause(bounded), folded.Close())
			}
			if _, err := fmt.Fprintf(writer, "%s %d\n", key, evidence.Stacks[key]); err != nil {
				return errors.Join(resultErr, err, folded.Close())
			}
		}
		err = errors.Join(writer.Flush(), folded.Close())
		if err != nil {
			return errors.Join(resultErr, err)
		}
		if script := os.Getenv("TPCH_COMPARE_FLAMEGRAPH"); script != "" {
			renderCtx, renderCancel := context.WithTimeoutCause(bounded, 5*time.Second, errors.New("flame graph rendering deadline"))
			title := "Whole application CPU samples"
			if fullErr != nil {
				title += " (leaf only)"
			}
			err := command(renderCtx, filepath.Join(root, "cpu.svg"), 8<<20, "perl", script, "--countname", "CPU nanoseconds", "--title", title, filepath.Join(root, "stacks.folded"))
			renderCancel()
			if err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}
	return resultErr
}

type NativeMetricMeaning struct {
	Unit        string `json:"unit"`
	Scope       string `json:"scope"`
	Aggregation string `json:"aggregation"`
	Quality     string `json:"quality"`
}

func metricMeanings() map[string]NativeMetricMeaning {
	return map[string]NativeMetricMeaning{
		"mo.TimeConsumed":                  {"nanoseconds", "producer-local exclusive active elapsed; excludes owned waits/children", "sum across calls/workers; not OS CPU", "coverage-dependent"},
		"mo.WaitTimeConsumed":              {"nanoseconds", "observed operator waits", "overlapping worker intervals", "coverage-dependent"},
		"mo.MemorySize":                    {"bytes", "producer-specific allocation/max accounting", "mixed producer semantics; not whole-app peak", "partial"},
		"mo.DiskIO":                        {"unknown", "inspected AddDiskIO has no implementation", "not available", "unavailable; zero is not zero I/O"},
		"duckdb.CPU_TIME":                  {"seconds", "cumulative operator elapsed timings", "sum of operator/child timings; excludes planning", "not OS CPU"},
		"duckdb.OPERATOR_TIMING":           {"seconds", "native operator timing", "parallel timings can overlap", "native definition"},
		"duckdb.SYSTEM_PEAK_BUFFER_MEMORY": {"bytes", "buffer manager", "query peak", "not total native memory"},
		"application.cpu.stat":             {"microseconds", "all application processes/threads", "window delta", "OS CPU accounting"},
		"application.memory.peak":          {"bytes", "all memory charged to application cgroup", "simultaneous peak; reset FD where supported", "see peak_quality"},
	}
}

func workFamily(name string) string {
	s := strings.ToLower(name)
	switch {
	case strings.Contains(s, "scan") || strings.Contains(s, "read"):
		return "scan/decode"
	case strings.Contains(s, "join") || strings.Contains(s, "hashbuild"):
		return "join"
	case strings.Contains(s, "agg") || strings.Contains(s, "group"):
		return "aggregate/merge"
	case strings.Contains(s, "sort") || strings.Contains(s, "topn") || strings.Contains(s, "top_n"):
		return "sort/top-N"
	case strings.Contains(s, "filter") || strings.Contains(s, "project"):
		return "expression/filter"
	case strings.Contains(s, "material") || strings.Contains(s, "exchange") || strings.Contains(s, "dispatch"):
		return "materialize/exchange"
	case strings.HasPrefix(s, "runtime."):
		return "runtime"
	default:
		return "unmapped"
	}
}

func writeProfileSummary(c Campaign, root string, m ProfileManifest) (retErr error) {
	if err := publishJSON(filepath.Join(root, "metric-semantics.json"), metricMeanings()); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(root, "profiles.md"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	_, _ = fmt.Fprintln(w, "# TPC-H diagnostic profiles\n\nThese target-warm, steady-repeat replays are separate from scored timing. CPU sampling and a small PMU group share the CPU lens; allocation and wait lenses run independently. Raw counter scopes and symbolization coverage remain visible. Operator timings are not OS CPU.\n\n| Scale | Query | Engine | Lens | Status | Complete calls | CPU s/call | Wall s/call | Evidence |\n|---|---|---|---|---|---:|---:|---:|---|")
	for _, a := range m.Attempts {
		if err := a.Arm.verify(root); err != nil {
			return err
		}
		path := filepath.Join(root, a.Arm.Path)
		record, err := readJSON[ArmRecord](path)
		if err != nil {
			return err
		}
		calls := 0
		wall := 0.0
		if receipt, _, err := readArm(filepath.Dir(path), record); err == nil {
			calls = len(receipt.Queries)
			for _, q := range receipt.Queries {
				wall += float64(q.Capture.ElapsedNS) / 1e9
			}
		}
		cpu := math.NaN()
		if calls > 0 && record.After.CPUUsec >= record.Before.CPUUsec {
			cpu = float64(record.After.CPUUsec-record.Before.CPUUsec) / 1e6 / float64(calls)
			wall /= float64(calls)
		}
		_, _ = fmt.Fprintf(w, "| %s | Q%d | %s | %s | %s | %d | %s | %s | [%s](%s) |\n", a.Scale, a.Query, a.Engine, a.Kind, a.Status, calls, fmtNumber(cpu), fmtNumber(wall), a.Arm.Path, a.Arm.Path)
		if a.Error != "" {
			_, _ = fmt.Fprintf(w, "\nCapture limitation: %s\n\n", strings.ReplaceAll(a.Error, "\n", " "))
		}
	}
	_, _ = fmt.Fprintln(w, "\nGo heap/GC in the DuckDB worker belongs to its Go client, while C++ allocations require native allocator coverage. Native heap allocation bytes remain unavailable when no validated allocator collector is installed. Copy samples show CPU cost, not copied-byte counts. Aggregate operator JSON cannot reconstruct a pipeline critical path; OS context-switch records and Go wait evidence retain their distinct scopes.")
	_ = c
	return w.Flush()
}

func decodeNativeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

type NativeWork struct {
	Path   string         `json:"path"`
	Name   string         `json:"name"`
	Family string         `json:"family"`
	Facts  map[string]any `json:"facts"`
}

func nativeWork(value any, path string, out *[]NativeWork) {
	switch node := value.(type) {
	case map[string]any:
		name := ""
		for _, key := range []string{"operator_name", "operator", "name", "nodeType", "operatorType"} {
			if s, ok := node[key].(string); ok {
				name = s
				break
			}
		}
		if name != "" {
			facts := map[string]any{}
			for key, v := range node {
				switch v.(type) {
				case string, json.Number, bool, nil:
					facts[key] = v
				default:
					if key == "extra_info" || key == "labels" || key == "inputs" || key == "table_names" || key == "projection" || key == "statistics" || key == "order_by_specs" {
						facts[key] = v
					}
				}
			}
			*out = append(*out, NativeWork{path, name, workFamily(name), facts})
		}
		keys := make([]string, 0, len(node))
		for key := range node {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			nativeWork(node[key], path+"/"+key, out)
		}
	case []any:
		for i, v := range node {
			nativeWork(v, fmt.Sprintf("%s/%d", path, i), out)
		}
	}
}

func normalizeNativeCapture(root, engine string) error {
	pattern := "native-*-statement-*.json"
	if engine == "mo" {
		pattern = "native-*-plan.json"
	}
	paths, err := filepath.Glob(filepath.Join(root, "output", pattern))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("native structured plan/profile missing")
	}
	var work []NativeWork
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.Size() > 16<<20 {
			return errors.New("native profile exceeds decode bound")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		value, err := decodeNativeJSON(raw)
		if err != nil {
			return err
		}
		nativeWork(value, filepath.Base(path), &work)
	}
	if len(work) == 0 {
		return errors.New("native plan has no recognized operator records; preserve raw evidence")
	}
	return publishJSON(filepath.Join(root, "native-work.json"), work)
}
