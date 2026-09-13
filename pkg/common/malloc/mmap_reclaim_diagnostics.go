// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package malloc

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"
)

// Only the rate-limited retry worker calls this. Never read smaps (page-table
// walks) or allocate a maps-sized buffer under memory pressure. Counting maps
// is best effort: bound bytes and elapsed work, explicitly label partial counts.
func mmapReclaimDiagnostics() string {
	if runtime.GOOS != "linux" {
		return "proc diagnostics unavailable on " + runtime.GOOS
	}
	read := func(path string) string {
		f, err := os.Open(path)
		if err != nil {
			return err.Error()
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, 16<<10))
		if err != nil {
			return err.Error()
		}
		return strings.TrimSpace(string(data))
	}
	count, complete := 0, false
	f, err := os.Open("/proc/self/maps")
	if err == nil {
		defer f.Close()
		reader := &io.LimitedReader{R: f, N: 128 << 20}
		scanner := bufio.NewScanner(reader)
		deadline := time.Now().Add(250 * time.Millisecond)
		for scanner.Scan() {
			count++
			if count%1024 == 0 && time.Now().After(deadline) {
				break
			}
		}
		complete = scanner.Err() == nil && reader.N > 0 && time.Now().Before(deadline)
	}
	var memory strings.Builder
	for _, line := range strings.Split(read("/proc/self/status"), "\n") {
		if strings.HasPrefix(line, "Vm") || strings.HasPrefix(line, "Rss") || strings.HasPrefix(line, "Threads:") {
			memory.WriteString(strings.TrimSpace(line))
			memory.WriteString("; ")
		}
	}
	return fmt.Sprintf("maps=%d maps_complete=%t max_map_count=%s kernel=%s memory=[%s] cgroup=[%s]",
		count, complete, read("/proc/sys/vm/max_map_count"), read("/proc/sys/kernel/osrelease"),
		memory.String(), read("/proc/self/cgroup"))
}
