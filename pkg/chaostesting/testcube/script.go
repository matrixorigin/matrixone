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

package main

import (
	"errors"
	"os"
	"time"

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/starlarkutil"
	"go.starlark.net/starlark"
)

func loadScript() (
	defs []any,
	err error,
) {
	defer he(&err)

	const scriptFileName = "script.py"

	src, err := os.ReadFile(scriptFileName)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	ce(err)

	_, err = starlark.ExecFile(
		new(starlark.Thread),
		scriptFileName,
		src,
		starlark.StringDict{

			"parallel": starlarkutil.MakeFunc("parallel", func(n Parallel) {
				defs = append(defs, &n)
			}),

			"listen_host": starlarkutil.MakeFunc("listen_host", func(host ListenHost) {
				defs = append(defs, &host)
			}),

			"port_range": starlarkutil.MakeFunc("port_range", func(lower int64, upper int64) {
				defs = append(defs, func() PortRange {
					return PortRange{lower, upper}
				})
			}),

			"timeout_report_threshold": starlarkutil.MakeFunc("timeout_report_threshold", func(n TimeoutReportThreshold) {
				defs = append(defs, &n)
			}),

			"execute_timeout": starlarkutil.MakeFunc("execute_timeout", func(t fz.ExecuteTimeout) {
				defs = append(defs, &t)
			}),

			"hour":        starlark.MakeUint64(uint64(time.Hour)),
			"minute":      starlark.MakeUint64(uint64(time.Minute)),
			"second":      starlark.MakeUint64(uint64(time.Second)),
			"millisecond": starlark.MakeUint64(uint64(time.Millisecond)),
			"microsecond": starlark.MakeUint64(uint64(time.Microsecond)),
			"nanosecond":  starlark.MakeUint64(uint64(time.Nanosecond)),

			"enable_cpu_profile": starlarkutil.MakeFunc("enable_cpu_profile", func(enable EnableCPUProfile) {
				defs = append(defs, &enable)
			}),

			"http_server_addr": starlarkutil.MakeFunc("http_server_addr", func(addr HTTPServerAddr) {
				defs = append(defs, &addr)
			}),

			"use_dummy_interface": starlarkutil.MakeFunc("use_dummy_interface", func(use UseDummyInterface) {
				defs = append(defs, &use)
			}),

			//
		},
	)
	ce(err)

	return
}
