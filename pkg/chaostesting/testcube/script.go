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
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/starlarkutil"
)

func (_ Def) ScriptBuiltins(
	add fz.AddScriptDef,
) fz.ScriptBuiltins {
	return fz.ScriptBuiltins{
		"parallel": starlarkutil.MakeFunc("parallel", func(n Parallel) {
			add(&n)
		}),

		"timeout_report_threshold": starlarkutil.MakeFunc("timeout_report_threshold", func(n TimeoutReportThreshold) {
			add(&n)
		}),

		"enable_cpu_profile": starlarkutil.MakeFunc("enable_cpu_profile", func(enable EnableCPUProfile) {
			add(&enable)
		}),

		"http_server_addr": starlarkutil.MakeFunc("http_server_addr", func(addr HTTPServerAddr) {
			add(&addr)
		}),

		"enable_runtime_trace": starlarkutil.MakeFunc("enable_runtime_trace", func(enable EnableRuntimeTrace) {
			add(&enable)
		}),

		"enable_fg_profile": starlarkutil.MakeFunc("enable_fg_profile", func(enable EnableFGProfile) {
			add(&enable)
		}),

		"retry_timeout": starlarkutil.MakeFunc("retry_timeout", func(timeout RetryTimeout) {
			add(&timeout)
		}),
	}
}
