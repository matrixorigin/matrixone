// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"strings"
	"text/template"
)

type templateArgs struct {
	I           int
	ID          uint64
	DataDir     string
	ServicePort int
}

var templateFuncs = map[string]any{
	"NextBasePort": getNextBasePort,
}

var (
	logConfig = template.Must(template.New("log").Funcs(templateFuncs).Parse(`
service-type = "LOG"
data-dir = "{{.DataDir}}"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
  "127.0.0.1:{{.ServicePort}}",
]
`))

	tnConfig = template.Must(template.New("tn").Funcs(templateFuncs).Parse(`
service-type = "TN"
data-dir = "{{.DataDir}}"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
  "127.0.0.1:{{.ServicePort}}",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "S3"
[fileservice.s3]
endpoint = "disk"
bucket = "{{.DataDir}}/s3"

[fileservice.cache]
memory-capacity = "32MB"
disk-capacity = "1GB"
disk-path = "{{.DataDir}}/file-service-cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[tn]
uuid = "{{.ID}}-tn"
port-base = {{NextBasePort}}

[tn.Txn.Storage]
backend = "TAE"
log-backend = "logservice"

[tn.Ckp]
flush-interval = "60s"
min-count = 100
scan-interval = "5s"
incremental-interval = "180s"
global-min-count = 60

[tn.LogtailServer]
rpc-max-message-size = "16KiB"
rpc-payload-copy-buffer-size = "16KiB"
rpc-enable-checksum = true
logtail-collect-interval = "2ms"
logtail-response-send-timeout = "10s"
max-logtail-fetch-failure = 5
`))

	cnConfig = template.Must(template.New("cn").Funcs(templateFuncs).Parse(`
service-type = "CN"
data-dir = "{{.DataDir}}"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
	"127.0.0.1:{{.ServicePort}}",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "S3"
[fileservice.s3]
endpoint = "disk"
bucket = "{{.DataDir}}/s3"

[fileservice.cache]
memory-capacity = "32MB"
disk-capacity = "32MB"
disk-path = "{{.DataDir}}/file-service-cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[cn]
uuid = "{{.I}}-cn-{{.ID}}"
port-base = {{NextBasePort}}

[cn.txn.trace]
dir = "trace{{.I}}"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = {{NextBasePort}}
unix-socket = "{{.DataDir}}/mysql{{.I}}.sock"
`))

	proxyConfig = template.Must(template.New("proxy").Funcs(templateFuncs).Parse(`
service-type = "PROXY"
data-dir = "{{.DataDir}}"

[log]
level = "info"
format = "console"
max-size = 512

[hakeeper-client]
service-addresses = [
  "127.0.0.1:{{.ServicePort}}",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "S3"
[fileservice.s3]
endpoint = "disk"
bucket = "{{.DataDir}}/s3"

[fileservice.cache]
memory-capacity = "32MB"
disk-capacity = "32MB"
disk-path = "{{.DataDir}}file-service-cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[proxy]
uuid = "proxy"
`))
)

func genConfigText(template *template.Template, args templateArgs) string {
	buf := new(strings.Builder)
	if err := template.Execute(buf, args); err != nil {
		panic(err)
	}
	return buf.String()
}
