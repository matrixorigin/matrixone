module matrixone

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/cockroachdb/errors v1.8.2 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210526183633-dd2a545f5d75
	github.com/fagongzi/goetty v1.9.0
	github.com/fagongzi/util v0.0.0-20210409031311-a10fdf8fbd7a
	github.com/frankban/quicktest v1.11.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/kr/text v0.2.0 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible
	github.com/pingcap/parser v0.0.0-20210310110710-c7333a4927e6
	github.com/traetox/goaio v0.0.0-20171005222435-46641abceb17 // indirect
	go.uber.org/zap v1.17.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20210816071009-649d0fc2fce7
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace go.etcd.io/etcd/raft/v3 => github.com/matrixorigin/etcd/raft/v3 v3.5.1-0.20210824022435-0203115049c2

replace go.etcd.io/etcd/v3 => github.com/matrixorigin/etcd/v3 v3.5.1-0.20210824022435-0203115049c2
