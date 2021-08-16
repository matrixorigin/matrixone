module matrixone

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/RoaringBitmap/roaring v0.8.0
	github.com/cockroachdb/errors v1.8.1
	github.com/cockroachdb/pebble v0.0.0-20210526183633-dd2a545f5d75
	github.com/fagongzi/goetty v1.9.0
	github.com/fagongzi/log v0.0.0-20201106014031-b41ebf3bd287
	github.com/fagongzi/util v0.0.0-20210409031311-a10fdf8fbd7a
	github.com/frankban/quicktest v1.11.3 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.2.0 // indirect
	github.com/matrixorigin/matrixcube v0.0.0-20210816141818-5d69b76cebaa
	github.com/panjf2000/ants/v2 v2.4.5
	github.com/pierrec/lz4 v2.6.0+incompatible
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7
	github.com/pingcap/parser v0.0.0-20210310110710-c7333a4927e6
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.7.0
	github.com/yireyun/go-queue v0.0.0-20210520035143-72b190eafcba
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b // indirect
	golang.org/x/sys v0.0.0-20200930185726-fdedc70b468f
	golang.org/x/tools v0.0.0-20201105001634-bc3cf281b174 // indirect
)

replace go.etcd.io/etcd => github.com/deepfabric/etcd v1.4.15
