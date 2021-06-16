module matrixone

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/aws/aws-sdk-go v1.37.14 // indirect
	github.com/cockroachdb/pebble v0.0.0-20210526183633-dd2a545f5d75
	github.com/fagongzi/goetty v1.8.0
	github.com/fagongzi/log v0.0.0-20201106014031-b41ebf3bd287 // indirect
	github.com/fagongzi/util v0.0.0-20210409031311-a10fdf8fbd7a
	github.com/frankban/quicktest v1.11.3 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.2.0
	github.com/matrixorigin/matrixcube v0.0.0-20210611054819-ffd6d1e59f32
	github.com/orcaman/concurrent-map v0.0.0-20210501183033-44dafcb38ecc
	github.com/pierrec/lz4 v2.6.0+incompatible
	github.com/pilosa/pilosa v1.4.1
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad // indirect
	github.com/pingcap/parser v0.0.0-20210310110710-c7333a4927e6
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.7.0
	github.com/traetox/goaio v0.0.0-20171005222435-46641abceb17 // indirect
	go.uber.org/zap v1.15.0 // indirect
	golang.org/x/sys v0.0.0-20200930185726-fdedc70b468f
	golang.org/x/tools v0.0.0-20201105001634-bc3cf281b174 // indirect
)

replace go.etcd.io/etcd => github.com/deepfabric/etcd v3.4.15+incompatible
