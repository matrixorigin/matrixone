module github.com/matrixorigin/matrixone

go 1.18

require (
	github.com/BurntSushi/toml v1.0.0
	github.com/FastFilter/xorfilter v0.1.1
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/aws/aws-sdk-go-v2 v1.16.5
	github.com/aws/aws-sdk-go-v2/config v1.15.11
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.11
	github.com/axiomhq/hyperloglog v0.0.0-20220105174342-98591331716a
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cockroachdb/pebble v0.0.0-20220407171941-2120d145e292
	github.com/fagongzi/goetty v1.13.0
	github.com/fagongzi/goetty/v2 v2.0.3-0.20220629100032-f8e8c3c38772
	github.com/fagongzi/util v0.0.0-20210923134909-bccc37b5040d
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/btree v1.0.1
	github.com/google/gofuzz v1.2.0
	github.com/google/uuid v1.3.0
	github.com/lni/dragonboat/v4 v4.0.0-20220620120319-6de09e19d1d8
	github.com/lni/goutils v1.3.1-0.20220604063047-388d67b4dbc4
	github.com/matrixorigin/matrixcube v0.3.1-0.20220606032431-c944d801f1e5
	github.com/matrixorigin/simdcsv v0.0.0-20210926114300-591bf748a770
	github.com/minio/minio-go/v7 v7.0.27
	github.com/panjf2000/ants/v2 v2.4.6
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pierrec/lz4/v4 v4.1.14
	github.com/plar/go-adaptive-radix-tree v1.0.4
	github.com/prashantv/gostub v1.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/goconvey v1.7.2
	github.com/stretchr/testify v1.7.5
	github.com/yireyun/go-queue v0.0.0-20210520035143-72b190eafcba
	go.etcd.io/etcd/raft/v3 v3.5.1
	go.uber.org/zap v1.21.0
	golang.org/x/sys v0.0.0-20220622161953-175b2fd9d664
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.2 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.7 // indirect
	github.com/aws/smithy-go v1.11.3 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/memberlist v0.3.1 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/minio/md5-simd v1.1.0 // indirect
	github.com/minio/sha256-simd v0.1.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/rs/xid v1.2.1 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	gopkg.in/ini.v1 v1.57.0 // indirect
)

require (
	cloud.google.com/go v0.99.0 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/cockroachdb/errors v1.9.0
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/frankban/quicktest v1.14.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/juju/ratelimit v1.0.2-0.20191002062651-f60b32039441 // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/klauspost/cpuid/v2 v2.0.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d // indirect
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/shirou/gopsutil/v3 v3.22.3
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/api/v3 v3.5.1 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.1 // indirect
	go.etcd.io/etcd/client/v2 v2.305.1 // indirect
	go.etcd.io/etcd/client/v3 v3.5.1 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.1 // indirect
	go.etcd.io/etcd/server/v3 v3.5.1 // indirect
	go.opentelemetry.io/contrib v0.20.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0 // indirect
	go.opentelemetry.io/otel v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/export/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v0.20.0 // indirect
	go.opentelemetry.io/proto/otlp v0.7.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/exp v0.0.0-20220414153411-bcd21879b8fd
	golang.org/x/net v0.0.0-20211216030914-fe4d6282115f // indirect
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa // indirect
	google.golang.org/grpc v1.43.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace go.etcd.io/etcd/raft/v3 => github.com/matrixorigin/etcd/raft/v3 v3.5.1-0.20210824022435-0203115049c2

replace go.etcd.io/etcd/v3 => github.com/matrixorigin/etcd/v3 v3.5.1-0.20210824022435-0203115049c2
