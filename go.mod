module github.com/matrixorigin/matrixone

go 1.19

require (
	github.com/BurntSushi/toml v1.0.0
	github.com/FastFilter/xorfilter v0.1.2
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/aws/aws-sdk-go-v2 v1.16.5
	github.com/aws/aws-sdk-go-v2/config v1.15.11
	github.com/aws/aws-sdk-go-v2/credentials v1.12.6
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.11
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.7
	github.com/aws/smithy-go v1.11.3
	github.com/axiomhq/hyperloglog v0.0.0-20220105174342-98591331716a
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/docker/go-units v0.4.0
	github.com/fagongzi/goetty/v2 v2.0.3-0.20221212132037-abf2d4c05484
	github.com/fagongzi/util v0.0.0-20210923134909-bccc37b5040d
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/btree v1.0.1
	github.com/google/gofuzz v1.2.0
	github.com/google/gops v0.3.25
	github.com/google/uuid v1.3.0
	github.com/lni/dragonboat/v4 v4.0.0-20220815145555-6f622e8bcbef
	github.com/lni/goutils v1.3.1-0.20220604063047-388d67b4dbc4
	github.com/matrixorigin/simdcsv v0.0.0-20221106123050-31511b2d3fa8
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826
	github.com/panjf2000/ants/v2 v2.4.6
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pierrec/lz4/v4 v4.1.14
	github.com/plar/go-adaptive-radix-tree v1.0.4
	github.com/prashantv/gostub v1.1.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/samber/lo v1.33.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/stretchr/testify v1.8.0
	github.com/tidwall/btree v1.4.3
	github.com/tidwall/pretty v1.2.1
	github.com/yireyun/go-queue v0.0.0-20220725040158-a4dd64810e1e
	go.opentelemetry.io/proto/otlp v0.19.0
	go.uber.org/ratelimit v0.2.0
	go.uber.org/zap v1.21.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220811171246-fbc7d0a398ab
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.2 // indirect
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
	github.com/cockroachdb/pebble v0.0.0-20220407171941-2120d145e292 // indirect
	github.com/frankban/quicktest v1.14.3 // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/memberlist v0.3.1 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/cockroachdb/errors v1.9.0
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/klauspost/cpuid/v2 v2.0.3 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.26.0
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/shirou/gopsutil/v3 v3.22.4
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/exp v0.0.0-20220414153411-bcd21879b8fd
	golang.org/x/net v0.0.0-20211216030914-fe4d6282115f // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// required until memberlist issue 272 is resolved
// see https://github.com/hashicorp/memberlist/pull/273 for progress
replace github.com/hashicorp/memberlist => github.com/matrixorigin/memberlist v0.4.1-0.20221125074841-7595e1626d36

replace github.com/lni/dragonboat/v4 v4.0.0-20220815145555-6f622e8bcbef => github.com/matrixorigin/dragonboat/v4 v4.0.0-20221226075848-d266f8c2420c

replace github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376 => github.com/matrixorigin/vfs v0.2.1-0.20220616104132-8852fd867376

replace github.com/lni/goutils v1.3.1-0.20220604063047-388d67b4dbc4 => github.com/matrixorigin/goutils v1.3.1-0.20220604063047-388d67b4dbc4
