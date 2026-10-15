# #30107：有序 FK 有界 SQL 测量原始附件

此附件由 `optools/insert_ignore_fk_bench/report.py` 从成功结束的 JSONL 生成。它不是设计 PASS，接受限制见主设计。

## 来源与计量限定

### base

- 源 revision：`17a6200f59e764729e5544908fffd5480ae88279`；二进制 SHA-256：`8f1f4b7c6207d1671653dfed0a9a96697b7a7d021b15475cc05f004e74a055ff`。
- 原始 JSONL SHA-256：`e4396ffa39233fd11f0638c17334f8c4d34eb8aadc69b1a2aae9cf3dfc12dc73`；成功 samples=270；数据库清理已确认。
- 平台：`macOS-26.6.2-arm64-arm-64bit`；GOMAXPROCS=2，GOGC=50；启动负载：`11:51  up 22 days, 21:42, 24 users, load averages: 16.45 23.96 33.35`。
- 工具链/编译：

```text
<worktree>/mo-service: go1.27.1-X:simd
	path	github.com/matrixorigin/matrixone/cmd/mo-service
	mod	github.com/matrixorigin/matrixone	v0.7.1-0.20261015015045-17a6200f59e7+dirty
	dep	filippo.io/edwards25519	v1.1.1	h1:YpjwWWlNmGIDyXOn8zLzqiD+9TyIlPhGFG96P39uBpw=
	dep	github.com/BurntSushi/toml	v1.2.1	h1:9F2/+DoOYIOksmaJFPw1tGFy1eDnIJXg+UHjuD8lTak=
	dep	github.com/DataDog/zstd	v1.5.0	h1:+K/VEwIAaPcHiMtQvpLD4lqW7f0Gk3xdYZmI1hD+CXo=
	dep	github.com/FastFilter/xorfilter	v0.5.1	h1:UjtPttI1SnKWUmeQKu8Y+4ZvMJv7KP6/NZTsGBPQY08=
	dep	github.com/KimMachineGun/automemlimit	v0.6.0	h1:p/BXkH+K40Hax+PuWWPQ478hPjsp9h1CPDhLlA3Z37E=
	dep	github.com/Masterminds/semver/v3	v3.2.1	h1:RN9w6+7QoMeJVGyfmbcgs28Br8cvmnucEXnY0rYXWg0=
	dep	github.com/RoaringBitmap/roaring/v2	v2.16.0	h1:Kys1UNf49d5W8Tq3bpuAhIr/Z8/yPB+59CO8A6c/BbE=
	dep	github.com/VictoriaMetrics/metrics	v1.18.1	h1:OZ0+kTTto8oPfHnVAnTOoyl0XlRhRkoQrD2n2cOuRw0=
	dep	github.com/alibabacloud-go/debug	v1.0.1	h1:MsW9SmUtbb1Fnt3ieC6NNZi6aEwrXfDksD4QA6GSbPg=
	dep	github.com/alibabacloud-go/tea	v1.2.2	h1:aTsR6Rl3ANWPfqeQugPglfurloyBJY85eFy7Gc1+8oU=
	dep	github.com/aliyun/alibaba-cloud-sdk-go	v1.63.34	h1:eZM2MHY/p4TFO1pGf9O5HiuYE59hwrrkf3HvCtkL5Ok=
	dep	github.com/aliyun/aliyun-oss-go-sdk	v3.0.2+incompatible	h1:8psS8a+wKfiLt1iVDX79F7Y6wUM49Lcha2FMXt4UM8g=
	dep	github.com/aliyun/credentials-go	v1.3.10	h1:45Xxrae/evfzQL9V10zL3xX31eqgLWEaIdCoPipOEQA=
	dep	github.com/andres-erbsen/clock	v0.0.0-20160526145045-9e14626cd129	h1:MzBOUgng9orim59UnfUTLRjMpd09C5uEVQ6RPGeCaVI=
	dep	github.com/andybalholm/brotli	v1.2.2	h1:HzTuoo2ErYQqf5qvcJInB8uvqSVxRttzkFexPWtnceM=
	dep	github.com/apache/arrow-go/v18	v18.7.0	h1:Vw/i+cJyebUofT7JlqFpe65LrmwxULn166jjwStM4HY=
	dep	github.com/armon/go-metrics	v0.4.1	h1:hR91U9KYmb6bLBYLQjyM+3j+rcd/UhE+G78SFnF8gJA=
	dep	github.com/aws/aws-sdk-go	v1.55.5	h1:KKUZBfBoyqy5d3swXyiC7Q76ic40rYcbqH7qjh59kzU=
	dep	github.com/aws/aws-sdk-go-v2	v1.41.5	h1:dj5kopbwUsVUVFgO4Fi5BIT3t4WyqIDjGKCangnV/yY=
	dep	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream	v1.7.8	h1:eBMB84YGghSocM7PsjmmPffTa+1FBUeNvGvFou6V/4o=
	dep	github.com/aws/aws-sdk-go-v2/config	v1.32.14	h1:opVIRo/ZbbI8OIqSOKmpFaY7IwfFUOCCXBsUpJOwDdI=
	dep	github.com/aws/aws-sdk-go-v2/credentials	v1.19.14	h1:n+UcGWAIZHkXzYt87uMFBv/l8THYELoX6gVcUvgl6fI=
	dep	github.com/aws/aws-sdk-go-v2/feature/ec2/imds	v1.18.21	h1:NUS3K4BTDArQqNu2ih7yeDLaS3bmHD0YndtA6UP884g=
	dep	github.com/aws/aws-sdk-go-v2/internal/configsources	v1.4.21	h1:Rgg6wvjjtX8bNHcvi9OnXWwcE0a2vGpbwmtICOsvcf4=
	dep	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2	v2.7.21	h1:PEgGVtPoB6NTpPrBgqSE5hE/o47Ij9qk/SEZFbUOe9A=
	dep	github.com/aws/aws-sdk-go-v2/internal/ini	v1.8.6	h1:qYQ4pzQ2Oz6WpQ8T3HvGHnZydA72MnLuFK9tJwmrbHw=
	dep	github.com/aws/aws-sdk-go-v2/internal/v4a	v1.4.22	h1:rWyie/PxDRIdhNf4DzRk0lvjVOqFJuNnO8WwaIRVxzQ=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding	v1.13.7	h1:5EniKhLZe4xzL7a+fU3C2tfUN4nWIqlLesfrjkuPFTY=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/checksum	v1.9.13	h1:JRaIgADQS/U6uXDqlPiefP32yXTda7Kqfx+LgspooZM=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url	v1.13.21	h1:c31//R3xgIJMSC8S6hEVq+38DcvUlgFY0FM6mSI5oto=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/s3shared	v1.19.21	h1:ZlvrNcHSFFWURB8avufQq9gFsheUgjVD9536obIknfM=
	dep	github.com/aws/aws-sdk-go-v2/service/s3	v1.99.0	h1:hlSuz394kV0vhv9drL5lhuEFbEOEP1VyQpy15qWh1Pk=
	dep	github.com/aws/aws-sdk-go-v2/service/signin	v1.0.9	h1:QKZH0S178gCmFEgst8hN0mCX1KxLgHBKKY/CLqwP8lg=
	dep	github.com/aws/aws-sdk-go-v2/service/sso	v1.30.15	h1:lFd1+ZSEYJZYvv9d6kXzhkZu07si3f+GQ1AaYwa2LUM=
	dep	github.com/aws/aws-sdk-go-v2/service/ssooidc	v1.35.19	h1:dzztQ1YmfPrxdrOiuZRMF6fuOwWlWpD2StNLTceKpys=
	dep	github.com/aws/aws-sdk-go-v2/service/sts	v1.41.10	h1:p8ogvvLugcR/zLBXTXrTkj0RYBUdErbMnAFFp12Lm/U=
	dep	github.com/aws/smithy-go	v1.24.3	h1:XgOAaUgx+HhVBoP4v8n6HCQoTRDhoMghKqw4LNHsDNg=
	dep	github.com/axiomhq/hyperloglog	v0.2.6	h1:sRhvvF3RIXWQgAXaTphLp4yJiX4S0IN3MWTaAgZoRJw=
	dep	github.com/beorn7/perks	v1.0.1	h1:VlbKKnNfV8bJzeqoa4cOKqO6bYr3WgKZxO8Z16+hsOM=
	dep	github.com/bits-and-blooms/bitset	v1.24.2	h1:M7/NzVbsytmtfHbumG+K2bremQPMJuqv1JD3vOaFxp0=
	dep	github.com/blevesearch/mmap-go	v1.2.0	h1:l33nNKPFcBjJUMwem6sAYJPUzhUCABoK9FxZDGiFNBI=
	dep	github.com/blevesearch/vellum	v1.2.0	h1:xkDiOEsHc2t3Cp0NsNZZ36pvc130sCzcGKOPMzXe+e0=
	dep	github.com/buger/jsonparser	v1.1.2	h1:frqHqw7otoVbk5M8LlE/L7HTnIq2v9RX6EJ48i9AxJk=
	dep	github.com/bytedance/gopkg	v0.1.3	h1:TPBSwH8RsouGCBcMBktLt1AymVo2TVsBVCY4b6TnZ/M=
	dep	github.com/bytedance/sonic	v1.15.2	h1:90H+rcF/FwLXwfB1cudOLq/je83n683Utf4Cbp0xHCo=
	dep	github.com/cespare/xxhash/v2	v2.3.0	h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=
	dep	github.com/clbanning/mxj	v1.8.4	h1:HuhwZtbyvyOw+3Z1AowPkU87JkJUSv751ELWaiTpj8I=
	dep	github.com/clipperhouse/uax29/v2	v2.7.0	h1:+gs4oBZ2gPfVrKPthwbMzWZDaAFPGYK72F0NJv2v7Vk=
	dep	github.com/cockroachdb/errors	v1.9.1	h1:yFVvsI0VxmRShfawbt/laCIDy/mtTqqnvoNgiy5bEV8=
	dep	github.com/cockroachdb/logtags	v0.0.0-20230118201751-21c54148d20b	h1:r6VH0faHjZeQy818SGhaone5OnYfxFR/+AzdY3sf5aE=
	dep	github.com/cockroachdb/pebble	v0.0.0-20220407171941-2120d145e292	h1:7fLxcRpQTdi1iic0cIUpK+hKKLM5XdPhEbWRRrxfvLU=
	dep	github.com/cockroachdb/redact	v1.1.3	h1:AKZds10rFSIj7qADf0g46UixK8NNLwWTNdCIGS5wfSQ=
	dep	github.com/colinmarc/hdfs/v2	v2.4.0	h1:v6R8oBx/Wu9fHpdPoJJjpGSUxo8NhHIwrwsfhFvU9W0=
	dep	github.com/cpegeric/pdftotext-go	v0.0.0-20241112123704-49cb86a3790e	h1:tQSCiEjYPRU+AuuVR+zd+xYVOsEqX1clPhmIAM6FCHU=
	dep	github.com/davecgh/go-spew	v1.1.2-0.20180830191138-d8f796af33cc	h1:U9qPSI2PIWSS1VwoXQT9A3Wy9MM3WgvqSxFWenqJduM=
	dep	github.com/detailyang/go-fallocate	v0.0.0-20180908115635-432fa640bd2e	h1:lj77EKYUpYXTd8CD/+QMIf8b6OIOTsfEBSXiAzuEHTU=
	dep	github.com/dgryski/go-metro	v0.0.0-20250106013310-edb8663e5e33	h1:ucRHb6/lvW/+mTEIGbvhcYU3S8+uSNkuMjx/qZFfhtM=
	dep	github.com/dlclark/regexp2	v1.10.0
	=>	github.com/XuPeng-SH/regexp2	v1.10.1-0.20261004024751-718f43e031fe	h1:7TlZYr/D2KQlZ2lwGehHN7Ix3ICj43OXaZx2UT4DOIo=

	dep	github.com/docker/go-units	v0.5.0	h1:69rxXcBk27SvSaaxTtLh/8llcHD8vYHT7WSdRZ/jvr4=
	dep	github.com/dolthub/maphash	v0.1.0	h1:bsQ7JsF4FkkWyrP3oCnFJgrCUAFbFf3kOl4L/QxPDyQ=
	dep	github.com/dslipak/pdf	v0.0.2	h1:djAvcM5neg9Ush+zR6QXB+VMJzR6TdnX766HPIg1JmI=
	dep	github.com/dustin/go-humanize	v1.0.1	h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=
	dep	github.com/dylibso/observe-sdk/go	v0.0.0-20240819160327-2d926c5d788a	h1:UwSIFv5g5lIvbGgtf3tVwC7Ky9rmMFBp0RMs+6f6YqE=
	dep	github.com/elastic/elastic-transport-go/v8	v8.6.0	h1:Y2S/FBjx1LlCv5m6pWAF2kDJAHoSjSRSJCApolgfthA=
	dep	github.com/elastic/go-elasticsearch/v8	v8.15.0	h1:IZyJhe7t7WI3NEFdcHnf6IJXqpRf+8S8QWLtZYYyBYk=
	dep	github.com/elastic/gosigar	v0.14.2
	=>	github.com/matrixorigin/gosigar	v0.14.3-0.20241204071856-40aab500bfac	h1:KRPOwOcSZRfWT7w8mr7BmFLoFB75ljgqjkg47xkN/Pc=

	dep	github.com/extism/go-sdk	v1.6.0	h1:crFRMhjcPAn6R9M4eIvkjHQs7CLBs3yzPqwnj+uwzdg=
	dep	github.com/fagongzi/goetty/v2	v2.0.3-0.20230628075727-26c9a2fd5fb8
	=>	github.com/matrixorigin/goetty/v2	v2.0.0-20260811155919-8edc32ac370a	h1:D8kXjySDJHA8nyZzzi/4cx3KB6qrSPQeugUCLkgoEho=

	dep	github.com/fagongzi/util	v0.0.0-20210923134909-bccc37b5040d	h1:1pILVCatHj3eVo9i52dZyY4BwjTmSIeN+/hoJh8rD0Y=
	dep	github.com/felixge/fgprof	v0.9.6-0.20240831122612-49987e680f04	h1:6exK5FZYMJwsXo4xGqax5rdCHCYeQz+zgTO+Sk+tNHE=
	dep	github.com/getsentry/sentry-go	v0.12.0	h1:era7g0re5iY13bHSdN/xMkyV+5zZppjRVQhZrXCaEIk=
	dep	github.com/go-ini/ini	v1.67.0	h1:z6ZrTEZqSWOTyH2FlglNbNgARyHG8oLW9gMELqKr06A=
	dep	github.com/go-logr/logr	v1.4.3	h1:CjnDlHq8ikf6E492q6eKboGOC0T8CDaOvkHCIg8idEI=
	dep	github.com/go-logr/stdr	v1.2.2	h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=
	dep	github.com/go-sql-driver/mysql	v1.9.3	h1:U/N249h2WzJ3Ukj8SowVFjdtZKfu9vlLZxjPXV1aweo=
	dep	github.com/go-viper/mapstructure/v2	v2.5.0	h1:vM5IJoUAy3d7zRSVtIwQgBj7BiWtMPfmPEgAXnvj1Ro=
	dep	github.com/gobwas/glob	v0.2.3	h1:A4xDbljILXROh+kObIiy5kIaPYD8e96x1tgBhUI5J+Y=
	dep	github.com/goccy/go-json	v0.10.6	h1:p8HrPJzOakx/mn/bQtjgNjdTcN+/S6FcG2CTtQOrHVU=
	dep	github.com/gogo/protobuf	v1.3.2	h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=
	dep	github.com/golang-collections/collections	v0.0.0-20130729185459-604e922904d3	h1:zN2lZNZRflqFyxVaTIU61KNKQ9C0055u9CAfpmqUvo4=
	dep	github.com/golang/geo	v0.0.0-20260526120156-e21b7802b0ee	h1:L0DB+dB7f2E+BvC518GbeU+WgELvWXTrbFiitwnFtuc=
	dep	github.com/golang/glog	v1.2.5	h1:DrW6hGnjIhtvhOIiAKT6Psh/Kd/ldepEa81DKeiRJ5I=
	dep	github.com/golang/snappy	v1.0.0	h1:Oy607GVXHs7RtbggtPBnr2RmDArIsAefDwvrdWvRhGs=
	dep	github.com/google/btree	v1.1.3	h1:CVpQJjYgC4VbzxeGVHfvZrv1ctoYCAI8vbl07Fcxlyg=
	dep	github.com/google/flatbuffers	v25.12.19+incompatible	h1:haMV2JRRJCe1998HeW/p0X9UaMTK6SDo0ffLn2+DbLs=
	dep	github.com/google/go-querystring	v1.1.0	h1:AnCroh3fv4ZBgVIf1Iwtovgjaw/GiKJo8M8yD/fhyJ8=
	dep	github.com/google/pprof	v0.0.0-20260302011040-a15ffb7f9dcc	h1:VBbFa1lDYWEeV5FZKUiYKYT0VxCp9twUmmaq9eb8sXw=
	dep	github.com/google/shlex	v0.0.0-20191202100458-e7afc7fbc510	h1:El6M4kTTCOh6aBiKaUGG7oYTSPP8MxqL4YI3kZKwcP4=
	dep	github.com/google/uuid	v1.6.0	h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=
	dep	github.com/hamba/avro/v2	v2.31.0	h1:wv3nmua7lCEIwWsb6vqsTS3pXktTxcKg5eoyNu0VhrU=
	dep	github.com/hashicorp/errwrap	v1.1.0	h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=
	dep	github.com/hashicorp/go-immutable-radix	v1.3.1	h1:DKHmCUm2hRBK510BaiZlwvpD40f8bJFeZnpfm2KLowc=
	dep	github.com/hashicorp/go-msgpack	v0.5.3	h1:zKjpN5BK/P5lMYrLmBHdBULWbJ0XpYR+7NGzqkZzoD4=
	dep	github.com/hashicorp/go-multierror	v1.1.1	h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=
	dep	github.com/hashicorp/go-sockaddr	v1.0.0	h1:GeH6tui99pF4NJgfnhp+L6+FfobzVW3Ah46sLo0ICXs=
	dep	github.com/hashicorp/go-uuid	v1.0.3	h1:2gKiV6YVmrJ1i2CKKa9obLvRieoRGviZFL26PcT/Co8=
	dep	github.com/hashicorp/golang-lru	v1.0.2	h1:dV3g9Z/unq5DpblPpw+Oqcv4dU/1omnb4Ok8iPY6p1c=
	dep	github.com/hashicorp/memberlist	v0.3.1
	=>	github.com/matrixorigin/memberlist	v0.5.1-0.20230322082342-95015c95ee76	h1:MpmqMPooJ0Ea7W4ldIGbQV4D3z+sEiCu6C6aTibiwiQ=

	dep	github.com/hayageek/threadsafe	v1.0.1	h1:QTMJrninAaGQE7+CYdJWPzTlnhcBPwhxD5GDdvIf5oU=
	dep	github.com/ianlancetaylor/demangle	v0.0.0-20250417193237-f615e6bd150b	h1:ogbOPx86mIhFy764gGkqnkFC8m5PJA7sPzlk9ppLVQA=
	dep	github.com/itchyny/gojq	v0.12.16	h1:yLfgLxhIr/6sJNVmYfQjTIv0jGctu6/DgDoivmxTr7g=
	dep	github.com/itchyny/timefmt-go	v0.1.6	h1:ia3s54iciXDdzWzwaVKXZPbiXzxxnv1SPGFfM/myJ5Q=
	dep	github.com/jackc/pgpassfile	v1.0.0	h1:/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=
	dep	github.com/jackc/pgservicefile	v0.0.0-20221227161230-091c0ba34f0a	h1:bbPeKD0xmW/Y25WS6cokEszi5g+S0QxI/d45PkRi7Nk=
	dep	github.com/jackc/pgx/v5	v5.5.5	h1:amBjrZVmksIdNjxGW/IiIMzxMKZFelXbUoPNb+8sjQw=
	dep	github.com/jackc/puddle/v2	v2.2.1	h1:RhxXJtFG022u4ibrCSMSiu5aOq1i77R3OHKNJj77OAk=
	dep	github.com/jcmturner/aescts/v2	v2.0.0	h1:9YKLH6ey7H4eDBXW8khjYslgyqG2xZikXP0EQFKrle8=
	dep	github.com/jcmturner/dnsutils/v2	v2.0.0	h1:lltnkeZGL0wILNvrNiVCR6Ro5PGU/SeBvVO/8c/iPbo=
	dep	github.com/jcmturner/gofork	v1.7.6	h1:QH0l3hzAU1tfT3rZCnW5zXl+orbkNMMRGJfdJjHVETg=
	dep	github.com/jcmturner/goidentity/v6	v6.0.1	h1:VKnZd2oEIMorCTsFBnJWbExfNN7yZr3EhJAxwOkZg6o=
	dep	github.com/jcmturner/gokrb5/v8	v8.4.4	h1:x1Sv4HaTpepFkXbt2IkL29DXRf8sOfZXo8eRKh687T8=
	dep	github.com/jcmturner/rpc/v2	v2.0.3	h1:7FXXj8Ti1IaVFpSAziCZWNzbNuZmnvw/i6CqLNdWfZY=
	dep	github.com/jmespath/go-jmespath	v0.4.0	h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=
	dep	github.com/jonboulle/clockwork	v0.4.0	h1:p4Cf1aMWXnXAUh8lVfewRBx1zaTSYKrKMF2g3ST4RZ4=
	dep	github.com/json-iterator/go	v1.1.12	h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=
	dep	github.com/kamstrup/intmap	v0.5.2	h1:qnwBm1mh4XAnW9W9Ue9tZtTff8pS6+s6iKF6JRIV2Dk=
	dep	github.com/klauspost/compress	v1.19.0	h1:sXLILfc9jV2QYWkzFOPWStmcUVH2RHEB1JCdY2oVvCQ=
	dep	github.com/klauspost/cpuid/v2	v2.4.0	h1:S6Hrbc7+ywsr0r+RLapfGBHfyefhCTwEh3A0tV913Dw=
	dep	github.com/klauspost/crc32	v1.3.0	h1:sSmTt3gUt81RP655XGZPElI0PelVTZ6YwCRnPSupoFM=
	dep	github.com/kr/pretty	v0.3.1	h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=
	dep	github.com/kr/text	v0.2.0	h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=
	dep	github.com/lmittmann/tint	v1.1.3	h1:Hv4EaHWXQr+GTFnOU4VKf8UvAtZgn0VuKT+G0wFlO3I=
	dep	github.com/lni/dragonboat/v4	v4.0.0-20220815145555-6f622e8bcbef
	=>	github.com/matrixorigin/dragonboat/v4	v4.0.0-20261006150417-abc3d2fce5a6	h1:AQWG/u9fUQvn7WTGRvjV9CkHyNI8mCxpr8qZf3vRAmg=

	dep	github.com/lni/goutils	v1.3.1-0.20220604063047-388d67b4dbc4
	=>	github.com/matrixorigin/goutils	v1.3.1-0.20220604063047-388d67b4dbc4	h1:+SmZP2bG+YcO/ntzjCleCu6hFTJiue7Oj2tftpJKTlU=

	dep	github.com/lni/vfs	v0.2.1-0.20220616104132-8852fd867376
	=>	github.com/matrixorigin/vfs	v0.2.1-0.20220616104132-8852fd867376	h1:1XpEh8nPx1yFTeTFMj/aMipEiilDhQToQS8gjEU2HxM=

	dep	github.com/matrixorigin/mysql	v1.8.2-0.20241106110439-6ac9ee94770d	h1:27vD3JGbrFmaQtDYQT/W1jFFr0xvipdwH5R4bZPGQdE=
	dep	github.com/matrixorigin/simdcsv	v0.0.0-20230210060146-09b8e45209dd	h1:DvqhuH3kOpsE6vXZA5WEaRNAUUUcf44S1p5VInbjdfU=
	dep	github.com/mattn/go-isatty	v0.0.21	h1:xYae+lCNBP7QuW4PUnNG61ffM4hVIfm+zUzDuSzYLGs=
	dep	github.com/miekg/dns	v1.1.53	h1:ZBkuHr5dxHtB1caEOlZTLPo7D3L3TWckgUUs/RHfDxw=
	dep	github.com/minio/crc64nvme	v1.1.1	h1:8dwx/Pz49suywbO+auHCBpCtlW1OfpcLN7wYgVR6wAI=
	dep	github.com/minio/md5-simd	v1.1.2	h1:Gdi1DZK69+ZVMoNHRXJyNcxrMA4dSxoYHZSQbirFg34=
	dep	github.com/minio/minio-go/v7	v7.0.99	h1:2vH/byrwUkIpFQFOilvTfaUpvAX3fEFhEzO+DR3DlCE=
	dep	github.com/mitchellh/mapstructure	v1.5.0	h1:jeMsZIYE/09sWLaz43PL7Gy6RuMjD2eJVyuac5Z2hdY=
	dep	github.com/modern-go/concurrent	v0.0.0-20180306012644-bacd9c7ef1dd	h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=
	dep	github.com/modern-go/reflect2	v1.0.3-0.20250322232337-35a7c28c31ee	h1:W5t00kpgFdJifH4BDsTlE89Zl93FEloxaWZfGcifgq8=
	dep	github.com/mohae/deepcopy	v0.0.0-20170929034955-c48cc78d4826	h1:RWengNIwukTxcDr9M+97sNutRR1RKhG96O6jWumTTnw=
	dep	github.com/mozillazg/go-httpheader	v0.2.1	h1:geV7TrjbL8KXSyvghnFm+NyTux/hxwueTSrwhe88TQQ=
	dep	github.com/mschoch/smat	v0.2.0	h1:8imxQsjDm8yFEAVBe7azKmKSgzSkZXDuKkSq9374khM=
	dep	github.com/munnerz/goautoneg	v0.0.0-20191010083416-a7dc8b61c822	h1:C3w9PqII01/Oq1c1nUAm88MOHcQC9l5mIlSMApZMrHA=
	dep	github.com/ncruces/go-dns	v1.2.5	h1:d2zZVWUassOyUOc8okB70hXBZrupqkuls7C7ZXMaKMM=
	dep	github.com/openacid/slimarray	v0.1.3	h1:+/+G8k+Nz4p8QUj4J2kd7IzFC5DiJzk5H2QPp/BpHHk=
	dep	github.com/opentracing/opentracing-go	v1.2.1-0.20220228012449-10b1cf09e00b	h1:FfH+VrHHk6Lxt9HdVS0PXzSXFyS2NbZKXv33FYPol0A=
	dep	github.com/panjf2000/ants/v2	v2.12.0	h1:u9JhESo83i/GkZnhfTNuFMMWcNt7mnV1bGJ6FT4wXH8=
	dep	github.com/parquet-go/parquet-go	v0.25.1	h1:l7jJwNM0xrk0cnIIptWMtnSnuxRkwq53S+Po3KG8Xgo=
	dep	github.com/pbnjay/memory	v0.0.0-20210728143218-7b4eea64cf58	h1:onHthvaw9LFnH4t2DcNVpwGmV9E1BkGknEliJkfwQj0=
	dep	github.com/petermattis/goid	v0.0.0-20260330135022-df67b199bc81	h1:WDsQxOJDy0N1VRAjXLpi8sCEZRSGarLWQevDxpTBRrM=
	dep	github.com/philhofer/fwd	v1.2.0	h1:e6DnBTl7vGY+Gz322/ASL4Gyp1FspeMvx1RNDoToZuM=
	dep	github.com/pierrec/lz4/v4	v4.1.27	h1:+PhzhWDrjRj89TH2sw43nE3+4+W8lSxIuQadEHZyjUk=
	dep	github.com/pkg/errors	v0.9.1	h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=
	dep	github.com/pkoukk/tiktoken-go	v0.1.6	h1:JF0TlJzhTbrI30wCvFuiw6FzP2+/bR+FIxUdgEAcUsw=
	dep	github.com/planetscale/vtprotobuf	v0.6.1-0.20250313105119-ba97887b0a25	h1:S1hI5JiKP7883xBzZAr1ydcxrKNSVNm7+3+JwjxZEsg=
	dep	github.com/plar/go-adaptive-radix-tree	v1.0.5	h1:rHR89qy/6c24TBAHullFMrJsU9hGlKmPibdBGU6/gbM=
	dep	github.com/pmezard/go-difflib	v1.0.1-0.20181226105442-5d4384ee4fb2	h1:Jamvg5psRIccs7FGNTlIRMkT8wgtp5eCXdBlqhYGL6U=
	dep	github.com/prometheus/client_golang	v1.23.2	h1:Je96obch5RDVy3FDMndoUsjAhG5Edi49h0RJWRi/o0o=
	dep	github.com/prometheus/client_model	v0.6.2	h1:oBsgwpGs7iVziMvrGhE53c/GrLUsZdHnqNwqPLxwZyk=
	dep	github.com/prometheus/common	v0.67.5	h1:pIgK94WWlQt1WLwAC5j2ynLaBRDiinoAb86HZHTUGI4=
	dep	github.com/robfig/cron/v3	v3.0.1	h1:WdRxkvbJztn8LMz/QEvLN5sBU+xKpSqwwUO1Pjr4qDs=
	dep	github.com/rogpeppe/go-internal	v1.14.1	h1:UQB4HGPB6osV0SQTLymcB4TgvyWu6ZyliaW0tI/otEQ=
	dep	github.com/rs/xid	v1.6.0	h1:fV591PaemRlL6JfRxGDEPl69wICngIQ3shQtzfy2gxU=
	dep	github.com/samber/lo	v1.38.1	h1:j2XEAqXKb09Am4ebOg31SpvzUTTs6EN3VfgeLUhPdXM=
	dep	github.com/sean-/seed	v0.0.0-20170313163322-e2103e2c3529	h1:nn5Wsu0esKSJiIVhscUtVbo7ada43DJhG55ua/hjS5I=
	dep	github.com/segmentio/asm	v1.1.3	h1:WM03sfUOENvvKexOLp+pCqgb/WDjsi7EK8gIsICtzhc=
	dep	github.com/segmentio/encoding	v0.4.0	h1:MEBYvRqiUB2nfR2criEXWqwdY6HJOUrCn5hboVOVmy8=
	dep	github.com/shirou/gopsutil/v3	v3.23.12	h1:z90NtUkp3bMtmICZKpC4+WaknU1eXtp5vtbQ11DgpE4=
	dep	github.com/shoenig/go-m1cpu	v0.1.6
	=>	github.com/shoenig/go-m1cpu	v0.1.7	h1:C76Yd0ObKR82W4vhfjZiCp0HxcSZ8Nqd84v+HZ0qyI0=

	dep	github.com/spf13/cobra	v1.10.2	h1:DMTTonx5m65Ic0GOoRY2c16WCbHxOOw6xxezuLaBpcU=
	dep	github.com/spf13/pflag	v1.0.10	h1:4EBh2KAYBwaONj6b2Ye1GiHfwjqyROoF4RwYO+vPwFk=
	dep	github.com/spkg/bom	v1.0.0	h1:S939THe0ukL5WcTGiGqkgtaW5JW+O6ITaIlpJXTYY64=
	dep	github.com/stretchr/testify	v1.11.1	h1:7s2iGBzp5EwR7/aIZr8ao5+dra3wiQyKjjFuvgVKu7U=
	dep	github.com/substrait-io/substrait-protobuf/go	v0.85.0	h1:zk6MtNWLtDSl8a7qCZRFH0+EIIXVrrd/hsgYK/SQTgM=
	dep	github.com/tencentyun/cos-go-sdk-v5	v0.7.55	h1:9DfH3umWUd0I2jdqcUxrU1kLfUPOydULNy4T9qN5PF8=
	dep	github.com/tetratelabs/wabin	v0.0.0-20230304001439-f6f874872834	h1:ZF+QBjOI+tILZjBaFj3HgFonKXUcwgJ4djLb6i42S3Q=
	dep	github.com/tetratelabs/wazero	v1.8.1-0.20240916092830-1353ca24fef0	h1:NCRnJ+X6eZt3awiReoHCcDuC6Wf+CgWk6p4IDkIuxTo=
	dep	github.com/tidwall/btree	v1.7.0	h1:L1fkJH/AuEh5zBnnBbmTwQ5Lt+bRJ5A8EWecslvo9iI=
	dep	github.com/tidwall/pretty	v1.2.1	h1:qjsOFOWWQl+N3RsoF5/ssm1pHmJJwhjlSbZ51I6wMl4=
	dep	github.com/tinylib/msgp	v1.6.4	h1:mOwYbyYDLPj35mkA2BjjYejgJk9BuHxDdvRnb6v2ZcQ=
	dep	github.com/tklauser/go-sysconf	v0.3.16	h1:frioLaCQSsF5Cy1jgRBrzr6t502KIIwQ0MArYICU0nA=
	dep	github.com/tmc/langchaingo	v0.1.13	h1:rcpMWBIi2y3B90XxfE4Ao8dhCQPVDMaNPnN5cGB1CaA=
	dep	github.com/twmb/franz-go	v1.21.6	h1:+v0dQJVIIuw9uPmPWmPrkoUHs1pPeV8MSwA4eU/Y2kY=
	dep	github.com/twmb/franz-go/pkg/kadm	v1.18.0	h1:WRf/LZmDdcDXwX7WMbtDU++v+b3NzYh2bCGoPMmzirw=
	dep	github.com/twmb/franz-go/pkg/kmsg	v1.13.1	h1:fG5kItwysTk5UXqVwb64EpQEy3TydF3vYYK21nUQ+bI=
	dep	github.com/uber/h3-go/v4	v4.5.0	h1:7ruJoHCtYOCyihXfQRsPb4o6CfkhCBtVeZFM7+z1kww=
	dep	github.com/unum-cloud/usearch/golang	v0.0.0-20260524141737-9fd6b0115dcd	h1:y37IxTSOZw3uSbMJ1gJse5x9Mcrzto6d6iWkb3NnZxs=
	dep	github.com/valyala/fastrand	v1.1.0	h1:f+5HkLW4rsgzdNoleUOB69hyT9IlD2ZQh9GyDMfb5G8=
	dep	github.com/valyala/histogram	v1.2.0	h1:wyYGAZZt3CpwUiIb9AU/Zbllg1llXyrtApRS815OLoQ=
	dep	github.com/xdg-go/scram	v1.2.0	h1:bYKF2AEwG5rqd1BumT4gAnvwU/M9nBp2pTSxeZw7Wvs=
	dep	github.com/xdg-go/stringprep	v1.0.4	h1:XLI/Ng3O1Atzq0oBs3TWm+5ZVgkq2aqdlvP9JtoZ6c8=
	dep	github.com/xeipuuv/gojsonpointer	v0.0.0-20190905194746-02993c407bfb	h1:zGWFAtiMcyryUHoUjUJX0/lt1H2+i2Ka2n+D3DImSNo=
	dep	github.com/xeipuuv/gojsonreference	v0.0.0-20180127040603-bd5ef7bd5415	h1:EzJWgHovont7NscjpAxXsDA8S8BMYve8Y5+7cuRE7R0=
	dep	github.com/xeipuuv/gojsonschema	v1.2.0	h1:LhYJRs+L4fBtjZUfuSZIKGeVu0QRy8e5Xi7D17UxZ74=
	dep	github.com/yalue/onnxruntime_go	v1.31.0	h1:1ln4YW1SFOFfGJZXe3jNOb2JUSt+l2pEneZfV8HdtFA=
	dep	github.com/yanyiwu/gojieba	v1.4.7	h1:2YkXELcYLTE0SJetq6xv4MjpEikWga6VpFn4jIFFQ/k=
	dep	github.com/youmark/pkcs8	v0.0.0-20240726163527-a2c0da244d78	h1:ilQV1hzziu+LLM3zUTJ0trRztfwgjqKnBWNtSRkbmwM=
	dep	github.com/zeebo/xxh3	v1.1.0	h1:s7DLGDK45Dyfg7++yxI0khrfwq9661w9EN78eP/UZVs=
	dep	go.mongodb.org/mongo-driver/v2	v2.8.0	h1:CxWDGQYY8QQwNjAl/aq2sfWakdnWZynnqJ9F4DhHbP8=
	dep	go.opentelemetry.io/auto/sdk	v1.2.1	h1:jXsnJ4Lmnqd11kwkBV2LgLoFMZKizbCi5fNZ/ipaZ64=
	dep	go.opentelemetry.io/otel	v1.43.0	h1:mYIM03dnh5zfN7HautFE4ieIig9amkNANT+xcVxAj9I=
	dep	go.opentelemetry.io/otel/metric	v1.43.0	h1:d7638QeInOnuwOONPp4JAOGfbCEpYb+K6DVWvdxGzgM=
	dep	go.opentelemetry.io/otel/trace	v1.43.0	h1:BkNrHpup+4k4w+ZZ86CZoHHEkohws8AY+WTX09nk+3A=
	dep	go.opentelemetry.io/proto/otlp	v1.10.0	h1:IQRWgT5srOCYfiWnpqUYz9CVmbO8bFmKcwYxpuCSL2g=
	dep	go.starlark.net	v0.0.0-20250701195324-d457b4515e0e	h1:/WX+ZvcgVJxdIxVR9J3u45ds+Bl4IWPIHRSSICp0t3Q=
	dep	go.uber.org/automaxprocs	v1.5.3	h1:kWazyxZUrS3Gs4qUpbwo5kEIMGe/DAvi5Z4tl2NW4j8=
	dep	go.uber.org/multierr	v1.11.0	h1:blXXJkSxSSfBVBlC76pxqeO+LN3aDfLQo+309xJstO0=
	dep	go.uber.org/ratelimit	v0.2.0	h1:UQE2Bgi7p2B85uP5dC2bbRtig0C+OeNRnNEafLjsLPA=
	dep	go.uber.org/zap	v1.27.1	h1:08RqriUEv8+ArZRYSTXy1LeBScaMpVSTBhCeaZYfMYc=
	dep	go.yaml.in/yaml/v2	v2.4.4	h1:tuyd0P+2Ont/d6e2rl3be67goVK4R6deVxCUX5vyPaQ=
	dep	go.yaml.in/yaml/v3	v3.0.4	h1:tfq32ie2Jv2UxXFdLJdh3jXuOzWiL1fo0bu/FbuKpbc=
	dep	golang.org/x/crypto	v0.51.0	h1:IBPXwPfKxY7cWQZ38ZCIRPI50YLeevDLlLnyC5wRGTI=
	dep	golang.org/x/exp	v0.0.0-20260410095643-746e56fc9e2f	h1:W3F4c+6OLc6H2lb//N1q4WpJkhzJCK5J6kUi1NTVXfM=
	dep	golang.org/x/net	v0.55.0	h1:bcvxaJn3e1U6InsFWt1JUq1aSjnRxLzT2rtD2KfkDF8=
	dep	golang.org/x/sync	v0.22.0	h1:SZjpbeLmrCk4xhRSZFNZW5gFUeCeFgjekvI/+gfScek=
	dep	golang.org/x/sys	v0.47.0	h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs=
	dep	golang.org/x/text	v0.37.0	h1:Cqjiwd9eSg8e0QAkyCaQTNHFIIzWtidPahFWR83rTrc=
	dep	golang.org/x/time	v0.15.0	h1:bbrp8t3bGUeFOx08pvsMYRTCVSMk89u4tKbNOZbp88U=
	dep	gonum.org/v1/gonum	v0.17.0	h1:VbpOemQlsSMrYmn7T2OUvQ4dqxQXU+ouZFQsZOx50z4=
	dep	google.golang.org/genproto/googleapis/rpc	v0.0.0-20260414002931-afd174a4e478	h1:RmoJA1ujG+/lRGNfUnOMfhCy5EipVMyvUE+KNbPbTlw=
	dep	google.golang.org/grpc	v1.82.0	h1:vguDnZUPjE26w09A63VoxZPnvPjB5Riyc0mkXPFmAIU=
	dep	google.golang.org/protobuf	v1.36.11	h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=
	dep	gopkg.in/ini.v1	v1.67.1	h1:tVBILHy0R6e4wkYOn3XmiITt/hEVH4TFMYvAX2Ytz6k=
	dep	gopkg.in/natefinch/lumberjack.v2	v2.2.1	h1:bBRl1b0OH9s/DuPhuXpNl+VtCaJXFZ5/uEFST95x9zc=
	dep	gopkg.in/yaml.v3	v3.0.1	h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=
	dep	vitess.io/vitess	v0.24.0	h1:1beFkQ3hbozQv0pmuicu1LL22szH7yVwZR/fofGiXxk=
	build	-buildmode=exe
	build	-compiler=gc
	build	-ldflags="-extldflags '-L<worktree>/cgo -lmo -L<worktree>/thirdparties/install/lib -Wl,-rpath,@executable_path/lib' -X 'github.com/matrixorigin/matrixone/pkg/version.GoVersion=go version go1.27.1 darwin/arm64' -X 'github.com/matrixorigin/matrixone/pkg/version.BranchName=issue-30107-perf-main' -X 'github.com/matrixorigin/matrixone/pkg/version.CommitID=17a6200f59' -X 'github.com/matrixorigin/matrixone/pkg/version.BuildTime=1792035874' -X 'github.com/matrixorigin/matrixone/pkg/version.Version=issue-30107-perf-main'"
	build	CGO_ENABLED=1
	build	CGO_CFLAGS="-I<worktree>/cgo -I<worktree>/thirdparties/install/include  "
	build	CGO_CPPFLAGS=
	build	CGO_CXXFLAGS=
	build	CGO_LDFLAGS=
	build	GOARCH=arm64
	build	GOEXPERIMENT=simd
	build	GOOS=darwin
	build	GOARM64=v8.0
	build	vcs=git
	build	vcs.revision=17a6200f59e764729e5544908fffd5480ae88279
	build	vcs.time=2026-10-15T01:50:45Z
	build	vcs.modified=true
```

- 对称插桩源指纹：

```json
{
  "before": {
    "pkg/vm/engine/readutil/reader.go": "ba338c488b0ad5e4f8511925dffbacdfd439bece5b5d35168c9273c6b50c342d",
    "pkg/incrservice/allocator.go": "4ccea6a24443aa49108af591a47b19c0b9ff8c4aa2a9ee85636a3e4c817aa305"
  },
  "after": {
    "pkg/vm/engine/readutil/reader.go": "6de423432f81c0a580e8ce506b7833544eb6333153af597f8844d8e0d2de34ee",
    "pkg/incrservice/allocator.go": "89fc97f7c05fdc84f74dc1cc7be824747153ba79a69ebbe9fdb3643d762ff61f",
    "pkg/perfcounter/CLAUDE_fkbench.go": "dc67e97f7573913de7e8ef370746d7c032697944502b0dcb6c55a3dcefa94a0f",
    "pkg/vm/engine/readutil/CLAUDE_fkbench.go": "f8c4718329319d7f8bf671d87647b44b4f9a60c98d1ec083c488026ccc40ff7f"
  }
}
```

### head

- 源 revision：`b812d7b6b3ed6b463065789903c7034b7b8f28ef`；二进制 SHA-256：`56aa9bd49d0391585da67bfc6283f0914efbcf621c08c00795bca8b525802929`。
- 原始 JSONL SHA-256：`37a6c0748953e588a7e86cd8703ea2c05e0f33618a9a58df344df4b5de8f5131`；成功 samples=270；数据库清理已确认。
- 平台：`macOS-26.6.2-arm64-arm-64bit`；GOMAXPROCS=2，GOGC=50；启动负载：`11:53  up 22 days, 21:44, 24 users, load averages: 12.39 19.40 29.92`。
- 工具链/编译：

```text
<worktree>/CLAUDE_mo_fkprofile: go1.27.1-X:simd
	path	github.com/matrixorigin/matrixone/cmd/mo-service
	mod	github.com/matrixorigin/matrixone	v0.7.1-0.20261015031633-b812d7b6b3ed+dirty
	dep	filippo.io/edwards25519	v1.1.1	h1:YpjwWWlNmGIDyXOn8zLzqiD+9TyIlPhGFG96P39uBpw=
	dep	github.com/BurntSushi/toml	v1.2.1	h1:9F2/+DoOYIOksmaJFPw1tGFy1eDnIJXg+UHjuD8lTak=
	dep	github.com/DataDog/zstd	v1.5.0	h1:+K/VEwIAaPcHiMtQvpLD4lqW7f0Gk3xdYZmI1hD+CXo=
	dep	github.com/FastFilter/xorfilter	v0.5.1	h1:UjtPttI1SnKWUmeQKu8Y+4ZvMJv7KP6/NZTsGBPQY08=
	dep	github.com/KimMachineGun/automemlimit	v0.6.0	h1:p/BXkH+K40Hax+PuWWPQ478hPjsp9h1CPDhLlA3Z37E=
	dep	github.com/Masterminds/semver/v3	v3.2.1	h1:RN9w6+7QoMeJVGyfmbcgs28Br8cvmnucEXnY0rYXWg0=
	dep	github.com/RoaringBitmap/roaring/v2	v2.16.0	h1:Kys1UNf49d5W8Tq3bpuAhIr/Z8/yPB+59CO8A6c/BbE=
	dep	github.com/VictoriaMetrics/metrics	v1.18.1	h1:OZ0+kTTto8oPfHnVAnTOoyl0XlRhRkoQrD2n2cOuRw0=
	dep	github.com/alibabacloud-go/debug	v1.0.1	h1:MsW9SmUtbb1Fnt3ieC6NNZi6aEwrXfDksD4QA6GSbPg=
	dep	github.com/alibabacloud-go/tea	v1.2.2	h1:aTsR6Rl3ANWPfqeQugPglfurloyBJY85eFy7Gc1+8oU=
	dep	github.com/aliyun/alibaba-cloud-sdk-go	v1.63.34	h1:eZM2MHY/p4TFO1pGf9O5HiuYE59hwrrkf3HvCtkL5Ok=
	dep	github.com/aliyun/aliyun-oss-go-sdk	v3.0.2+incompatible	h1:8psS8a+wKfiLt1iVDX79F7Y6wUM49Lcha2FMXt4UM8g=
	dep	github.com/aliyun/credentials-go	v1.3.10	h1:45Xxrae/evfzQL9V10zL3xX31eqgLWEaIdCoPipOEQA=
	dep	github.com/andres-erbsen/clock	v0.0.0-20160526145045-9e14626cd129	h1:MzBOUgng9orim59UnfUTLRjMpd09C5uEVQ6RPGeCaVI=
	dep	github.com/andybalholm/brotli	v1.2.2	h1:HzTuoo2ErYQqf5qvcJInB8uvqSVxRttzkFexPWtnceM=
	dep	github.com/apache/arrow-go/v18	v18.7.0	h1:Vw/i+cJyebUofT7JlqFpe65LrmwxULn166jjwStM4HY=
	dep	github.com/armon/go-metrics	v0.4.1	h1:hR91U9KYmb6bLBYLQjyM+3j+rcd/UhE+G78SFnF8gJA=
	dep	github.com/aws/aws-sdk-go	v1.55.5	h1:KKUZBfBoyqy5d3swXyiC7Q76ic40rYcbqH7qjh59kzU=
	dep	github.com/aws/aws-sdk-go-v2	v1.41.5	h1:dj5kopbwUsVUVFgO4Fi5BIT3t4WyqIDjGKCangnV/yY=
	dep	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream	v1.7.8	h1:eBMB84YGghSocM7PsjmmPffTa+1FBUeNvGvFou6V/4o=
	dep	github.com/aws/aws-sdk-go-v2/config	v1.32.14	h1:opVIRo/ZbbI8OIqSOKmpFaY7IwfFUOCCXBsUpJOwDdI=
	dep	github.com/aws/aws-sdk-go-v2/credentials	v1.19.14	h1:n+UcGWAIZHkXzYt87uMFBv/l8THYELoX6gVcUvgl6fI=
	dep	github.com/aws/aws-sdk-go-v2/feature/ec2/imds	v1.18.21	h1:NUS3K4BTDArQqNu2ih7yeDLaS3bmHD0YndtA6UP884g=
	dep	github.com/aws/aws-sdk-go-v2/internal/configsources	v1.4.21	h1:Rgg6wvjjtX8bNHcvi9OnXWwcE0a2vGpbwmtICOsvcf4=
	dep	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2	v2.7.21	h1:PEgGVtPoB6NTpPrBgqSE5hE/o47Ij9qk/SEZFbUOe9A=
	dep	github.com/aws/aws-sdk-go-v2/internal/ini	v1.8.6	h1:qYQ4pzQ2Oz6WpQ8T3HvGHnZydA72MnLuFK9tJwmrbHw=
	dep	github.com/aws/aws-sdk-go-v2/internal/v4a	v1.4.22	h1:rWyie/PxDRIdhNf4DzRk0lvjVOqFJuNnO8WwaIRVxzQ=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding	v1.13.7	h1:5EniKhLZe4xzL7a+fU3C2tfUN4nWIqlLesfrjkuPFTY=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/checksum	v1.9.13	h1:JRaIgADQS/U6uXDqlPiefP32yXTda7Kqfx+LgspooZM=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url	v1.13.21	h1:c31//R3xgIJMSC8S6hEVq+38DcvUlgFY0FM6mSI5oto=
	dep	github.com/aws/aws-sdk-go-v2/service/internal/s3shared	v1.19.21	h1:ZlvrNcHSFFWURB8avufQq9gFsheUgjVD9536obIknfM=
	dep	github.com/aws/aws-sdk-go-v2/service/s3	v1.99.0	h1:hlSuz394kV0vhv9drL5lhuEFbEOEP1VyQpy15qWh1Pk=
	dep	github.com/aws/aws-sdk-go-v2/service/signin	v1.0.9	h1:QKZH0S178gCmFEgst8hN0mCX1KxLgHBKKY/CLqwP8lg=
	dep	github.com/aws/aws-sdk-go-v2/service/sso	v1.30.15	h1:lFd1+ZSEYJZYvv9d6kXzhkZu07si3f+GQ1AaYwa2LUM=
	dep	github.com/aws/aws-sdk-go-v2/service/ssooidc	v1.35.19	h1:dzztQ1YmfPrxdrOiuZRMF6fuOwWlWpD2StNLTceKpys=
	dep	github.com/aws/aws-sdk-go-v2/service/sts	v1.41.10	h1:p8ogvvLugcR/zLBXTXrTkj0RYBUdErbMnAFFp12Lm/U=
	dep	github.com/aws/smithy-go	v1.24.3	h1:XgOAaUgx+HhVBoP4v8n6HCQoTRDhoMghKqw4LNHsDNg=
	dep	github.com/axiomhq/hyperloglog	v0.2.6	h1:sRhvvF3RIXWQgAXaTphLp4yJiX4S0IN3MWTaAgZoRJw=
	dep	github.com/beorn7/perks	v1.0.1	h1:VlbKKnNfV8bJzeqoa4cOKqO6bYr3WgKZxO8Z16+hsOM=
	dep	github.com/bits-and-blooms/bitset	v1.24.2	h1:M7/NzVbsytmtfHbumG+K2bremQPMJuqv1JD3vOaFxp0=
	dep	github.com/blevesearch/mmap-go	v1.2.0	h1:l33nNKPFcBjJUMwem6sAYJPUzhUCABoK9FxZDGiFNBI=
	dep	github.com/blevesearch/vellum	v1.2.0	h1:xkDiOEsHc2t3Cp0NsNZZ36pvc130sCzcGKOPMzXe+e0=
	dep	github.com/buger/jsonparser	v1.1.2	h1:frqHqw7otoVbk5M8LlE/L7HTnIq2v9RX6EJ48i9AxJk=
	dep	github.com/bytedance/gopkg	v0.1.3	h1:TPBSwH8RsouGCBcMBktLt1AymVo2TVsBVCY4b6TnZ/M=
	dep	github.com/bytedance/sonic	v1.15.2	h1:90H+rcF/FwLXwfB1cudOLq/je83n683Utf4Cbp0xHCo=
	dep	github.com/cespare/xxhash/v2	v2.3.0	h1:UL815xU9SqsFlibzuggzjXhog7bL6oX9BbNZnL2UFvs=
	dep	github.com/clbanning/mxj	v1.8.4	h1:HuhwZtbyvyOw+3Z1AowPkU87JkJUSv751ELWaiTpj8I=
	dep	github.com/clipperhouse/uax29/v2	v2.7.0	h1:+gs4oBZ2gPfVrKPthwbMzWZDaAFPGYK72F0NJv2v7Vk=
	dep	github.com/cockroachdb/errors	v1.9.1	h1:yFVvsI0VxmRShfawbt/laCIDy/mtTqqnvoNgiy5bEV8=
	dep	github.com/cockroachdb/logtags	v0.0.0-20230118201751-21c54148d20b	h1:r6VH0faHjZeQy818SGhaone5OnYfxFR/+AzdY3sf5aE=
	dep	github.com/cockroachdb/pebble	v0.0.0-20220407171941-2120d145e292	h1:7fLxcRpQTdi1iic0cIUpK+hKKLM5XdPhEbWRRrxfvLU=
	dep	github.com/cockroachdb/redact	v1.1.3	h1:AKZds10rFSIj7qADf0g46UixK8NNLwWTNdCIGS5wfSQ=
	dep	github.com/colinmarc/hdfs/v2	v2.4.0	h1:v6R8oBx/Wu9fHpdPoJJjpGSUxo8NhHIwrwsfhFvU9W0=
	dep	github.com/cpegeric/pdftotext-go	v0.0.0-20241112123704-49cb86a3790e	h1:tQSCiEjYPRU+AuuVR+zd+xYVOsEqX1clPhmIAM6FCHU=
	dep	github.com/davecgh/go-spew	v1.1.2-0.20180830191138-d8f796af33cc	h1:U9qPSI2PIWSS1VwoXQT9A3Wy9MM3WgvqSxFWenqJduM=
	dep	github.com/detailyang/go-fallocate	v0.0.0-20180908115635-432fa640bd2e	h1:lj77EKYUpYXTd8CD/+QMIf8b6OIOTsfEBSXiAzuEHTU=
	dep	github.com/dgryski/go-metro	v0.0.0-20250106013310-edb8663e5e33	h1:ucRHb6/lvW/+mTEIGbvhcYU3S8+uSNkuMjx/qZFfhtM=
	dep	github.com/dlclark/regexp2	v1.10.0
	=>	github.com/XuPeng-SH/regexp2	v1.10.1-0.20261004024751-718f43e031fe	h1:7TlZYr/D2KQlZ2lwGehHN7Ix3ICj43OXaZx2UT4DOIo=

	dep	github.com/docker/go-units	v0.5.0	h1:69rxXcBk27SvSaaxTtLh/8llcHD8vYHT7WSdRZ/jvr4=
	dep	github.com/dolthub/maphash	v0.1.0	h1:bsQ7JsF4FkkWyrP3oCnFJgrCUAFbFf3kOl4L/QxPDyQ=
	dep	github.com/dslipak/pdf	v0.0.2	h1:djAvcM5neg9Ush+zR6QXB+VMJzR6TdnX766HPIg1JmI=
	dep	github.com/dustin/go-humanize	v1.0.1	h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY=
	dep	github.com/dylibso/observe-sdk/go	v0.0.0-20240819160327-2d926c5d788a	h1:UwSIFv5g5lIvbGgtf3tVwC7Ky9rmMFBp0RMs+6f6YqE=
	dep	github.com/elastic/elastic-transport-go/v8	v8.6.0	h1:Y2S/FBjx1LlCv5m6pWAF2kDJAHoSjSRSJCApolgfthA=
	dep	github.com/elastic/go-elasticsearch/v8	v8.15.0	h1:IZyJhe7t7WI3NEFdcHnf6IJXqpRf+8S8QWLtZYYyBYk=
	dep	github.com/elastic/gosigar	v0.14.2
	=>	github.com/matrixorigin/gosigar	v0.14.3-0.20241204071856-40aab500bfac	h1:KRPOwOcSZRfWT7w8mr7BmFLoFB75ljgqjkg47xkN/Pc=

	dep	github.com/extism/go-sdk	v1.6.0	h1:crFRMhjcPAn6R9M4eIvkjHQs7CLBs3yzPqwnj+uwzdg=
	dep	github.com/fagongzi/goetty/v2	v2.0.3-0.20230628075727-26c9a2fd5fb8
	=>	github.com/matrixorigin/goetty/v2	v2.0.0-20260811155919-8edc32ac370a	h1:D8kXjySDJHA8nyZzzi/4cx3KB6qrSPQeugUCLkgoEho=

	dep	github.com/fagongzi/util	v0.0.0-20210923134909-bccc37b5040d	h1:1pILVCatHj3eVo9i52dZyY4BwjTmSIeN+/hoJh8rD0Y=
	dep	github.com/felixge/fgprof	v0.9.6-0.20240831122612-49987e680f04	h1:6exK5FZYMJwsXo4xGqax5rdCHCYeQz+zgTO+Sk+tNHE=
	dep	github.com/getsentry/sentry-go	v0.12.0	h1:era7g0re5iY13bHSdN/xMkyV+5zZppjRVQhZrXCaEIk=
	dep	github.com/go-ini/ini	v1.67.0	h1:z6ZrTEZqSWOTyH2FlglNbNgARyHG8oLW9gMELqKr06A=
	dep	github.com/go-logr/logr	v1.4.3	h1:CjnDlHq8ikf6E492q6eKboGOC0T8CDaOvkHCIg8idEI=
	dep	github.com/go-logr/stdr	v1.2.2	h1:hSWxHoqTgW2S2qGc0LTAI563KZ5YKYRhT3MFKZMbjag=
	dep	github.com/go-sql-driver/mysql	v1.9.3	h1:U/N249h2WzJ3Ukj8SowVFjdtZKfu9vlLZxjPXV1aweo=
	dep	github.com/go-viper/mapstructure/v2	v2.5.0	h1:vM5IJoUAy3d7zRSVtIwQgBj7BiWtMPfmPEgAXnvj1Ro=
	dep	github.com/gobwas/glob	v0.2.3	h1:A4xDbljILXROh+kObIiy5kIaPYD8e96x1tgBhUI5J+Y=
	dep	github.com/goccy/go-json	v0.10.6	h1:p8HrPJzOakx/mn/bQtjgNjdTcN+/S6FcG2CTtQOrHVU=
	dep	github.com/gogo/protobuf	v1.3.2	h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=
	dep	github.com/golang-collections/collections	v0.0.0-20130729185459-604e922904d3	h1:zN2lZNZRflqFyxVaTIU61KNKQ9C0055u9CAfpmqUvo4=
	dep	github.com/golang/geo	v0.0.0-20260526120156-e21b7802b0ee	h1:L0DB+dB7f2E+BvC518GbeU+WgELvWXTrbFiitwnFtuc=
	dep	github.com/golang/glog	v1.2.5	h1:DrW6hGnjIhtvhOIiAKT6Psh/Kd/ldepEa81DKeiRJ5I=
	dep	github.com/golang/snappy	v1.0.0	h1:Oy607GVXHs7RtbggtPBnr2RmDArIsAefDwvrdWvRhGs=
	dep	github.com/google/btree	v1.1.3	h1:CVpQJjYgC4VbzxeGVHfvZrv1ctoYCAI8vbl07Fcxlyg=
	dep	github.com/google/flatbuffers	v25.12.19+incompatible	h1:haMV2JRRJCe1998HeW/p0X9UaMTK6SDo0ffLn2+DbLs=
	dep	github.com/google/go-querystring	v1.1.0	h1:AnCroh3fv4ZBgVIf1Iwtovgjaw/GiKJo8M8yD/fhyJ8=
	dep	github.com/google/pprof	v0.0.0-20260302011040-a15ffb7f9dcc	h1:VBbFa1lDYWEeV5FZKUiYKYT0VxCp9twUmmaq9eb8sXw=
	dep	github.com/google/shlex	v0.0.0-20191202100458-e7afc7fbc510	h1:El6M4kTTCOh6aBiKaUGG7oYTSPP8MxqL4YI3kZKwcP4=
	dep	github.com/google/uuid	v1.6.0	h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=
	dep	github.com/hamba/avro/v2	v2.31.0	h1:wv3nmua7lCEIwWsb6vqsTS3pXktTxcKg5eoyNu0VhrU=
	dep	github.com/hashicorp/errwrap	v1.1.0	h1:OxrOeh75EUXMY8TBjag2fzXGZ40LB6IKw45YeGUDY2I=
	dep	github.com/hashicorp/go-immutable-radix	v1.3.1	h1:DKHmCUm2hRBK510BaiZlwvpD40f8bJFeZnpfm2KLowc=
	dep	github.com/hashicorp/go-msgpack	v0.5.3	h1:zKjpN5BK/P5lMYrLmBHdBULWbJ0XpYR+7NGzqkZzoD4=
	dep	github.com/hashicorp/go-multierror	v1.1.1	h1:H5DkEtf6CXdFp0N0Em5UCwQpXMWke8IA0+lD48awMYo=
	dep	github.com/hashicorp/go-sockaddr	v1.0.0	h1:GeH6tui99pF4NJgfnhp+L6+FfobzVW3Ah46sLo0ICXs=
	dep	github.com/hashicorp/go-uuid	v1.0.3	h1:2gKiV6YVmrJ1i2CKKa9obLvRieoRGviZFL26PcT/Co8=
	dep	github.com/hashicorp/golang-lru	v1.0.2	h1:dV3g9Z/unq5DpblPpw+Oqcv4dU/1omnb4Ok8iPY6p1c=
	dep	github.com/hashicorp/memberlist	v0.3.1
	=>	github.com/matrixorigin/memberlist	v0.5.1-0.20230322082342-95015c95ee76	h1:MpmqMPooJ0Ea7W4ldIGbQV4D3z+sEiCu6C6aTibiwiQ=

	dep	github.com/hayageek/threadsafe	v1.0.1	h1:QTMJrninAaGQE7+CYdJWPzTlnhcBPwhxD5GDdvIf5oU=
	dep	github.com/ianlancetaylor/demangle	v0.0.0-20250417193237-f615e6bd150b	h1:ogbOPx86mIhFy764gGkqnkFC8m5PJA7sPzlk9ppLVQA=
	dep	github.com/itchyny/gojq	v0.12.16	h1:yLfgLxhIr/6sJNVmYfQjTIv0jGctu6/DgDoivmxTr7g=
	dep	github.com/itchyny/timefmt-go	v0.1.6	h1:ia3s54iciXDdzWzwaVKXZPbiXzxxnv1SPGFfM/myJ5Q=
	dep	github.com/jackc/pgpassfile	v1.0.0	h1:/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=
	dep	github.com/jackc/pgservicefile	v0.0.0-20221227161230-091c0ba34f0a	h1:bbPeKD0xmW/Y25WS6cokEszi5g+S0QxI/d45PkRi7Nk=
	dep	github.com/jackc/pgx/v5	v5.5.5	h1:amBjrZVmksIdNjxGW/IiIMzxMKZFelXbUoPNb+8sjQw=
	dep	github.com/jackc/puddle/v2	v2.2.1	h1:RhxXJtFG022u4ibrCSMSiu5aOq1i77R3OHKNJj77OAk=
	dep	github.com/jcmturner/aescts/v2	v2.0.0	h1:9YKLH6ey7H4eDBXW8khjYslgyqG2xZikXP0EQFKrle8=
	dep	github.com/jcmturner/dnsutils/v2	v2.0.0	h1:lltnkeZGL0wILNvrNiVCR6Ro5PGU/SeBvVO/8c/iPbo=
	dep	github.com/jcmturner/gofork	v1.7.6	h1:QH0l3hzAU1tfT3rZCnW5zXl+orbkNMMRGJfdJjHVETg=
	dep	github.com/jcmturner/goidentity/v6	v6.0.1	h1:VKnZd2oEIMorCTsFBnJWbExfNN7yZr3EhJAxwOkZg6o=
	dep	github.com/jcmturner/gokrb5/v8	v8.4.4	h1:x1Sv4HaTpepFkXbt2IkL29DXRf8sOfZXo8eRKh687T8=
	dep	github.com/jcmturner/rpc/v2	v2.0.3	h1:7FXXj8Ti1IaVFpSAziCZWNzbNuZmnvw/i6CqLNdWfZY=
	dep	github.com/jmespath/go-jmespath	v0.4.0	h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=
	dep	github.com/jonboulle/clockwork	v0.4.0	h1:p4Cf1aMWXnXAUh8lVfewRBx1zaTSYKrKMF2g3ST4RZ4=
	dep	github.com/json-iterator/go	v1.1.12	h1:PV8peI4a0ysnczrg+LtxykD8LfKY9ML6u2jnxaEnrnM=
	dep	github.com/kamstrup/intmap	v0.5.2	h1:qnwBm1mh4XAnW9W9Ue9tZtTff8pS6+s6iKF6JRIV2Dk=
	dep	github.com/klauspost/compress	v1.19.0	h1:sXLILfc9jV2QYWkzFOPWStmcUVH2RHEB1JCdY2oVvCQ=
	dep	github.com/klauspost/cpuid/v2	v2.4.0	h1:S6Hrbc7+ywsr0r+RLapfGBHfyefhCTwEh3A0tV913Dw=
	dep	github.com/klauspost/crc32	v1.3.0	h1:sSmTt3gUt81RP655XGZPElI0PelVTZ6YwCRnPSupoFM=
	dep	github.com/kr/pretty	v0.3.1	h1:flRD4NNwYAUpkphVc1HcthR4KEIFJ65n8Mw5qdRn3LE=
	dep	github.com/kr/text	v0.2.0	h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=
	dep	github.com/lmittmann/tint	v1.1.3	h1:Hv4EaHWXQr+GTFnOU4VKf8UvAtZgn0VuKT+G0wFlO3I=
	dep	github.com/lni/dragonboat/v4	v4.0.0-20220815145555-6f622e8bcbef
	=>	github.com/matrixorigin/dragonboat/v4	v4.0.0-20261006150417-abc3d2fce5a6	h1:AQWG/u9fUQvn7WTGRvjV9CkHyNI8mCxpr8qZf3vRAmg=

	dep	github.com/lni/goutils	v1.3.1-0.20220604063047-388d67b4dbc4
	=>	github.com/matrixorigin/goutils	v1.3.1-0.20220604063047-388d67b4dbc4	h1:+SmZP2bG+YcO/ntzjCleCu6hFTJiue7Oj2tftpJKTlU=

	dep	github.com/lni/vfs	v0.2.1-0.20220616104132-8852fd867376
	=>	github.com/matrixorigin/vfs	v0.2.1-0.20220616104132-8852fd867376	h1:1XpEh8nPx1yFTeTFMj/aMipEiilDhQToQS8gjEU2HxM=

	dep	github.com/matrixorigin/mysql	v1.8.2-0.20241106110439-6ac9ee94770d	h1:27vD3JGbrFmaQtDYQT/W1jFFr0xvipdwH5R4bZPGQdE=
	dep	github.com/matrixorigin/simdcsv	v0.0.0-20230210060146-09b8e45209dd	h1:DvqhuH3kOpsE6vXZA5WEaRNAUUUcf44S1p5VInbjdfU=
	dep	github.com/mattn/go-isatty	v0.0.21	h1:xYae+lCNBP7QuW4PUnNG61ffM4hVIfm+zUzDuSzYLGs=
	dep	github.com/miekg/dns	v1.1.53	h1:ZBkuHr5dxHtB1caEOlZTLPo7D3L3TWckgUUs/RHfDxw=
	dep	github.com/minio/crc64nvme	v1.1.1	h1:8dwx/Pz49suywbO+auHCBpCtlW1OfpcLN7wYgVR6wAI=
	dep	github.com/minio/md5-simd	v1.1.2	h1:Gdi1DZK69+ZVMoNHRXJyNcxrMA4dSxoYHZSQbirFg34=
	dep	github.com/minio/minio-go/v7	v7.0.99	h1:2vH/byrwUkIpFQFOilvTfaUpvAX3fEFhEzO+DR3DlCE=
	dep	github.com/mitchellh/mapstructure	v1.5.0	h1:jeMsZIYE/09sWLaz43PL7Gy6RuMjD2eJVyuac5Z2hdY=
	dep	github.com/modern-go/concurrent	v0.0.0-20180306012644-bacd9c7ef1dd	h1:TRLaZ9cD/w8PVh93nsPXa1VrQ6jlwL5oN8l14QlcNfg=
	dep	github.com/modern-go/reflect2	v1.0.3-0.20250322232337-35a7c28c31ee	h1:W5t00kpgFdJifH4BDsTlE89Zl93FEloxaWZfGcifgq8=
	dep	github.com/mohae/deepcopy	v0.0.0-20170929034955-c48cc78d4826	h1:RWengNIwukTxcDr9M+97sNutRR1RKhG96O6jWumTTnw=
	dep	github.com/mozillazg/go-httpheader	v0.2.1	h1:geV7TrjbL8KXSyvghnFm+NyTux/hxwueTSrwhe88TQQ=
	dep	github.com/mschoch/smat	v0.2.0	h1:8imxQsjDm8yFEAVBe7azKmKSgzSkZXDuKkSq9374khM=
	dep	github.com/munnerz/goautoneg	v0.0.0-20191010083416-a7dc8b61c822	h1:C3w9PqII01/Oq1c1nUAm88MOHcQC9l5mIlSMApZMrHA=
	dep	github.com/ncruces/go-dns	v1.2.5	h1:d2zZVWUassOyUOc8okB70hXBZrupqkuls7C7ZXMaKMM=
	dep	github.com/openacid/slimarray	v0.1.3	h1:+/+G8k+Nz4p8QUj4J2kd7IzFC5DiJzk5H2QPp/BpHHk=
	dep	github.com/opentracing/opentracing-go	v1.2.1-0.20220228012449-10b1cf09e00b	h1:FfH+VrHHk6Lxt9HdVS0PXzSXFyS2NbZKXv33FYPol0A=
	dep	github.com/panjf2000/ants/v2	v2.12.0	h1:u9JhESo83i/GkZnhfTNuFMMWcNt7mnV1bGJ6FT4wXH8=
	dep	github.com/parquet-go/parquet-go	v0.25.1	h1:l7jJwNM0xrk0cnIIptWMtnSnuxRkwq53S+Po3KG8Xgo=
	dep	github.com/pbnjay/memory	v0.0.0-20210728143218-7b4eea64cf58	h1:onHthvaw9LFnH4t2DcNVpwGmV9E1BkGknEliJkfwQj0=
	dep	github.com/petermattis/goid	v0.0.0-20260330135022-df67b199bc81	h1:WDsQxOJDy0N1VRAjXLpi8sCEZRSGarLWQevDxpTBRrM=
	dep	github.com/philhofer/fwd	v1.2.0	h1:e6DnBTl7vGY+Gz322/ASL4Gyp1FspeMvx1RNDoToZuM=
	dep	github.com/pierrec/lz4/v4	v4.1.27	h1:+PhzhWDrjRj89TH2sw43nE3+4+W8lSxIuQadEHZyjUk=
	dep	github.com/pkg/errors	v0.9.1	h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=
	dep	github.com/pkoukk/tiktoken-go	v0.1.6	h1:JF0TlJzhTbrI30wCvFuiw6FzP2+/bR+FIxUdgEAcUsw=
	dep	github.com/planetscale/vtprotobuf	v0.6.1-0.20250313105119-ba97887b0a25	h1:S1hI5JiKP7883xBzZAr1ydcxrKNSVNm7+3+JwjxZEsg=
	dep	github.com/plar/go-adaptive-radix-tree	v1.0.5	h1:rHR89qy/6c24TBAHullFMrJsU9hGlKmPibdBGU6/gbM=
	dep	github.com/pmezard/go-difflib	v1.0.1-0.20181226105442-5d4384ee4fb2	h1:Jamvg5psRIccs7FGNTlIRMkT8wgtp5eCXdBlqhYGL6U=
	dep	github.com/prometheus/client_golang	v1.23.2	h1:Je96obch5RDVy3FDMndoUsjAhG5Edi49h0RJWRi/o0o=
	dep	github.com/prometheus/client_model	v0.6.2	h1:oBsgwpGs7iVziMvrGhE53c/GrLUsZdHnqNwqPLxwZyk=
	dep	github.com/prometheus/common	v0.67.5	h1:pIgK94WWlQt1WLwAC5j2ynLaBRDiinoAb86HZHTUGI4=
	dep	github.com/robfig/cron/v3	v3.0.1	h1:WdRxkvbJztn8LMz/QEvLN5sBU+xKpSqwwUO1Pjr4qDs=
	dep	github.com/rogpeppe/go-internal	v1.14.1	h1:UQB4HGPB6osV0SQTLymcB4TgvyWu6ZyliaW0tI/otEQ=
	dep	github.com/rs/xid	v1.6.0	h1:fV591PaemRlL6JfRxGDEPl69wICngIQ3shQtzfy2gxU=
	dep	github.com/samber/lo	v1.38.1	h1:j2XEAqXKb09Am4ebOg31SpvzUTTs6EN3VfgeLUhPdXM=
	dep	github.com/sean-/seed	v0.0.0-20170313163322-e2103e2c3529	h1:nn5Wsu0esKSJiIVhscUtVbo7ada43DJhG55ua/hjS5I=
	dep	github.com/segmentio/asm	v1.1.3	h1:WM03sfUOENvvKexOLp+pCqgb/WDjsi7EK8gIsICtzhc=
	dep	github.com/segmentio/encoding	v0.4.0	h1:MEBYvRqiUB2nfR2criEXWqwdY6HJOUrCn5hboVOVmy8=
	dep	github.com/shirou/gopsutil/v3	v3.23.12	h1:z90NtUkp3bMtmICZKpC4+WaknU1eXtp5vtbQ11DgpE4=
	dep	github.com/shoenig/go-m1cpu	v0.1.6
	=>	github.com/shoenig/go-m1cpu	v0.1.7	h1:C76Yd0ObKR82W4vhfjZiCp0HxcSZ8Nqd84v+HZ0qyI0=

	dep	github.com/spf13/cobra	v1.10.2	h1:DMTTonx5m65Ic0GOoRY2c16WCbHxOOw6xxezuLaBpcU=
	dep	github.com/spf13/pflag	v1.0.10	h1:4EBh2KAYBwaONj6b2Ye1GiHfwjqyROoF4RwYO+vPwFk=
	dep	github.com/spkg/bom	v1.0.0	h1:S939THe0ukL5WcTGiGqkgtaW5JW+O6ITaIlpJXTYY64=
	dep	github.com/stretchr/testify	v1.11.1	h1:7s2iGBzp5EwR7/aIZr8ao5+dra3wiQyKjjFuvgVKu7U=
	dep	github.com/substrait-io/substrait-protobuf/go	v0.85.0	h1:zk6MtNWLtDSl8a7qCZRFH0+EIIXVrrd/hsgYK/SQTgM=
	dep	github.com/tencentyun/cos-go-sdk-v5	v0.7.55	h1:9DfH3umWUd0I2jdqcUxrU1kLfUPOydULNy4T9qN5PF8=
	dep	github.com/tetratelabs/wabin	v0.0.0-20230304001439-f6f874872834	h1:ZF+QBjOI+tILZjBaFj3HgFonKXUcwgJ4djLb6i42S3Q=
	dep	github.com/tetratelabs/wazero	v1.8.1-0.20240916092830-1353ca24fef0	h1:NCRnJ+X6eZt3awiReoHCcDuC6Wf+CgWk6p4IDkIuxTo=
	dep	github.com/tidwall/btree	v1.7.0	h1:L1fkJH/AuEh5zBnnBbmTwQ5Lt+bRJ5A8EWecslvo9iI=
	dep	github.com/tidwall/pretty	v1.2.1	h1:qjsOFOWWQl+N3RsoF5/ssm1pHmJJwhjlSbZ51I6wMl4=
	dep	github.com/tinylib/msgp	v1.6.4	h1:mOwYbyYDLPj35mkA2BjjYejgJk9BuHxDdvRnb6v2ZcQ=
	dep	github.com/tklauser/go-sysconf	v0.3.16	h1:frioLaCQSsF5Cy1jgRBrzr6t502KIIwQ0MArYICU0nA=
	dep	github.com/tmc/langchaingo	v0.1.13	h1:rcpMWBIi2y3B90XxfE4Ao8dhCQPVDMaNPnN5cGB1CaA=
	dep	github.com/twmb/franz-go	v1.21.6	h1:+v0dQJVIIuw9uPmPWmPrkoUHs1pPeV8MSwA4eU/Y2kY=
	dep	github.com/twmb/franz-go/pkg/kadm	v1.18.0	h1:WRf/LZmDdcDXwX7WMbtDU++v+b3NzYh2bCGoPMmzirw=
	dep	github.com/twmb/franz-go/pkg/kmsg	v1.13.1	h1:fG5kItwysTk5UXqVwb64EpQEy3TydF3vYYK21nUQ+bI=
	dep	github.com/uber/h3-go/v4	v4.5.0	h1:7ruJoHCtYOCyihXfQRsPb4o6CfkhCBtVeZFM7+z1kww=
	dep	github.com/unum-cloud/usearch/golang	v0.0.0-20260524141737-9fd6b0115dcd	h1:y37IxTSOZw3uSbMJ1gJse5x9Mcrzto6d6iWkb3NnZxs=
	dep	github.com/valyala/fastrand	v1.1.0	h1:f+5HkLW4rsgzdNoleUOB69hyT9IlD2ZQh9GyDMfb5G8=
	dep	github.com/valyala/histogram	v1.2.0	h1:wyYGAZZt3CpwUiIb9AU/Zbllg1llXyrtApRS815OLoQ=
	dep	github.com/xdg-go/scram	v1.2.0	h1:bYKF2AEwG5rqd1BumT4gAnvwU/M9nBp2pTSxeZw7Wvs=
	dep	github.com/xdg-go/stringprep	v1.0.4	h1:XLI/Ng3O1Atzq0oBs3TWm+5ZVgkq2aqdlvP9JtoZ6c8=
	dep	github.com/xeipuuv/gojsonpointer	v0.0.0-20190905194746-02993c407bfb	h1:zGWFAtiMcyryUHoUjUJX0/lt1H2+i2Ka2n+D3DImSNo=
	dep	github.com/xeipuuv/gojsonreference	v0.0.0-20180127040603-bd5ef7bd5415	h1:EzJWgHovont7NscjpAxXsDA8S8BMYve8Y5+7cuRE7R0=
	dep	github.com/xeipuuv/gojsonschema	v1.2.0	h1:LhYJRs+L4fBtjZUfuSZIKGeVu0QRy8e5Xi7D17UxZ74=
	dep	github.com/yalue/onnxruntime_go	v1.31.0	h1:1ln4YW1SFOFfGJZXe3jNOb2JUSt+l2pEneZfV8HdtFA=
	dep	github.com/yanyiwu/gojieba	v1.4.7	h1:2YkXELcYLTE0SJetq6xv4MjpEikWga6VpFn4jIFFQ/k=
	dep	github.com/youmark/pkcs8	v0.0.0-20240726163527-a2c0da244d78	h1:ilQV1hzziu+LLM3zUTJ0trRztfwgjqKnBWNtSRkbmwM=
	dep	github.com/zeebo/xxh3	v1.1.0	h1:s7DLGDK45Dyfg7++yxI0khrfwq9661w9EN78eP/UZVs=
	dep	go.mongodb.org/mongo-driver/v2	v2.8.0	h1:CxWDGQYY8QQwNjAl/aq2sfWakdnWZynnqJ9F4DhHbP8=
	dep	go.opentelemetry.io/auto/sdk	v1.2.1	h1:jXsnJ4Lmnqd11kwkBV2LgLoFMZKizbCi5fNZ/ipaZ64=
	dep	go.opentelemetry.io/otel	v1.43.0	h1:mYIM03dnh5zfN7HautFE4ieIig9amkNANT+xcVxAj9I=
	dep	go.opentelemetry.io/otel/metric	v1.43.0	h1:d7638QeInOnuwOONPp4JAOGfbCEpYb+K6DVWvdxGzgM=
	dep	go.opentelemetry.io/otel/trace	v1.43.0	h1:BkNrHpup+4k4w+ZZ86CZoHHEkohws8AY+WTX09nk+3A=
	dep	go.opentelemetry.io/proto/otlp	v1.10.0	h1:IQRWgT5srOCYfiWnpqUYz9CVmbO8bFmKcwYxpuCSL2g=
	dep	go.starlark.net	v0.0.0-20250701195324-d457b4515e0e	h1:/WX+ZvcgVJxdIxVR9J3u45ds+Bl4IWPIHRSSICp0t3Q=
	dep	go.uber.org/automaxprocs	v1.5.3	h1:kWazyxZUrS3Gs4qUpbwo5kEIMGe/DAvi5Z4tl2NW4j8=
	dep	go.uber.org/multierr	v1.11.0	h1:blXXJkSxSSfBVBlC76pxqeO+LN3aDfLQo+309xJstO0=
	dep	go.uber.org/ratelimit	v0.2.0	h1:UQE2Bgi7p2B85uP5dC2bbRtig0C+OeNRnNEafLjsLPA=
	dep	go.uber.org/zap	v1.27.1	h1:08RqriUEv8+ArZRYSTXy1LeBScaMpVSTBhCeaZYfMYc=
	dep	go.yaml.in/yaml/v2	v2.4.4	h1:tuyd0P+2Ont/d6e2rl3be67goVK4R6deVxCUX5vyPaQ=
	dep	go.yaml.in/yaml/v3	v3.0.4	h1:tfq32ie2Jv2UxXFdLJdh3jXuOzWiL1fo0bu/FbuKpbc=
	dep	golang.org/x/crypto	v0.51.0	h1:IBPXwPfKxY7cWQZ38ZCIRPI50YLeevDLlLnyC5wRGTI=
	dep	golang.org/x/exp	v0.0.0-20260410095643-746e56fc9e2f	h1:W3F4c+6OLc6H2lb//N1q4WpJkhzJCK5J6kUi1NTVXfM=
	dep	golang.org/x/net	v0.55.0	h1:bcvxaJn3e1U6InsFWt1JUq1aSjnRxLzT2rtD2KfkDF8=
	dep	golang.org/x/sync	v0.22.0	h1:SZjpbeLmrCk4xhRSZFNZW5gFUeCeFgjekvI/+gfScek=
	dep	golang.org/x/sys	v0.47.0	h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs=
	dep	golang.org/x/text	v0.37.0	h1:Cqjiwd9eSg8e0QAkyCaQTNHFIIzWtidPahFWR83rTrc=
	dep	golang.org/x/time	v0.15.0	h1:bbrp8t3bGUeFOx08pvsMYRTCVSMk89u4tKbNOZbp88U=
	dep	gonum.org/v1/gonum	v0.17.0	h1:VbpOemQlsSMrYmn7T2OUvQ4dqxQXU+ouZFQsZOx50z4=
	dep	google.golang.org/genproto/googleapis/rpc	v0.0.0-20260414002931-afd174a4e478	h1:RmoJA1ujG+/lRGNfUnOMfhCy5EipVMyvUE+KNbPbTlw=
	dep	google.golang.org/grpc	v1.82.0	h1:vguDnZUPjE26w09A63VoxZPnvPjB5Riyc0mkXPFmAIU=
	dep	google.golang.org/protobuf	v1.36.11	h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=
	dep	gopkg.in/ini.v1	v1.67.1	h1:tVBILHy0R6e4wkYOn3XmiITt/hEVH4TFMYvAX2Ytz6k=
	dep	gopkg.in/natefinch/lumberjack.v2	v2.2.1	h1:bBRl1b0OH9s/DuPhuXpNl+VtCaJXFZ5/uEFST95x9zc=
	dep	gopkg.in/yaml.v3	v3.0.1	h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=
	dep	vitess.io/vitess	v0.24.0	h1:1beFkQ3hbozQv0pmuicu1LL22szH7yVwZR/fofGiXxk=
	build	-buildmode=exe
	build	-compiler=gc
	build	-ldflags="-extldflags '-L<worktree>/cgo -lmo -L<worktree>/thirdparties/install/lib -Wl,-rpath,@executable_path/lib' -X 'github.com/matrixorigin/matrixone/pkg/version.GoVersion=go version go1.27.1 darwin/arm64' -X 'github.com/matrixorigin/matrixone/pkg/version.BranchName=issue-30107-sca-main' -X 'github.com/matrixorigin/matrixone/pkg/version.CommitID=b812d7b6b3' -X 'github.com/matrixorigin/matrixone/pkg/version.BuildTime=1792036143' -X 'github.com/matrixorigin/matrixone/pkg/version.Version=issue-30107-sca-main'"
	build	CGO_ENABLED=1
	build	CGO_CFLAGS="-I<worktree>/cgo -I<worktree>/thirdparties/install/include  "
	build	CGO_CPPFLAGS=
	build	CGO_CXXFLAGS=
	build	CGO_LDFLAGS=
	build	GOARCH=arm64
	build	GOEXPERIMENT=simd
	build	GOOS=darwin
	build	GOARM64=v8.0
	build	vcs=git
	build	vcs.revision=b812d7b6b3ed6b463065789903c7034b7b8f28ef
	build	vcs.time=2026-10-15T03:16:33Z
	build	vcs.modified=true
```

- 对称插桩源指纹：

```json
{
  "before": {
    "pkg/vm/engine/readutil/reader.go": "ba338c488b0ad5e4f8511925dffbacdfd439bece5b5d35168c9273c6b50c342d",
    "pkg/incrservice/allocator.go": "4ccea6a24443aa49108af591a47b19c0b9ff8c4aa2a9ee85636a3e4c817aa305"
  },
  "after": {
    "pkg/vm/engine/readutil/reader.go": "6de423432f81c0a580e8ce506b7833544eb6333153af597f8844d8e0d2de34ee",
    "pkg/incrservice/allocator.go": "89fc97f7c05fdc84f74dc1cc7be824747153ba79a69ebbe9fdb3643d762ff61f",
    "pkg/perfcounter/CLAUDE_fkbench.go": "dc67e97f7573913de7e8ef370746d7c032697944502b0dcb6c55a3dcefa94a0f",
    "pkg/vm/engine/readutil/CLAUDE_fkbench.go": "f8c4718329319d7f8bf671d87647b44b4f9a60c98d1ec083c488026ccc40ff7f"
  }
}
```

计量包含公开 SQL frontend 和事务；DDL/seed/flush/oracle 与计量 HTTP 排除，commit 独立记录。父表已 flush，但未清空任何缓存，不能声称冷盘结果。两端采用相同观察点；ctx 附加块 CounterSet 强制覆盖无 PK filter 的 reader，避免旧全局 counter 仅在 filter.Valid 时生效而把缺失 I/O 错记为零。

`R/B` 是 reader 返回给执行层的逻辑行数/batch Size，不是底层全行 predicate 次数或磁盘字节；`selected block rows` 是读块原始行数，不能等同完全解码行。FS bytes 是读取 storage layer（含缓存）的进程计数窗口差，不是父表独占磁盘/S3 字节。peak 是 10ms 进程采样绝对峰值，非语句独占或精确高水位。少量样本只报告范围/中位数，不报告 p99。

## 各组合中位数

| shape | P | N | mode | base exec ms | head exec ms | head/base | base R/B | head R/B | head reader calls | allocator calls base/head |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- | --- | ---: | --- |
| auto_composite | 256 | 16 | insert | 11.908 | 8.400 | 0.71 | 256/4096 | 4096/65536 | 16 | 0/0 |
| auto_composite | 256 | 16 | load | 8.250 | 6.182 | 0.75 | 256/4096 | 4096/65536 | 16 | 0/0 |
| auto_composite | 256 | 16 | local | 23.233 | 24.485 | 1.05 | 256/4096 | 4096/65536 | 16 | 0/0 |
| auto_composite | 256 | 64 | insert | 15.915 | 22.137 | 1.39 | 256/4096 | 16384/262144 | 64 | 0/0 |
| auto_composite | 256 | 64 | load | 37.766 | 9.306 | 0.25 | 256/4096 | 16384/262144 | 64 | 0/0 |
| auto_composite | 256 | 64 | local | 39.796 | 20.634 | 0.52 | 256/4096 | 16384/262144 | 64 | 0/0 |
| auto_composite | 256 | 256 | insert | 15.869 | 29.028 | 1.83 | 256/4096 | 65536/1048576 | 256 | 0/0 |
| auto_composite | 256 | 256 | load | 11.408 | 37.731 | 3.31 | 256/4096 | 65536/1048576 | 256 | 0/0 |
| auto_composite | 256 | 256 | local | 28.079 | 46.085 | 1.64 | 256/4096 | 65536/1048576 | 256 | 0/0 |
| auto_composite | 4096 | 16 | insert | 7.174 | 9.219 | 1.28 | 4096/65536 | 65536/1048576 | 16 | 0/0 |
| auto_composite | 4096 | 16 | load | 6.208 | 6.981 | 1.12 | 4096/65536 | 65536/1048576 | 16 | 0/0 |
| auto_composite | 4096 | 16 | local | 19.055 | 22.239 | 1.17 | 4096/65536 | 65536/1048576 | 16 | 0/0 |
| auto_composite | 4096 | 64 | insert | 9.168 | 10.934 | 1.19 | 4096/65536 | 262144/4194304 | 64 | 0/0 |
| auto_composite | 4096 | 64 | load | 10.470 | 11.204 | 1.07 | 4096/65536 | 262144/4194304 | 64 | 0/0 |
| auto_composite | 4096 | 64 | local | 34.848 | 28.106 | 0.81 | 4096/65536 | 262144/4194304 | 64 | 0/0 |
| auto_composite | 4096 | 256 | insert | 66.376 | 64.696 | 0.97 | 4096/65536 | 1048576/16777216 | 256 | 0/0 |
| auto_composite | 4096 | 256 | load | 30.907 | 37.032 | 1.20 | 4096/65536 | 1048576/16777216 | 256 | 0/0 |
| auto_composite | 4096 | 256 | local | 50.922 | 126.009 | 2.47 | 4096/65536 | 1048576/16777216 | 256 | 0/0 |
| auto_nonpk | 256 | 16 | insert | 8.702 | 4.908 | 0.56 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_nonpk | 256 | 16 | load | 5.419 | 22.153 | 4.09 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_nonpk | 256 | 16 | local | 23.800 | 52.329 | 2.20 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_nonpk | 256 | 64 | insert | 17.097 | 12.073 | 0.71 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_nonpk | 256 | 64 | load | 6.526 | 12.735 | 1.95 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_nonpk | 256 | 64 | local | 24.373 | 30.031 | 1.23 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_nonpk | 256 | 256 | insert | 17.445 | 66.768 | 3.83 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_nonpk | 256 | 256 | load | 6.564 | 21.647 | 3.30 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_nonpk | 256 | 256 | local | 17.332 | 28.498 | 1.64 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_nonpk | 4096 | 16 | insert | 13.167 | 6.681 | 0.51 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_nonpk | 4096 | 16 | load | 6.001 | 11.506 | 1.92 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_nonpk | 4096 | 16 | local | 19.855 | 15.168 | 0.76 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_nonpk | 4096 | 64 | insert | 7.571 | 8.999 | 1.19 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_nonpk | 4096 | 64 | load | 7.284 | 8.536 | 1.17 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_nonpk | 4096 | 64 | local | 21.592 | 21.693 | 1.00 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_nonpk | 4096 | 256 | insert | 14.569 | 31.336 | 2.15 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| auto_nonpk | 4096 | 256 | load | 6.742 | 25.668 | 3.81 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| auto_nonpk | 4096 | 256 | local | 20.652 | 31.183 | 1.51 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| auto_pk | 256 | 16 | insert | 10.952 | 9.798 | 0.89 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_pk | 256 | 16 | load | 4.609 | 9.499 | 2.06 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_pk | 256 | 16 | local | 24.557 | 20.335 | 0.83 | 256/2048 | 4096/32768 | 16 | 0/0 |
| auto_pk | 256 | 64 | insert | 7.805 | 12.606 | 1.62 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_pk | 256 | 64 | load | 12.712 | 10.888 | 0.86 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_pk | 256 | 64 | local | 32.909 | 17.578 | 0.53 | 256/2048 | 16384/131072 | 64 | 0/0 |
| auto_pk | 256 | 256 | insert | 9.277 | 31.781 | 3.43 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_pk | 256 | 256 | load | 8.438 | 33.091 | 3.92 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_pk | 256 | 256 | local | 29.197 | 42.163 | 1.44 | 256/2048 | 65536/524288 | 256 | 0/0 |
| auto_pk | 4096 | 16 | insert | 11.044 | 6.942 | 0.63 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_pk | 4096 | 16 | load | 4.866 | 7.857 | 1.61 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_pk | 4096 | 16 | local | 12.264 | 18.842 | 1.54 | 4096/32768 | 65536/524288 | 16 | 0/0 |
| auto_pk | 4096 | 64 | insert | 14.068 | 7.981 | 0.57 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_pk | 4096 | 64 | load | 6.370 | 9.872 | 1.55 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_pk | 4096 | 64 | local | 14.518 | 63.024 | 4.34 | 4096/32768 | 262144/2097152 | 64 | 0/0 |
| auto_pk | 4096 | 256 | insert | 13.870 | 20.894 | 1.51 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| auto_pk | 4096 | 256 | load | 22.245 | 28.462 | 1.28 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| auto_pk | 4096 | 256 | local | 49.015 | 39.898 | 0.81 | 4096/32768 | 1048576/8388608 | 256 | 0/0 |
| ordinary | 256 | 16 | insert | 9.432 | 8.015 | 0.85 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 16 | load | 9.628 | 7.894 | 0.82 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 16 | local | 31.797 | 24.534 | 0.77 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 64 | insert | 10.250 | 4.921 | 0.48 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 64 | load | 5.901 | 6.892 | 1.17 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 64 | local | 25.201 | 27.880 | 1.11 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 256 | insert | 11.700 | 10.179 | 0.87 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 256 | load | 5.330 | 11.429 | 2.14 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 256 | 256 | local | 29.182 | 34.816 | 1.19 | 256/2048 | 256/2048 | 2 | 0/0 |
| ordinary | 4096 | 16 | insert | 4.792 | 4.871 | 1.02 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 16 | load | 7.241 | 5.717 | 0.79 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 16 | local | 15.404 | 25.140 | 1.63 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 64 | insert | 10.795 | 8.353 | 0.77 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 64 | load | 5.694 | 7.135 | 1.25 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 64 | local | 16.565 | 30.013 | 1.81 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 256 | insert | 13.967 | 13.235 | 0.95 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 256 | load | 6.222 | 13.297 | 2.14 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| ordinary | 4096 | 256 | local | 15.051 | 36.344 | 2.41 | 4096/32768 | 4096/32768 | 2 | 0/0 |
| self_composite | 256 | 16 | insert | 8.107 | 9.998 | 1.23 | 800/10752 | 4352/67584 | 18 | 0/0 |
| self_composite | 256 | 16 | load | 18.348 | 4.723 | 0.26 | 544/8704 | 4352/67584 | 18 | 0/0 |
| self_composite | 256 | 16 | local | 61.278 | 27.716 | 0.45 | 544/8704 | 4352/67584 | 18 | 0/0 |
| self_composite | 256 | 64 | insert | 17.423 | 18.357 | 1.05 | 896/12288 | 16640/264192 | 66 | 0/0 |
| self_composite | 256 | 64 | load | 8.079 | 16.061 | 1.99 | 640/10240 | 16640/264192 | 66 | 0/0 |
| self_composite | 256 | 64 | local | 37.293 | 34.900 | 0.94 | 640/10240 | 16640/264192 | 66 | 0/0 |
| self_composite | 256 | 256 | insert | 27.067 | 33.385 | 1.23 | 1280/18432 | 65792/1050624 | 258 | 0/0 |
| self_composite | 256 | 256 | load | 4.625 | 56.742 | 12.27 | 1024/16384 | 65792/1050624 | 258 | 0/0 |
| self_composite | 256 | 256 | local | 55.825 | 85.165 | 1.53 | 1024/16384 | 65792/1050624 | 258 | 0/0 |
| self_composite | 4096 | 16 | insert | 26.022 | 15.165 | 0.58 | 12320/164352 | 69632/1081344 | 18 | 0/0 |
| self_composite | 4096 | 16 | load | 4.687 | 18.245 | 3.89 | 8224/131584 | 69632/1081344 | 18 | 0/0 |
| self_composite | 4096 | 16 | local | 27.423 | 34.785 | 1.27 | 8224/131584 | 69632/1081344 | 18 | 0/0 |
| self_composite | 4096 | 64 | insert | 173.757 | 56.875 | 0.33 | 12416/165888 | 266240/4227072 | 66 | 0/0 |
| self_composite | 4096 | 64 | load | 10.946 | 83.533 | 7.63 | 8320/133120 | 266240/4227072 | 66 | 0/0 |
| self_composite | 4096 | 64 | local | 39.382 | 72.524 | 1.84 | 8320/133120 | 266240/4227072 | 66 | 0/0 |
| self_composite | 4096 | 256 | insert | 310.723 | 275.648 | 0.89 | 12800/172032 | 1052672/16809984 | 258 | 0/0 |
| self_composite | 4096 | 256 | load | 4.956 | 386.453 | 77.98 | 8704/139264 | 1052672/16809984 | 258 | 0/0 |
| self_composite | 4096 | 256 | local | 49.551 | 431.083 | 8.70 | 8704/139264 | 1052672/16809984 | 258 | 0/0 |

## 逐样本原始计量

表中时间保留至微秒精度（ms 三位小数），原始 JSONL 保留完整浮点值；执行吞吐可由 N/(exec ms/1000) 复算，含 commit 吞吐由 N/((exec+commit)/1000) 复算。

| variant | shape | P | N | mode | trial | exec ms | commit ms | rows | bytes | block rows | FS bytes | allocator calls/values | heap peak bytes | RSS peak bytes | memory samples |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| base | ordinary | 256 | 16 | insert | 0 | 12.148 | 19.407 | 256 | 2048 | 256 | 1072 | 0/0 | 169227672 | 250052608 | 5 |
| base | ordinary | 256 | 16 | insert | 1 | 9.262 | 6.060 | 256 | 2048 | 256 | 1072 | 0/0 | 180424984 | 250691584 | 3 |
| base | ordinary | 256 | 16 | insert | 2 | 9.432 | 6.331 | 256 | 2048 | 256 | 1072 | 0/0 | 191217464 | 243105792 | 3 |
| base | ordinary | 256 | 16 | load | 0 | 9.628 | 7.372 | 256 | 2048 | 256 | 1072 | 0/0 | 201410184 | 247857152 | 3 |
| base | ordinary | 256 | 16 | load | 1 | 14.052 | 7.979 | 256 | 2048 | 256 | 1072 | 0/0 | 158501704 | 260636672 | 4 |
| base | ordinary | 256 | 16 | load | 2 | 6.502 | 12.413 | 256 | 2048 | 256 | 1072 | 0/0 | 169629720 | 260947968 | 4 |
| base | ordinary | 256 | 16 | local | 0 | 34.076 | 15.638 | 256 | 2048 | 256 | 1072 | 0/0 | 181078904 | 262160384 | 7 |
| base | ordinary | 256 | 16 | local | 1 | 31.797 | 18.723 | 256 | 2048 | 256 | 1072 | 0/0 | 191680584 | 256409600 | 7 |
| base | ordinary | 256 | 16 | local | 2 | 15.603 | 6.633 | 256 | 2048 | 256 | 1072 | 0/0 | 202456584 | 245956608 | 4 |
| base | ordinary | 256 | 64 | insert | 0 | 10.250 | 5.124 | 256 | 2048 | 256 | 1072 | 0/0 | 213010944 | 251363328 | 3 |
| base | ordinary | 256 | 64 | insert | 1 | 13.276 | 17.931 | 256 | 2048 | 256 | 1072 | 0/0 | 225377280 | 280068096 | 4 |
| base | ordinary | 256 | 64 | insert | 2 | 7.630 | 8.540 | 256 | 2048 | 256 | 1072 | 0/0 | 164528952 | 281018368 | 3 |
| base | ordinary | 256 | 64 | load | 0 | 10.389 | 13.788 | 256 | 2048 | 256 | 1072 | 0/0 | 174659408 | 281706496 | 4 |
| base | ordinary | 256 | 64 | load | 1 | 5.851 | 8.932 | 256 | 2048 | 256 | 1072 | 0/0 | 185028408 | 281788416 | 3 |
| base | ordinary | 256 | 64 | load | 2 | 5.901 | 6.091 | 256 | 2048 | 256 | 1072 | 0/0 | 195354368 | 281985024 | 3 |
| base | ordinary | 256 | 64 | local | 0 | 25.201 | 5.097 | 256 | 2048 | 256 | 1072 | 0/0 | 206735720 | 282378240 | 5 |
| base | ordinary | 256 | 64 | local | 1 | 36.237 | 33.847 | 256 | 2048 | 256 | 1072 | 0/0 | 218666848 | 281296896 | 9 |
| base | ordinary | 256 | 64 | local | 2 | 16.977 | 17.906 | 256 | 2048 | 256 | 1072 | 0/0 | 162044856 | 282673152 | 5 |
| base | ordinary | 256 | 256 | insert | 0 | 10.519 | 4.683 | 256 | 2048 | 256 | 1072 | 0/0 | 176516440 | 282853376 | 3 |
| base | ordinary | 256 | 256 | insert | 1 | 11.700 | 5.058 | 256 | 2048 | 256 | 1072 | 0/0 | 190641440 | 282411008 | 3 |
| base | ordinary | 256 | 256 | insert | 2 | 14.284 | 9.063 | 256 | 2048 | 256 | 1072 | 0/0 | 204814712 | 282116096 | 4 |
| base | ordinary | 256 | 256 | load | 0 | 5.592 | 7.520 | 256 | 2048 | 256 | 1072 | 0/0 | 216034664 | 283312128 | 3 |
| base | ordinary | 256 | 256 | load | 1 | 4.317 | 8.204 | 256 | 2048 | 256 | 1072 | 0/0 | 214169936 | 286081024 | 3 |
| base | ordinary | 256 | 256 | load | 2 | 5.330 | 9.525 | 256 | 2048 | 256 | 1072 | 0/0 | 171907608 | 286425088 | 3 |
| base | ordinary | 256 | 256 | local | 0 | 29.182 | 11.000 | 256 | 2048 | 256 | 1072 | 0/0 | 182642720 | 286425088 | 6 |
| base | ordinary | 256 | 256 | local | 1 | 19.475 | 15.890 | 256 | 2048 | 256 | 1072 | 0/0 | 194598648 | 286507008 | 5 |
| base | ordinary | 256 | 256 | local | 2 | 39.165 | 11.442 | 256 | 2048 | 256 | 1072 | 0/0 | 205365480 | 286703616 | 7 |
| base | auto_pk | 256 | 16 | insert | 0 | 10.952 | 7.380 | 256 | 2048 | 256 | 1072 | 0/0 | 217119616 | 286883840 | 3 |
| base | auto_pk | 256 | 16 | insert | 1 | 18.803 | 13.892 | 256 | 2048 | 256 | 1072 | 0/0 | 175299128 | 288358400 | 5 |
| base | auto_pk | 256 | 16 | insert | 2 | 7.602 | 9.429 | 256 | 2048 | 256 | 1072 | 0/0 | 172584864 | 288505856 | 3 |
| base | auto_pk | 256 | 16 | load | 0 | 4.609 | 6.961 | 256 | 2048 | 256 | 1072 | 0/0 | 183666304 | 288407552 | 3 |
| base | auto_pk | 256 | 16 | load | 1 | 4.141 | 8.792 | 256 | 2048 | 256 | 1072 | 0/0 | 194879376 | 286900224 | 3 |
| base | auto_pk | 256 | 16 | load | 2 | 10.197 | 10.907 | 256 | 2048 | 256 | 1072 | 0/0 | 206195104 | 285851648 | 4 |
| base | auto_pk | 256 | 16 | local | 0 | 19.455 | 19.728 | 256 | 2048 | 256 | 1072 | 0/0 | 218942112 | 284753920 | 5 |
| base | auto_pk | 256 | 16 | local | 1 | 36.542 | 6.350 | 256 | 2048 | 256 | 1072 | 0/0 | 189078744 | 286310400 | 6 |
| base | auto_pk | 256 | 16 | local | 2 | 24.557 | 10.668 | 256 | 2048 | 256 | 1072 | 0/0 | 175364024 | 286326784 | 5 |
| base | auto_pk | 256 | 64 | insert | 0 | 7.805 | 7.676 | 256 | 2048 | 256 | 1072 | 0/0 | 186822096 | 286343168 | 3 |
| base | auto_pk | 256 | 64 | insert | 1 | 5.201 | 6.046 | 256 | 2048 | 256 | 1072 | 0/0 | 198172800 | 286261248 | 3 |
| base | auto_pk | 256 | 64 | insert | 2 | 9.395 | 11.482 | 256 | 2048 | 256 | 1072 | 0/0 | 209569448 | 286408704 | 4 |
| base | auto_pk | 256 | 64 | load | 0 | 2.777 | 6.202 | 256 | 2048 | 256 | 1072 | 0/0 | 223121424 | 287145984 | 3 |
| base | auto_pk | 256 | 64 | load | 1 | 18.597 | 7.509 | 256 | 2048 | 256 | 1072 | 0/0 | 169174880 | 288948224 | 4 |
| base | auto_pk | 256 | 64 | load | 2 | 12.712 | 7.337 | 256 | 2048 | 256 | 1072 | 0/0 | 192893704 | 297615360 | 4 |
| base | auto_pk | 256 | 64 | local | 0 | 24.400 | 8.608 | 256 | 2048 | 256 | 1072 | 0/0 | 204566560 | 297828352 | 5 |
| base | auto_pk | 256 | 64 | local | 1 | 34.468 | 16.380 | 256 | 2048 | 256 | 1072 | 0/0 | 217556976 | 291487744 | 7 |
| base | auto_pk | 256 | 64 | local | 2 | 32.909 | 7.063 | 256 | 2048 | 256 | 1072 | 0/0 | 229230096 | 242941952 | 6 |
| base | auto_pk | 256 | 256 | insert | 0 | 12.843 | 13.744 | 256 | 2048 | 256 | 1072 | 0/0 | 177524976 | 293273600 | 4 |
| base | auto_pk | 256 | 256 | insert | 1 | 9.277 | 5.879 | 256 | 2048 | 256 | 1072 | 0/0 | 192355176 | 293027840 | 3 |
| base | auto_pk | 256 | 256 | insert | 2 | 8.504 | 19.859 | 256 | 2048 | 256 | 1072 | 0/0 | 207838248 | 292290560 | 4 |
| base | auto_pk | 256 | 256 | load | 0 | 7.968 | 13.087 | 256 | 2048 | 256 | 1072 | 0/0 | 219881744 | 294633472 | 4 |
| base | auto_pk | 256 | 256 | load | 1 | 8.438 | 16.433 | 256 | 2048 | 256 | 1072 | 0/0 | 234360616 | 299597824 | 4 |
| base | auto_pk | 256 | 256 | load | 2 | 15.965 | 37.501 | 256 | 2048 | 256 | 1072 | 0/0 | 179095256 | 300728320 | 6 |
| base | auto_pk | 256 | 256 | local | 0 | 73.170 | 28.270 | 256 | 2048 | 256 | 1072 | 0/0 | 194970408 | 299417600 | 10 |
| base | auto_pk | 256 | 256 | local | 1 | 29.197 | 10.770 | 256 | 2048 | 256 | 1072 | 0/0 | 210336416 | 287899648 | 6 |
| base | auto_pk | 256 | 256 | local | 2 | 19.197 | 13.436 | 256 | 2048 | 256 | 1072 | 0/0 | 222770352 | 288849920 | 5 |
| base | auto_nonpk | 256 | 16 | insert | 0 | 13.758 | 5.305 | 256 | 2048 | 256 | 1072 | 0/0 | 234105472 | 294092800 | 3 |
| base | auto_nonpk | 256 | 16 | insert | 1 | 8.702 | 7.856 | 256 | 2048 | 256 | 1072 | 0/0 | 179500656 | 294813696 | 3 |
| base | auto_nonpk | 256 | 16 | insert | 2 | 6.037 | 7.400 | 256 | 2048 | 256 | 1072 | 0/0 | 190602496 | 295387136 | 3 |
| base | auto_nonpk | 256 | 16 | load | 0 | 6.169 | 12.112 | 256 | 2048 | 256 | 1072 | 0/0 | 203135488 | 294273024 | 3 |
| base | auto_nonpk | 256 | 16 | load | 1 | 5.231 | 5.718 | 256 | 2048 | 256 | 1072 | 0/0 | 215051504 | 289751040 | 3 |
| base | auto_nonpk | 256 | 16 | load | 2 | 5.419 | 16.062 | 256 | 2048 | 256 | 1072 | 0/0 | 226419792 | 292306944 | 4 |
| base | auto_nonpk | 256 | 16 | local | 0 | 31.630 | 9.577 | 256 | 2048 | 256 | 1072 | 0/0 | 237082192 | 294993920 | 6 |
| base | auto_nonpk | 256 | 16 | local | 1 | 18.341 | 19.213 | 256 | 2048 | 256 | 1072 | 0/0 | 182374352 | 295239680 | 5 |
| base | auto_nonpk | 256 | 16 | local | 2 | 23.800 | 31.052 | 256 | 2048 | 256 | 1072 | 0/0 | 195104976 | 294469632 | 6 |
| base | auto_nonpk | 256 | 64 | insert | 0 | 22.445 | 7.278 | 256 | 2048 | 256 | 1072 | 0/0 | 210584376 | 294125568 | 5 |
| base | auto_nonpk | 256 | 64 | insert | 1 | 17.097 | 16.025 | 256 | 2048 | 256 | 1072 | 0/0 | 227225232 | 293257216 | 5 |
| base | auto_nonpk | 256 | 64 | insert | 2 | 14.151 | 10.433 | 256 | 2048 | 256 | 1072 | 0/0 | 177229144 | 296435712 | 4 |
| base | auto_nonpk | 256 | 64 | load | 0 | 5.886 | 6.781 | 256 | 2048 | 256 | 1072 | 0/0 | 189154072 | 296140800 | 3 |
| base | auto_nonpk | 256 | 64 | load | 1 | 6.526 | 6.335 | 256 | 2048 | 256 | 1072 | 0/0 | 200675352 | 295944192 | 3 |
| base | auto_nonpk | 256 | 64 | load | 2 | 6.926 | 9.645 | 256 | 2048 | 256 | 1072 | 0/0 | 212088368 | 292749312 | 3 |
| base | auto_nonpk | 256 | 64 | local | 0 | 24.373 | 19.431 | 256 | 2048 | 256 | 1072 | 0/0 | 223938208 | 292421632 | 6 |
| base | auto_nonpk | 256 | 64 | local | 1 | 21.618 | 11.991 | 256 | 2048 | 256 | 1072 | 0/0 | 236696792 | 292503552 | 4 |
| base | auto_nonpk | 256 | 64 | local | 2 | 29.001 | 9.339 | 256 | 2048 | 256 | 1072 | 0/0 | 185307968 | 300908544 | 6 |
| base | auto_nonpk | 256 | 256 | insert | 0 | 14.284 | 7.316 | 256 | 2048 | 256 | 1072 | 0/0 | 193873760 | 297811968 | 4 |
| base | auto_nonpk | 256 | 256 | insert | 1 | 19.804 | 11.839 | 256 | 2048 | 256 | 1072 | 0/0 | 208875024 | 300466176 | 5 |
| base | auto_nonpk | 256 | 256 | insert | 2 | 17.445 | 15.889 | 256 | 2048 | 256 | 1072 | 0/0 | 224139272 | 300040192 | 5 |
| base | auto_nonpk | 256 | 256 | load | 0 | 6.564 | 9.419 | 256 | 2048 | 256 | 1072 | 0/0 | 236360624 | 299106304 | 3 |
| base | auto_nonpk | 256 | 256 | load | 1 | 13.286 | 9.159 | 256 | 2048 | 256 | 1072 | 0/0 | 248827376 | 304316416 | 4 |
| base | auto_nonpk | 256 | 256 | load | 2 | 6.031 | 8.133 | 256 | 2048 | 256 | 1072 | 0/0 | 188459760 | 304758784 | 3 |
| base | auto_nonpk | 256 | 256 | local | 0 | 25.565 | 7.169 | 256 | 2048 | 256 | 1072 | 0/0 | 200386480 | 304955392 | 5 |
| base | auto_nonpk | 256 | 256 | local | 1 | 13.411 | 6.258 | 256 | 2048 | 256 | 1072 | 0/0 | 212349656 | 305053696 | 4 |
| base | auto_nonpk | 256 | 256 | local | 2 | 17.332 | 6.025 | 256 | 2048 | 256 | 1072 | 0/0 | 224261088 | 305020928 | 4 |
| base | auto_composite | 256 | 16 | insert | 0 | 7.830 | 5.674 | 256 | 4096 | 256 | 1134 | 0/0 | 236899240 | 305184768 | 3 |
| base | auto_composite | 256 | 16 | insert | 1 | 16.747 | 7.599 | 256 | 4096 | 256 | 1134 | 0/0 | 178669256 | 306085888 | 4 |
| base | auto_composite | 256 | 16 | insert | 2 | 11.908 | 7.823 | 256 | 4096 | 256 | 1134 | 0/0 | 196592816 | 306364416 | 4 |
| base | auto_composite | 256 | 16 | load | 0 | 5.651 | 4.561 | 256 | 4096 | 256 | 1134 | 0/0 | 208257608 | 305790976 | 3 |
| base | auto_composite | 256 | 16 | load | 1 | 8.250 | 19.395 | 256 | 4096 | 256 | 1134 | 0/0 | 245671456 | 332726272 | 3 |
| base | auto_composite | 256 | 16 | load | 2 | 10.206 | 6.526 | 256 | 4096 | 256 | 1134 | 0/0 | 232563232 | 362496000 | 3 |
| base | auto_composite | 256 | 16 | local | 0 | 18.287 | 6.911 | 256 | 4096 | 256 | 1134 | 0/0 | 244610352 | 341311488 | 4 |
| base | auto_composite | 256 | 16 | local | 1 | 23.233 | 11.275 | 256 | 4096 | 256 | 1134 | 0/0 | 256817712 | 278151168 | 5 |
| base | auto_composite | 256 | 16 | local | 2 | 49.864 | 18.686 | 256 | 4096 | 256 | 1134 | 0/0 | 270888784 | 322207744 | 7 |
| base | auto_composite | 256 | 64 | insert | 0 | 25.713 | 8.398 | 256 | 4096 | 256 | 1134 | 0/0 | 192407328 | 323092480 | 5 |
| base | auto_composite | 256 | 64 | insert | 1 | 11.801 | 14.144 | 256 | 4096 | 256 | 1134 | 0/0 | 206147568 | 317734912 | 4 |
| base | auto_composite | 256 | 64 | insert | 2 | 15.915 | 51.642 | 256 | 4096 | 256 | 1134 | 0/0 | 221876560 | 307412992 | 7 |
| base | auto_composite | 256 | 64 | load | 0 | 37.766 | 21.453 | 256 | 4096 | 256 | 1134 | 0/0 | 237581432 | 290160640 | 7 |
| base | auto_composite | 256 | 64 | load | 1 | 56.910 | 14.700 | 256 | 4096 | 256 | 1134 | 0/0 | 191684208 | 301105152 | 9 |
| base | auto_composite | 256 | 64 | load | 2 | 11.314 | 37.284 | 256 | 4096 | 256 | 1134 | 0/0 | 207935664 | 293535744 | 5 |
| base | auto_composite | 256 | 64 | local | 0 | 68.276 | 5.541 | 256 | 4096 | 256 | 1134 | 0/0 | 222721288 | 276430848 | 9 |
| base | auto_composite | 256 | 64 | local | 1 | 39.796 | 31.210 | 256 | 4096 | 256 | 1134 | 0/0 | 236825360 | 222199808 | 8 |
| base | auto_composite | 256 | 64 | local | 2 | 21.397 | 9.060 | 256 | 4096 | 256 | 1134 | 0/0 | 248982096 | 206225408 | 5 |
| base | auto_composite | 256 | 256 | insert | 0 | 30.086 | 33.034 | 256 | 4096 | 256 | 1134 | 0/0 | 265458296 | 306987008 | 7 |
| base | auto_composite | 256 | 256 | insert | 1 | 13.320 | 10.651 | 256 | 4096 | 256 | 1134 | 0/0 | 204978288 | 310853632 | 4 |
| base | auto_composite | 256 | 256 | insert | 2 | 15.869 | 9.272 | 256 | 4096 | 256 | 1134 | 0/0 | 220961944 | 311410688 | 4 |
| base | auto_composite | 256 | 256 | load | 0 | 11.408 | 9.661 | 256 | 4096 | 256 | 1134 | 0/0 | 232921992 | 312164352 | 4 |
| base | auto_composite | 256 | 256 | load | 1 | 9.330 | 10.419 | 256 | 4096 | 256 | 1134 | 0/0 | 245223112 | 314130432 | 4 |
| base | auto_composite | 256 | 256 | load | 2 | 17.798 | 10.346 | 256 | 4096 | 256 | 1134 | 0/0 | 256790344 | 316473344 | 5 |
| base | auto_composite | 256 | 256 | local | 0 | 28.079 | 19.684 | 256 | 4096 | 256 | 1134 | 0/0 | 198540096 | 316358656 | 6 |
| base | auto_composite | 256 | 256 | local | 1 | 39.289 | 12.971 | 256 | 4096 | 256 | 1134 | 0/0 | 210798560 | 315260928 | 7 |
| base | auto_composite | 256 | 256 | local | 2 | 19.781 | 4.957 | 256 | 4096 | 256 | 1134 | 0/0 | 223115200 | 297549824 | 4 |
| base | self_composite | 256 | 16 | insert | 0 | 16.628 | 13.707 | 800 | 10752 | 768 | 4471 | 0/0 | 244079192 | 195739648 | 5 |
| base | self_composite | 256 | 16 | insert | 1 | 8.107 | 7.700 | 800 | 10752 | 768 | 5308 | 0/0 | 199402568 | 292110336 | 3 |
| base | self_composite | 256 | 16 | insert | 2 | 4.443 | 30.702 | 800 | 10752 | 768 | 3729 | 0/0 | 219846720 | 185286656 | 4 |
| base | self_composite | 256 | 16 | load | 0 | 19.888 | 12.512 | 544 | 8704 | 512 | 2309 | 0/0 | 199550312 | 286310400 | 5 |
| base | self_composite | 256 | 16 | load | 1 | 9.564 | 10.793 | 544 | 8704 | 512 | 2332 | 0/0 | 246823824 | 210960384 | 4 |
| base | self_composite | 256 | 16 | load | 2 | 18.348 | 46.970 | 544 | 8704 | 512 | 2377 | 0/0 | 210724736 | 293126144 | 8 |
| base | self_composite | 256 | 16 | local | 0 | 61.278 | 7.585 | 544 | 8704 | 544 | 3493 | 0/0 | 232101160 | 182665216 | 7 |
| base | self_composite | 256 | 16 | local | 1 | 60.300 | 10.703 | 544 | 8704 | 544 | 3542 | 0/0 | 257751736 | 184483840 | 8 |
| base | self_composite | 256 | 16 | local | 2 | 88.352 | 37.409 | 544 | 8704 | 544 | 3226 | 0/0 | 193834440 | 286539776 | 7 |
| base | self_composite | 256 | 64 | insert | 0 | 17.423 | 12.579 | 896 | 12288 | 768 | 3636 | 0/0 | 214307360 | 184123392 | 5 |
| base | self_composite | 256 | 64 | insert | 1 | 60.671 | 56.630 | 896 | 12288 | 768 | 3813 | 0/0 | 264735552 | 300417024 | 11 |
| base | self_composite | 256 | 64 | insert | 2 | 11.191 | 16.725 | 896 | 12288 | 768 | 4464 | 0/0 | 226885728 | 315031552 | 4 |
| base | self_composite | 256 | 64 | load | 0 | 11.484 | 13.558 | 640 | 10240 | 512 | 2584 | 0/0 | 249020600 | 185565184 | 4 |
| base | self_composite | 256 | 64 | load | 1 | 6.315 | 13.802 | 640 | 10240 | 512 | 3327 | 0/0 | 266353000 | 196886528 | 4 |
| base | self_composite | 256 | 64 | load | 2 | 8.079 | 37.115 | 640 | 10240 | 512 | 3320 | 0/0 | 193667344 | 301662208 | 5 |
| base | self_composite | 256 | 64 | local | 0 | 30.346 | 10.980 | 640 | 10240 | 640 | 3615 | 0/0 | 213241760 | 180191232 | 5 |
| base | self_composite | 256 | 64 | local | 1 | 46.406 | 13.645 | 640 | 10240 | 640 | 3606 | 0/0 | 231401544 | 179847168 | 7 |
| base | self_composite | 256 | 64 | local | 2 | 37.293 | 11.215 | 640 | 10240 | 640 | 5562 | 0/0 | 193535832 | 307920896 | 5 |
| base | self_composite | 256 | 256 | insert | 0 | 31.299 | 23.132 | 1280 | 18432 | 768 | 5401 | 0/0 | 234085568 | 319143936 | 5 |
| base | self_composite | 256 | 256 | insert | 1 | 20.629 | 8.062 | 1280 | 18432 | 768 | 3632 | 0/0 | 213771592 | 324485120 | 4 |
| base | self_composite | 256 | 256 | insert | 2 | 27.067 | 8.418 | 1280 | 18432 | 768 | 4362 | 0/0 | 245437368 | 235257856 | 5 |
| base | self_composite | 256 | 256 | load | 0 | 4.925 | 19.498 | 1024 | 16384 | 512 | 3335 | 0/0 | 198914896 | 317620224 | 4 |
| base | self_composite | 256 | 256 | load | 1 | 4.625 | 16.736 | 1024 | 16384 | 512 | 4303 | 0/0 | 216999464 | 306216960 | 4 |
| base | self_composite | 256 | 256 | load | 2 | 3.717 | 7.780 | 1024 | 16384 | 512 | 2496 | 0/0 | 235474448 | 205668352 | 3 |
| base | self_composite | 256 | 256 | local | 0 | 55.825 | 8.797 | 1024 | 16384 | 1024 | 7895 | 0/0 | 270727760 | 300220416 | 8 |
| base | self_composite | 256 | 256 | local | 1 | 40.449 | 16.506 | 1024 | 16384 | 1024 | 7888 | 0/0 | 225239024 | 329940992 | 7 |
| base | self_composite | 256 | 256 | local | 2 | 74.755 | 21.846 | 1024 | 16384 | 1024 | 7889 | 0/0 | 250812912 | 338116608 | 7 |
| base | ordinary | 4096 | 16 | insert | 0 | 4.792 | 5.530 | 4096 | 32768 | 4096 | 16417 | 0/0 | 238087936 | 361742336 | 3 |
| base | ordinary | 4096 | 16 | insert | 1 | 7.332 | 8.823 | 4096 | 32768 | 4096 | 16417 | 0/0 | 250522576 | 362201088 | 3 |
| base | ordinary | 4096 | 16 | insert | 2 | 4.484 | 5.120 | 4096 | 32768 | 4096 | 16417 | 0/0 | 261212520 | 362561536 | 3 |
| base | ordinary | 4096 | 16 | load | 0 | 10.029 | 12.824 | 4096 | 32768 | 4096 | 16417 | 0/0 | 272267304 | 363036672 | 4 |
| base | ordinary | 4096 | 16 | load | 1 | 5.714 | 6.236 | 4096 | 32768 | 4096 | 16417 | 0/0 | 207990784 | 363429888 | 3 |
| base | ordinary | 4096 | 16 | load | 2 | 7.241 | 6.066 | 4096 | 32768 | 4096 | 16417 | 0/0 | 218835800 | 363560960 | 3 |
| base | ordinary | 4096 | 16 | local | 0 | 11.401 | 4.268 | 4096 | 32768 | 4096 | 16417 | 0/0 | 230621968 | 363888640 | 3 |
| base | ordinary | 4096 | 16 | local | 1 | 25.817 | 14.313 | 4096 | 32768 | 4096 | 16417 | 0/0 | 241897184 | 364134400 | 6 |
| base | ordinary | 4096 | 16 | local | 2 | 15.404 | 6.612 | 4096 | 32768 | 4096 | 16417 | 0/0 | 253086840 | 364314624 | 4 |
| base | ordinary | 4096 | 64 | insert | 0 | 29.298 | 6.311 | 4096 | 32768 | 4096 | 16417 | 0/0 | 264288680 | 364675072 | 4 |
| base | ordinary | 4096 | 64 | insert | 1 | 5.881 | 14.059 | 4096 | 32768 | 4096 | 16417 | 0/0 | 206150080 | 364822528 | 4 |
| base | ordinary | 4096 | 64 | insert | 2 | 10.795 | 5.706 | 4096 | 32768 | 4096 | 16417 | 0/0 | 217159112 | 365002752 | 3 |
| base | ordinary | 4096 | 64 | load | 0 | 6.823 | 13.896 | 4096 | 32768 | 4096 | 16417 | 0/0 | 228705048 | 365133824 | 4 |
| base | ordinary | 4096 | 64 | load | 1 | 5.382 | 6.704 | 4096 | 32768 | 4096 | 16417 | 0/0 | 239675952 | 365215744 | 3 |
| base | ordinary | 4096 | 64 | load | 2 | 5.694 | 5.611 | 4096 | 32768 | 4096 | 16417 | 0/0 | 250851160 | 365330432 | 3 |
| base | ordinary | 4096 | 64 | local | 0 | 16.565 | 5.128 | 4096 | 32768 | 4096 | 16417 | 0/0 | 262085016 | 365576192 | 4 |
| base | ordinary | 4096 | 64 | local | 1 | 20.181 | 5.123 | 4096 | 32768 | 4096 | 16417 | 0/0 | 272916432 | 365297664 | 4 |
| base | ordinary | 4096 | 64 | local | 2 | 13.050 | 4.320 | 4096 | 32768 | 4096 | 16417 | 0/0 | 214197072 | 365215744 | 3 |
| base | ordinary | 4096 | 256 | insert | 0 | 16.138 | 6.330 | 4096 | 32768 | 4096 | 16417 | 0/0 | 229419304 | 365281280 | 4 |
| base | ordinary | 4096 | 256 | insert | 1 | 13.107 | 5.428 | 4096 | 32768 | 4096 | 16417 | 0/0 | 244919824 | 360054784 | 3 |
| base | ordinary | 4096 | 256 | insert | 2 | 13.967 | 4.638 | 4096 | 32768 | 4096 | 16417 | 0/0 | 259859712 | 360169472 | 3 |
| base | ordinary | 4096 | 256 | load | 0 | 8.633 | 8.747 | 4096 | 32768 | 4096 | 16417 | 0/0 | 271030168 | 360464384 | 3 |
| base | ordinary | 4096 | 256 | load | 1 | 5.591 | 5.368 | 4096 | 32768 | 4096 | 16417 | 0/0 | 209769552 | 361267200 | 3 |
| base | ordinary | 4096 | 256 | load | 2 | 6.222 | 5.546 | 4096 | 32768 | 4096 | 16417 | 0/0 | 220882296 | 361676800 | 3 |
| base | ordinary | 4096 | 256 | local | 0 | 15.051 | 4.920 | 4096 | 32768 | 4096 | 16417 | 0/0 | 233537560 | 362004480 | 4 |
| base | ordinary | 4096 | 256 | local | 1 | 17.778 | 5.034 | 4096 | 32768 | 4096 | 16417 | 0/0 | 244841160 | 361889792 | 4 |
| base | ordinary | 4096 | 256 | local | 2 | 14.189 | 11.866 | 4096 | 32768 | 4096 | 16417 | 0/0 | 256313824 | 361578496 | 4 |
| base | auto_pk | 4096 | 16 | insert | 0 | 4.039 | 5.078 | 4096 | 32768 | 4096 | 16417 | 0/0 | 267831568 | 361431040 | 2 |
| base | auto_pk | 4096 | 16 | insert | 1 | 11.044 | 4.423 | 4096 | 32768 | 4096 | 16417 | 0/0 | 280120752 | 361218048 | 3 |
| base | auto_pk | 4096 | 16 | insert | 2 | 12.182 | 5.898 | 4096 | 32768 | 4096 | 16417 | 0/0 | 240926648 | 362201088 | 3 |
| base | auto_pk | 4096 | 16 | load | 0 | 5.578 | 5.579 | 4096 | 32768 | 4096 | 16417 | 0/0 | 218632256 | 362201088 | 3 |
| base | auto_pk | 4096 | 16 | load | 1 | 4.866 | 5.922 | 4096 | 32768 | 4096 | 16417 | 0/0 | 230456800 | 362364928 | 3 |
| base | auto_pk | 4096 | 16 | load | 2 | 4.671 | 6.192 | 4096 | 32768 | 4096 | 16417 | 0/0 | 242349336 | 362446848 | 3 |
| base | auto_pk | 4096 | 16 | local | 0 | 8.546 | 9.618 | 4096 | 32768 | 4096 | 16417 | 0/0 | 254573144 | 362643456 | 3 |
| base | auto_pk | 4096 | 16 | local | 1 | 13.247 | 8.077 | 4096 | 32768 | 4096 | 16417 | 0/0 | 267538448 | 362856448 | 4 |
| base | auto_pk | 4096 | 16 | local | 2 | 12.264 | 3.813 | 4096 | 32768 | 4096 | 16417 | 0/0 | 281533392 | 362610688 | 3 |
| base | auto_pk | 4096 | 64 | insert | 0 | 14.068 | 4.849 | 4096 | 32768 | 4096 | 16417 | 0/0 | 293735848 | 365445120 | 3 |
| base | auto_pk | 4096 | 64 | insert | 1 | 19.842 | 9.194 | 4096 | 32768 | 4096 | 16417 | 0/0 | 218116168 | 366182400 | 4 |
| base | auto_pk | 4096 | 64 | insert | 2 | 7.946 | 5.788 | 4096 | 32768 | 4096 | 16417 | 0/0 | 241326288 | 368754688 | 3 |
| base | auto_pk | 4096 | 64 | load | 0 | 6.370 | 5.602 | 4096 | 32768 | 4096 | 16417 | 0/0 | 253236152 | 368492544 | 3 |
| base | auto_pk | 4096 | 64 | load | 1 | 6.555 | 5.205 | 4096 | 32768 | 4096 | 16417 | 0/0 | 265230992 | 367788032 | 3 |
| base | auto_pk | 4096 | 64 | load | 2 | 5.231 | 5.633 | 4096 | 32768 | 4096 | 16417 | 0/0 | 283974664 | 368148480 | 3 |
| base | auto_pk | 4096 | 64 | local | 0 | 16.421 | 30.250 | 4096 | 32768 | 4096 | 16417 | 0/0 | 256051224 | 411992064 | 5 |
| base | auto_pk | 4096 | 64 | local | 1 | 14.518 | 5.137 | 4096 | 32768 | 4096 | 16417 | 0/0 | 271684944 | 409960448 | 4 |
| base | auto_pk | 4096 | 64 | local | 2 | 10.877 | 4.984 | 4096 | 32768 | 4096 | 16417 | 0/0 | 284617824 | 409747456 | 3 |
| base | auto_pk | 4096 | 256 | insert | 0 | 29.142 | 10.356 | 4096 | 32768 | 4096 | 16417 | 0/0 | 300959304 | 409698304 | 6 |
| base | auto_pk | 4096 | 256 | insert | 1 | 11.772 | 8.691 | 4096 | 32768 | 4096 | 16417 | 0/0 | 219644312 | 409698304 | 4 |
| base | auto_pk | 4096 | 256 | insert | 2 | 13.870 | 4.913 | 4096 | 32768 | 4096 | 16417 | 0/0 | 235313736 | 409747456 | 3 |
| base | auto_pk | 4096 | 256 | load | 0 | 5.396 | 10.608 | 4096 | 32768 | 4096 | 16417 | 0/0 | 247430736 | 408190976 | 3 |
| base | auto_pk | 4096 | 256 | load | 1 | 22.245 | 12.259 | 4096 | 32768 | 4096 | 16417 | 0/0 | 260476344 | 397623296 | 5 |
| base | auto_pk | 4096 | 256 | load | 2 | 23.722 | 18.583 | 4096 | 32768 | 4096 | 16417 | 0/0 | 274317680 | 400932864 | 7 |
| base | auto_pk | 4096 | 256 | local | 0 | 61.466 | 18.233 | 4096 | 32768 | 4096 | 16417 | 0/0 | 218192296 | 391823360 | 8 |
| base | auto_pk | 4096 | 256 | local | 1 | 35.782 | 14.913 | 4096 | 32768 | 4096 | 16417 | 0/0 | 233105752 | 375521280 | 5 |
| base | auto_pk | 4096 | 256 | local | 2 | 49.015 | 19.698 | 4096 | 32768 | 4096 | 16417 | 0/0 | 248325536 | 372785152 | 7 |
| base | auto_nonpk | 4096 | 16 | insert | 0 | 17.418 | 13.240 | 4096 | 32768 | 4096 | 16417 | 0/0 | 260940568 | 368001024 | 5 |
| base | auto_nonpk | 4096 | 16 | insert | 1 | 13.167 | 18.722 | 4096 | 32768 | 4096 | 16417 | 0/0 | 272792872 | 367853568 | 4 |
| base | auto_nonpk | 4096 | 16 | insert | 2 | 8.197 | 5.143 | 4096 | 32768 | 4096 | 16417 | 0/0 | 207633696 | 368214016 | 3 |
| base | auto_nonpk | 4096 | 16 | load | 0 | 5.286 | 8.906 | 4096 | 32768 | 4096 | 16417 | 0/0 | 219580296 | 368197632 | 3 |
| base | auto_nonpk | 4096 | 16 | load | 1 | 7.420 | 6.680 | 4096 | 32768 | 4096 | 16417 | 0/0 | 231657144 | 368328704 | 3 |
| base | auto_nonpk | 4096 | 16 | load | 2 | 6.001 | 8.535 | 4096 | 32768 | 4096 | 16417 | 0/0 | 243617352 | 368181248 | 3 |
| base | auto_nonpk | 4096 | 16 | local | 0 | 15.020 | 6.120 | 4096 | 32768 | 4096 | 16417 | 0/0 | 256455344 | 367460352 | 4 |
| base | auto_nonpk | 4096 | 16 | local | 1 | 19.855 | 7.235 | 4096 | 32768 | 4096 | 16417 | 0/0 | 203028504 | 364232704 | 4 |
| base | auto_nonpk | 4096 | 16 | local | 2 | 21.034 | 7.156 | 4096 | 32768 | 4096 | 16417 | 0/0 | 215267464 | 364265472 | 4 |
| base | auto_nonpk | 4096 | 64 | insert | 0 | 7.625 | 8.875 | 4096 | 32768 | 4096 | 16417 | 0/0 | 227564864 | 362987520 | 3 |
| base | auto_nonpk | 4096 | 64 | insert | 1 | 6.811 | 5.399 | 4096 | 32768 | 4096 | 16417 | 0/0 | 240198032 | 362463232 | 3 |
| base | auto_nonpk | 4096 | 64 | insert | 2 | 7.571 | 4.269 | 4096 | 32768 | 4096 | 16417 | 0/0 | 252498720 | 362266624 | 3 |
| base | auto_nonpk | 4096 | 64 | load | 0 | 7.467 | 6.799 | 4096 | 32768 | 4096 | 16417 | 0/0 | 263785656 | 361873408 | 3 |
| base | auto_nonpk | 4096 | 64 | load | 1 | 7.284 | 13.208 | 4096 | 32768 | 4096 | 16417 | 0/0 | 212539592 | 361611264 | 4 |
| base | auto_nonpk | 4096 | 64 | load | 2 | 5.861 | 6.779 | 4096 | 32768 | 4096 | 16417 | 0/0 | 224754872 | 360972288 | 3 |
| base | auto_nonpk | 4096 | 64 | local | 0 | 21.592 | 13.453 | 4096 | 32768 | 4096 | 16417 | 0/0 | 237115024 | 361152512 | 5 |
| base | auto_nonpk | 4096 | 64 | local | 1 | 21.683 | 7.906 | 4096 | 32768 | 4096 | 16417 | 0/0 | 250084248 | 360497152 | 5 |
| base | auto_nonpk | 4096 | 64 | local | 2 | 19.772 | 3.867 | 4096 | 32768 | 4096 | 16417 | 0/0 | 262910712 | 359464960 | 4 |
| base | auto_nonpk | 4096 | 256 | insert | 0 | 16.043 | 9.694 | 4096 | 32768 | 4096 | 16417 | 0/0 | 279932616 | 365608960 | 4 |
| base | auto_nonpk | 4096 | 256 | insert | 1 | 14.569 | 6.381 | 4096 | 32768 | 4096 | 16417 | 0/0 | 213814704 | 369360896 | 4 |
| base | auto_nonpk | 4096 | 256 | insert | 2 | 14.501 | 4.204 | 4096 | 32768 | 4096 | 16417 | 0/0 | 229573160 | 369377280 | 3 |
| base | auto_nonpk | 4096 | 256 | load | 0 | 7.551 | 16.415 | 4096 | 32768 | 4096 | 16417 | 0/0 | 241834384 | 369213440 | 4 |
| base | auto_nonpk | 4096 | 256 | load | 1 | 5.982 | 7.919 | 4096 | 32768 | 4096 | 16417 | 0/0 | 254789448 | 366067712 | 3 |
| base | auto_nonpk | 4096 | 256 | load | 2 | 6.742 | 7.296 | 4096 | 32768 | 4096 | 16417 | 0/0 | 267284328 | 365543424 | 3 |
| base | auto_nonpk | 4096 | 256 | local | 0 | 20.464 | 5.320 | 4096 | 32768 | 4096 | 16417 | 0/0 | 280057040 | 368427008 | 4 |
| base | auto_nonpk | 4096 | 256 | local | 1 | 20.652 | 8.032 | 4096 | 32768 | 4096 | 16417 | 0/0 | 293300544 | 375259136 | 4 |
| base | auto_nonpk | 4096 | 256 | local | 2 | 29.860 | 18.830 | 4096 | 32768 | 4096 | 16417 | 0/0 | 219661496 | 375668736 | 6 |
| base | auto_composite | 4096 | 16 | insert | 0 | 7.514 | 8.253 | 4096 | 65536 | 4096 | 16600 | 0/0 | 231883736 | 374964224 | 3 |
| base | auto_composite | 4096 | 16 | insert | 1 | 7.152 | 32.296 | 4096 | 65536 | 4096 | 16600 | 0/0 | 244580320 | 374849536 | 5 |
| base | auto_composite | 4096 | 16 | insert | 2 | 7.174 | 5.422 | 4096 | 65536 | 4096 | 16600 | 0/0 | 256915792 | 374538240 | 3 |
| base | auto_composite | 4096 | 16 | load | 0 | 6.208 | 9.143 | 4096 | 65536 | 4096 | 16600 | 0/0 | 269301360 | 374620160 | 3 |
| base | auto_composite | 4096 | 16 | load | 1 | 6.531 | 7.432 | 4096 | 65536 | 4096 | 16600 | 0/0 | 281815784 | 374603776 | 3 |
| base | auto_composite | 4096 | 16 | load | 2 | 4.372 | 8.272 | 4096 | 65536 | 4096 | 16600 | 0/0 | 293454408 | 376209408 | 3 |
| base | auto_composite | 4096 | 16 | local | 0 | 14.761 | 4.992 | 4096 | 65536 | 4096 | 16600 | 0/0 | 219978728 | 375504896 | 4 |
| base | auto_composite | 4096 | 16 | local | 1 | 19.055 | 8.045 | 4096 | 65536 | 4096 | 16600 | 0/0 | 233024280 | 373571584 | 4 |
| base | auto_composite | 4096 | 16 | local | 2 | 23.337 | 7.857 | 4096 | 65536 | 4096 | 16600 | 0/0 | 246922816 | 373981184 | 5 |
| base | auto_composite | 4096 | 64 | insert | 0 | 9.396 | 5.159 | 4096 | 65536 | 4096 | 16600 | 0/0 | 259759368 | 373555200 | 3 |
| base | auto_composite | 4096 | 64 | insert | 1 | 7.562 | 5.722 | 4096 | 65536 | 4096 | 16600 | 0/0 | 278766896 | 372359168 | 3 |
| base | auto_composite | 4096 | 64 | insert | 2 | 9.168 | 8.229 | 4096 | 65536 | 4096 | 16600 | 0/0 | 291015872 | 372981760 | 3 |
| base | auto_composite | 4096 | 64 | load | 0 | 11.873 | 4.767 | 4096 | 65536 | 4096 | 16600 | 0/0 | 218085072 | 372621312 | 3 |
| base | auto_composite | 4096 | 64 | load | 1 | 10.470 | 12.535 | 4096 | 65536 | 4096 | 16600 | 0/0 | 233648720 | 375111680 | 4 |
| base | auto_composite | 4096 | 64 | load | 2 | 6.348 | 6.229 | 4096 | 65536 | 4096 | 16600 | 0/0 | 246204304 | 374554624 | 3 |
| base | auto_composite | 4096 | 64 | local | 0 | 34.913 | 18.126 | 4096 | 65536 | 4096 | 16600 | 0/0 | 259086800 | 374505472 | 7 |
| base | auto_composite | 4096 | 64 | local | 1 | 29.383 | 11.884 | 4096 | 65536 | 4096 | 16600 | 0/0 | 271974784 | 374456320 | 6 |
| base | auto_composite | 4096 | 64 | local | 2 | 34.848 | 24.748 | 4096 | 65536 | 4096 | 16600 | 0/0 | 285280968 | 374554624 | 6 |
| base | auto_composite | 4096 | 256 | insert | 0 | 23.839 | 63.531 | 4096 | 65536 | 4096 | 16600 | 0/0 | 221603696 | 375521280 | 8 |
| base | auto_composite | 4096 | 256 | insert | 1 | 76.001 | 19.849 | 4096 | 65536 | 4096 | 16600 | 0/0 | 241052128 | 375947264 | 9 |
| base | auto_composite | 4096 | 256 | insert | 2 | 66.376 | 21.176 | 4096 | 65536 | 4096 | 16600 | 0/0 | 260744688 | 375504896 | 8 |
| base | auto_composite | 4096 | 256 | load | 0 | 7.145 | 28.175 | 4096 | 65536 | 4096 | 16600 | 0/0 | 275787968 | 374751232 | 5 |
| base | auto_composite | 4096 | 256 | load | 1 | 30.907 | 17.948 | 4096 | 65536 | 4096 | 16600 | 0/0 | 211546128 | 377389056 | 5 |
| base | auto_composite | 4096 | 256 | load | 2 | 46.779 | 25.410 | 4096 | 65536 | 4096 | 16600 | 0/0 | 226887728 | 376520704 | 7 |
| base | auto_composite | 4096 | 256 | local | 0 | 50.851 | 60.936 | 4096 | 65536 | 4096 | 16600 | 0/0 | 243602232 | 376930304 | 9 |
| base | auto_composite | 4096 | 256 | local | 1 | 53.365 | 53.366 | 4096 | 65536 | 4096 | 16600 | 0/0 | 259912448 | 376750080 | 9 |
| base | auto_composite | 4096 | 256 | local | 2 | 50.922 | 41.308 | 4096 | 65536 | 4096 | 16600 | 0/0 | 275245488 | 375013376 | 9 |
| base | self_composite | 4096 | 16 | insert | 0 | 35.240 | 6.608 | 12320 | 164352 | 12288 | 50535 | 0/0 | 288275984 | 382304256 | 6 |
| base | self_composite | 4096 | 16 | insert | 1 | 26.022 | 15.831 | 12320 | 164352 | 12288 | 66030 | 0/0 | 237953512 | 388317184 | 6 |
| base | self_composite | 4096 | 16 | insert | 2 | 23.897 | 7.294 | 12320 | 164352 | 12288 | 49719 | 0/0 | 292113112 | 397967360 | 5 |
| base | self_composite | 4096 | 16 | load | 0 | 12.786 | 8.889 | 8224 | 131584 | 8192 | 34249 | 0/0 | 287155768 | 386334720 | 4 |
| base | self_composite | 4096 | 16 | load | 1 | 3.957 | 4.854 | 8224 | 131584 | 8192 | 49775 | 0/0 | 279733296 | 369754112 | 2 |
| base | self_composite | 4096 | 16 | load | 2 | 4.687 | 7.019 | 8224 | 131584 | 8192 | 33389 | 0/0 | 269760240 | 373735424 | 3 |
| base | self_composite | 4096 | 16 | local | 0 | 34.588 | 8.795 | 8224 | 131584 | 8224 | 34771 | 0/0 | 268595128 | 390610944 | 5 |
| base | self_composite | 4096 | 16 | local | 1 | 24.040 | 5.893 | 8224 | 131584 | 8224 | 49981 | 0/0 | 314542136 | 429146112 | 5 |
| base | self_composite | 4096 | 16 | local | 2 | 27.423 | 6.401 | 8224 | 131584 | 8224 | 34226 | 0/0 | 299758272 | 388186112 | 5 |
| base | self_composite | 4096 | 64 | insert | 0 | 173.757 | 8.456 | 12416 | 165888 | 12288 | 49892 | 0/0 | 320439672 | 396034048 | 16 |
| base | self_composite | 4096 | 64 | insert | 1 | 63.233 | 7.992 | 12416 | 165888 | 12288 | 82177 | 0/0 | 273940728 | 407519232 | 9 |
| base | self_composite | 4096 | 64 | insert | 2 | 181.778 | 36.350 | 12416 | 165888 | 12288 | 66049 | 0/0 | 329555848 | 413368320 | 16 |
| base | self_composite | 4096 | 64 | load | 0 | 28.787 | 7.978 | 8320 | 133120 | 8192 | 46704 | 0/0 | 234850680 | 366608384 | 5 |
| base | self_composite | 4096 | 64 | load | 1 | 10.946 | 9.005 | 8320 | 133120 | 8192 | 33528 | 0/0 | 226539704 | 376471552 | 4 |
| base | self_composite | 4096 | 64 | load | 2 | 8.069 | 16.104 | 8320 | 133120 | 8192 | 49636 | 0/0 | 238227104 | 387235840 | 4 |
| base | self_composite | 4096 | 64 | local | 0 | 73.582 | 9.128 | 8320 | 133120 | 8320 | 35456 | 0/0 | 244854592 | 370163712 | 7 |
| base | self_composite | 4096 | 64 | local | 1 | 39.382 | 14.025 | 8320 | 133120 | 8320 | 34685 | 0/0 | 233228504 | 382517248 | 6 |
| base | self_composite | 4096 | 64 | local | 2 | 27.757 | 4.315 | 8320 | 133120 | 8320 | 66984 | 0/0 | 223358432 | 354467840 | 4 |
| base | self_composite | 4096 | 256 | insert | 0 | 310.723 | 8.532 | 12800 | 172032 | 12288 | 66190 | 0/0 | 345772304 | 466223104 | 32 |
| base | self_composite | 4096 | 256 | insert | 1 | 290.337 | 11.132 | 12800 | 172032 | 12288 | 56779 | 0/0 | 355223016 | 484737024 | 30 |
| base | self_composite | 4096 | 256 | insert | 2 | 376.263 | 9.546 | 12800 | 172032 | 12288 | 66186 | 0/0 | 344405552 | 468615168 | 35 |
| base | self_composite | 4096 | 256 | load | 0 | 10.953 | 22.381 | 8704 | 139264 | 8192 | 49542 | 0/0 | 308434472 | 467795968 | 5 |
| base | self_composite | 4096 | 256 | load | 1 | 4.319 | 6.860 | 8704 | 139264 | 8192 | 49795 | 0/0 | 219270872 | 474284032 | 3 |
| base | self_composite | 4096 | 256 | load | 2 | 4.956 | 6.161 | 8704 | 139264 | 8192 | 34111 | 0/0 | 229518680 | 443875328 | 3 |
| base | self_composite | 4096 | 256 | local | 0 | 23.324 | 8.067 | 8704 | 139264 | 8704 | 70058 | 0/0 | 238009656 | 400162816 | 5 |
| base | self_composite | 4096 | 256 | local | 1 | 49.551 | 10.051 | 8704 | 139264 | 8704 | 53902 | 0/0 | 249018624 | 400867328 | 7 |
| base | self_composite | 4096 | 256 | local | 2 | 77.675 | 7.466 | 8704 | 139264 | 8704 | 60564 | 0/0 | 328687256 | 306806784 | 9 |
| head | ordinary | 256 | 16 | insert | 0 | 8.015 | 8.228 | 256 | 2048 | 256 | 1072 | 0/0 | 177381288 | 291913728 | 3 |
| head | ordinary | 256 | 16 | insert | 1 | 8.650 | 5.471 | 256 | 2048 | 256 | 1072 | 0/0 | 187794112 | 292519936 | 3 |
| head | ordinary | 256 | 16 | insert | 2 | 6.273 | 5.566 | 256 | 2048 | 256 | 1072 | 0/0 | 198317208 | 293634048 | 3 |
| head | ordinary | 256 | 16 | load | 0 | 5.323 | 5.534 | 256 | 2048 | 256 | 1072 | 0/0 | 162396368 | 296255488 | 3 |
| head | ordinary | 256 | 16 | load | 1 | 9.530 | 5.818 | 256 | 2048 | 256 | 1072 | 0/0 | 172576160 | 296517632 | 3 |
| head | ordinary | 256 | 16 | load | 2 | 7.894 | 4.856 | 256 | 2048 | 256 | 1072 | 0/0 | 182946856 | 296615936 | 3 |
| head | ordinary | 256 | 16 | local | 0 | 24.534 | 8.728 | 256 | 2048 | 256 | 1072 | 0/0 | 194426232 | 297271296 | 5 |
| head | ordinary | 256 | 16 | local | 1 | 35.735 | 8.202 | 256 | 2048 | 256 | 1072 | 0/0 | 205333568 | 297779200 | 5 |
| head | ordinary | 256 | 16 | local | 2 | 22.162 | 6.048 | 256 | 2048 | 256 | 1072 | 0/0 | 216280608 | 302399488 | 3 |
| head | ordinary | 256 | 64 | insert | 0 | 5.444 | 5.769 | 256 | 2048 | 256 | 1072 | 0/0 | 185218024 | 311951360 | 3 |
| head | ordinary | 256 | 64 | insert | 1 | 4.521 | 4.791 | 256 | 2048 | 256 | 1072 | 0/0 | 165899528 | 312475648 | 2 |
| head | ordinary | 256 | 64 | insert | 2 | 4.921 | 5.156 | 256 | 2048 | 256 | 1072 | 0/0 | 176129360 | 312541184 | 3 |
| head | ordinary | 256 | 64 | load | 0 | 6.892 | 6.644 | 256 | 2048 | 256 | 1072 | 0/0 | 186687200 | 312721408 | 3 |
| head | ordinary | 256 | 64 | load | 1 | 6.110 | 6.177 | 256 | 2048 | 256 | 1072 | 0/0 | 198564976 | 312868864 | 3 |
| head | ordinary | 256 | 64 | load | 2 | 7.922 | 6.477 | 256 | 2048 | 256 | 1072 | 0/0 | 209342896 | 313098240 | 3 |
| head | ordinary | 256 | 64 | local | 0 | 27.880 | 12.966 | 256 | 2048 | 256 | 1072 | 0/0 | 220448880 | 313491456 | 6 |
| head | ordinary | 256 | 64 | local | 1 | 29.948 | 9.505 | 256 | 2048 | 256 | 1072 | 0/0 | 165618104 | 313737216 | 5 |
| head | ordinary | 256 | 64 | local | 2 | 17.201 | 2.611 | 256 | 2048 | 256 | 1072 | 0/0 | 177345112 | 314015744 | 4 |
| head | ordinary | 256 | 256 | insert | 0 | 10.179 | 6.362 | 256 | 2048 | 256 | 1072 | 0/0 | 192012264 | 314064896 | 3 |
| head | ordinary | 256 | 256 | insert | 1 | 7.751 | 7.322 | 256 | 2048 | 256 | 1072 | 0/0 | 206141968 | 314228736 | 3 |
| head | ordinary | 256 | 256 | insert | 2 | 30.226 | 11.327 | 256 | 2048 | 256 | 1072 | 0/0 | 220902456 | 314441728 | 6 |
| head | ordinary | 256 | 256 | load | 0 | 10.170 | 7.164 | 256 | 2048 | 256 | 1072 | 0/0 | 170226024 | 314884096 | 3 |
| head | ordinary | 256 | 256 | load | 1 | 11.429 | 10.064 | 256 | 2048 | 256 | 1072 | 0/0 | 184101672 | 315260928 | 4 |
| head | ordinary | 256 | 256 | load | 2 | 41.184 | 20.351 | 256 | 2048 | 256 | 1072 | 0/0 | 198138232 | 315359232 | 8 |
| head | ordinary | 256 | 256 | local | 0 | 31.277 | 6.707 | 256 | 2048 | 256 | 1072 | 0/0 | 214320000 | 315670528 | 4 |
| head | ordinary | 256 | 256 | local | 1 | 39.551 | 12.168 | 256 | 2048 | 256 | 1072 | 0/0 | 225017656 | 315981824 | 7 |
| head | ordinary | 256 | 256 | local | 2 | 34.816 | 4.190 | 256 | 2048 | 256 | 1072 | 0/0 | 176403040 | 316211200 | 4 |
| head | auto_pk | 256 | 16 | insert | 0 | 7.958 | 4.483 | 4096 | 32768 | 4096 | 17152 | 0/0 | 187516024 | 316309504 | 3 |
| head | auto_pk | 256 | 16 | insert | 1 | 9.798 | 4.789 | 4096 | 32768 | 4096 | 17152 | 0/0 | 201407368 | 316801024 | 3 |
| head | auto_pk | 256 | 16 | insert | 2 | 11.473 | 5.476 | 4096 | 32768 | 4096 | 17152 | 0/0 | 212591000 | 316882944 | 3 |
| head | auto_pk | 256 | 16 | load | 0 | 17.498 | 15.468 | 4096 | 32768 | 4096 | 17152 | 0/0 | 224083736 | 317014016 | 5 |
| head | auto_pk | 256 | 16 | load | 1 | 8.639 | 4.525 | 4096 | 32768 | 4096 | 17152 | 0/0 | 169541704 | 317112320 | 3 |
| head | auto_pk | 256 | 16 | load | 2 | 9.499 | 5.524 | 4096 | 32768 | 4096 | 17152 | 0/0 | 181046688 | 317292544 | 3 |
| head | auto_pk | 256 | 16 | local | 0 | 25.537 | 8.264 | 4096 | 32768 | 4096 | 17152 | 0/0 | 192834840 | 317505536 | 5 |
| head | auto_pk | 256 | 16 | local | 1 | 18.012 | 3.837 | 4096 | 32768 | 4096 | 17152 | 0/0 | 204842592 | 317603840 | 4 |
| head | auto_pk | 256 | 16 | local | 2 | 20.335 | 9.025 | 4096 | 32768 | 4096 | 17152 | 0/0 | 216582472 | 317669376 | 5 |
| head | auto_pk | 256 | 64 | insert | 0 | 37.084 | 10.355 | 16384 | 131072 | 16384 | 68608 | 0/0 | 229004048 | 320126976 | 6 |
| head | auto_pk | 256 | 64 | insert | 1 | 9.432 | 6.145 | 16384 | 131072 | 16384 | 68608 | 0/0 | 170488416 | 320618496 | 3 |
| head | auto_pk | 256 | 64 | insert | 2 | 12.606 | 6.265 | 16384 | 131072 | 16384 | 68608 | 0/0 | 183591520 | 320716800 | 3 |
| head | auto_pk | 256 | 64 | load | 0 | 10.888 | 5.879 | 16384 | 131072 | 16384 | 68608 | 0/0 | 198605568 | 322928640 | 3 |
| head | auto_pk | 256 | 64 | load | 1 | 6.708 | 6.136 | 16384 | 131072 | 16384 | 68608 | 0/0 | 211245968 | 322961408 | 3 |
| head | auto_pk | 256 | 64 | load | 2 | 13.743 | 5.760 | 16384 | 131072 | 16384 | 68608 | 0/0 | 223837832 | 323010560 | 4 |
| head | auto_pk | 256 | 64 | local | 0 | 31.166 | 9.112 | 16384 | 131072 | 16384 | 68608 | 0/0 | 167863560 | 327548928 | 6 |
| head | auto_pk | 256 | 64 | local | 1 | 17.578 | 13.989 | 16384 | 131072 | 16384 | 68608 | 0/0 | 186650592 | 327909376 | 5 |
| head | auto_pk | 256 | 64 | local | 2 | 15.081 | 5.139 | 16384 | 131072 | 16384 | 68608 | 0/0 | 199530000 | 327942144 | 4 |
| head | auto_pk | 256 | 256 | insert | 0 | 17.465 | 5.885 | 65536 | 524288 | 65536 | 274432 | 0/0 | 216916616 | 328040448 | 4 |
| head | auto_pk | 256 | 256 | insert | 1 | 66.388 | 6.849 | 65536 | 524288 | 65536 | 274432 | 0/0 | 233986600 | 328171520 | 8 |
| head | auto_pk | 256 | 256 | insert | 2 | 31.781 | 9.791 | 65536 | 524288 | 65536 | 274432 | 0/0 | 182503336 | 328302592 | 6 |
| head | auto_pk | 256 | 256 | load | 0 | 54.451 | 6.428 | 65536 | 524288 | 65536 | 274432 | 0/0 | 199226480 | 329089024 | 8 |
| head | auto_pk | 256 | 256 | load | 1 | 21.600 | 5.373 | 65536 | 524288 | 65536 | 274432 | 0/0 | 220151016 | 333447168 | 4 |
| head | auto_pk | 256 | 256 | load | 2 | 33.091 | 11.621 | 65536 | 524288 | 65536 | 274432 | 0/0 | 238223016 | 333627392 | 6 |
| head | auto_pk | 256 | 256 | local | 0 | 25.964 | 4.202 | 65536 | 524288 | 65536 | 274432 | 0/0 | 187349936 | 333807616 | 5 |
| head | auto_pk | 256 | 256 | local | 1 | 42.163 | 5.716 | 65536 | 524288 | 65536 | 274432 | 0/0 | 204079192 | 333889536 | 6 |
| head | auto_pk | 256 | 256 | local | 2 | 46.481 | 8.548 | 65536 | 524288 | 65536 | 274432 | 0/0 | 233492704 | 344293376 | 7 |
| head | auto_nonpk | 256 | 16 | insert | 0 | 7.408 | 6.666 | 4096 | 32768 | 4096 | 17152 | 0/0 | 243958648 | 344391680 | 3 |
| head | auto_nonpk | 256 | 16 | insert | 1 | 4.678 | 5.561 | 4096 | 32768 | 4096 | 17152 | 0/0 | 182635088 | 344440832 | 3 |
| head | auto_nonpk | 256 | 16 | insert | 2 | 4.908 | 5.890 | 4096 | 32768 | 4096 | 17152 | 0/0 | 194360552 | 344440832 | 3 |
| head | auto_nonpk | 256 | 16 | load | 0 | 6.652 | 11.674 | 4096 | 32768 | 4096 | 17152 | 0/0 | 206046760 | 344506368 | 3 |
| head | auto_nonpk | 256 | 16 | load | 1 | 22.153 | 8.796 | 4096 | 32768 | 4096 | 17152 | 0/0 | 217767176 | 344866816 | 5 |
| head | auto_nonpk | 256 | 16 | load | 2 | 41.009 | 12.324 | 4096 | 32768 | 4096 | 17152 | 0/0 | 232606216 | 345587712 | 8 |
| head | auto_nonpk | 256 | 16 | local | 0 | 65.766 | 14.289 | 4096 | 32768 | 4096 | 17152 | 0/0 | 176348344 | 346390528 | 9 |
| head | auto_nonpk | 256 | 16 | local | 1 | 52.329 | 29.308 | 4096 | 32768 | 4096 | 17152 | 0/0 | 193271632 | 346570752 | 7 |
| head | auto_nonpk | 256 | 16 | local | 2 | 41.467 | 5.962 | 4096 | 32768 | 4096 | 17152 | 0/0 | 207066056 | 347373568 | 6 |
| head | auto_nonpk | 256 | 64 | insert | 0 | 9.566 | 4.628 | 16384 | 131072 | 16384 | 68608 | 0/0 | 219598576 | 346800128 | 3 |
| head | auto_nonpk | 256 | 64 | insert | 1 | 12.073 | 4.640 | 16384 | 131072 | 16384 | 68608 | 0/0 | 232098840 | 346816512 | 3 |
| head | auto_nonpk | 256 | 64 | insert | 2 | 13.686 | 5.742 | 16384 | 131072 | 16384 | 68608 | 0/0 | 242641880 | 346652672 | 4 |
| head | auto_nonpk | 256 | 64 | load | 0 | 16.918 | 6.421 | 16384 | 131072 | 16384 | 68608 | 0/0 | 184724328 | 346718208 | 4 |
| head | auto_nonpk | 256 | 64 | load | 1 | 12.176 | 5.185 | 16384 | 131072 | 16384 | 68608 | 0/0 | 197355544 | 346800128 | 3 |
| head | auto_nonpk | 256 | 64 | load | 2 | 12.735 | 4.538 | 16384 | 131072 | 16384 | 68608 | 0/0 | 210163064 | 346849280 | 3 |
| head | auto_nonpk | 256 | 64 | local | 0 | 27.222 | 4.894 | 16384 | 131072 | 16384 | 68608 | 0/0 | 223172776 | 346898432 | 5 |
| head | auto_nonpk | 256 | 64 | local | 1 | 55.010 | 4.097 | 16384 | 131072 | 16384 | 68608 | 0/0 | 237569656 | 346963968 | 7 |
| head | auto_nonpk | 256 | 64 | local | 2 | 30.031 | 10.149 | 16384 | 131072 | 16384 | 68608 | 0/0 | 180906288 | 348176384 | 6 |
| head | auto_nonpk | 256 | 256 | insert | 0 | 262.690 | 15.748 | 65536 | 524288 | 65536 | 274432 | 0/0 | 206592536 | 348422144 | 27 |
| head | auto_nonpk | 256 | 256 | insert | 1 | 66.768 | 5.672 | 65536 | 524288 | 65536 | 274432 | 0/0 | 229857392 | 348504064 | 9 |
| head | auto_nonpk | 256 | 256 | insert | 2 | 38.532 | 5.256 | 65536 | 524288 | 65536 | 274432 | 0/0 | 247499512 | 350552064 | 5 |
| head | auto_nonpk | 256 | 256 | load | 0 | 14.516 | 5.615 | 65536 | 524288 | 65536 | 274432 | 0/0 | 201814720 | 359055360 | 4 |
| head | auto_nonpk | 256 | 256 | load | 1 | 24.521 | 4.696 | 65536 | 524288 | 65536 | 274432 | 0/0 | 219894312 | 358924288 | 4 |
| head | auto_nonpk | 256 | 256 | load | 2 | 21.647 | 9.143 | 65536 | 524288 | 65536 | 274432 | 0/0 | 236733096 | 358989824 | 5 |
| head | auto_nonpk | 256 | 256 | local | 0 | 33.100 | 9.404 | 65536 | 524288 | 65536 | 274432 | 0/0 | 247311968 | 359088128 | 6 |
| head | auto_nonpk | 256 | 256 | local | 1 | 28.498 | 5.060 | 65536 | 524288 | 65536 | 274432 | 0/0 | 199854960 | 359120896 | 5 |
| head | auto_nonpk | 256 | 256 | local | 2 | 27.122 | 5.907 | 65536 | 524288 | 65536 | 274432 | 0/0 | 217421256 | 359219200 | 5 |
| head | auto_composite | 256 | 16 | insert | 0 | 18.504 | 8.547 | 4096 | 65536 | 4096 | 18144 | 0/0 | 229473192 | 359268352 | 4 |
| head | auto_composite | 256 | 16 | insert | 1 | 6.735 | 7.524 | 4096 | 65536 | 4096 | 18144 | 0/0 | 242009664 | 360513536 | 3 |
| head | auto_composite | 256 | 16 | insert | 2 | 8.400 | 5.719 | 4096 | 65536 | 4096 | 18144 | 0/0 | 252784800 | 360628224 | 3 |
| head | auto_composite | 256 | 16 | load | 0 | 6.182 | 4.209 | 4096 | 65536 | 4096 | 18144 | 0/0 | 192411776 | 360677376 | 3 |
| head | auto_composite | 256 | 16 | load | 1 | 5.964 | 3.978 | 4096 | 65536 | 4096 | 18144 | 0/0 | 204803280 | 360710144 | 3 |
| head | auto_composite | 256 | 16 | load | 2 | 8.425 | 6.062 | 4096 | 65536 | 4096 | 18144 | 0/0 | 217081512 | 360759296 | 3 |
| head | auto_composite | 256 | 16 | local | 0 | 14.665 | 4.417 | 4096 | 65536 | 4096 | 18144 | 0/0 | 229648480 | 360792064 | 3 |
| head | auto_composite | 256 | 16 | local | 1 | 25.298 | 5.767 | 4096 | 65536 | 4096 | 18144 | 0/0 | 249853832 | 362790912 | 5 |
| head | auto_composite | 256 | 16 | local | 2 | 24.485 | 5.740 | 4096 | 65536 | 4096 | 18144 | 0/0 | 255127144 | 421068800 | 4 |
| head | auto_composite | 256 | 64 | insert | 0 | 22.137 | 7.049 | 16384 | 262144 | 16384 | 72576 | 0/0 | 310979904 | 463831040 | 4 |
| head | auto_composite | 256 | 64 | insert | 1 | 11.987 | 5.695 | 16384 | 262144 | 16384 | 72576 | 0/0 | 318250136 | 480051200 | 3 |
| head | auto_composite | 256 | 64 | insert | 2 | 22.518 | 20.295 | 16384 | 262144 | 16384 | 72576 | 0/0 | 271387328 | 512131072 | 6 |
| head | auto_composite | 256 | 64 | load | 0 | 9.306 | 5.020 | 16384 | 262144 | 16384 | 72576 | 0/0 | 288482600 | 496893952 | 3 |
| head | auto_composite | 256 | 64 | load | 1 | 8.589 | 4.522 | 16384 | 262144 | 16384 | 72576 | 0/0 | 219020344 | 497254400 | 3 |
| head | auto_composite | 256 | 64 | load | 2 | 11.110 | 6.730 | 16384 | 262144 | 16384 | 72576 | 0/0 | 209636464 | 497401856 | 3 |
| head | auto_composite | 256 | 64 | local | 0 | 27.938 | 5.183 | 16384 | 262144 | 16384 | 72576 | 0/0 | 224041280 | 497549312 | 5 |
| head | auto_composite | 256 | 64 | local | 1 | 20.634 | 10.762 | 16384 | 262144 | 16384 | 72576 | 0/0 | 238433272 | 497713152 | 5 |
| head | auto_composite | 256 | 64 | local | 2 | 19.161 | 4.280 | 16384 | 262144 | 16384 | 72576 | 0/0 | 252957032 | 497876992 | 4 |
| head | auto_composite | 256 | 256 | insert | 0 | 30.739 | 5.703 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 209967848 | 498106368 | 5 |
| head | auto_composite | 256 | 256 | insert | 1 | 29.028 | 6.268 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 231535560 | 498155520 | 5 |
| head | auto_composite | 256 | 256 | insert | 2 | 20.080 | 5.825 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 253689728 | 498237440 | 4 |
| head | auto_composite | 256 | 256 | load | 0 | 37.731 | 6.198 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 265331048 | 498417664 | 6 |
| head | auto_composite | 256 | 256 | load | 1 | 28.596 | 5.549 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 229451136 | 498548736 | 5 |
| head | auto_composite | 256 | 256 | load | 2 | 38.511 | 3.998 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 251992080 | 498728960 | 6 |
| head | auto_composite | 256 | 256 | local | 0 | 46.085 | 8.026 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 263139064 | 498941952 | 7 |
| head | auto_composite | 256 | 256 | local | 1 | 40.288 | 5.223 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 230749344 | 498991104 | 6 |
| head | auto_composite | 256 | 256 | local | 2 | 85.133 | 4.063 | 65536 | 1048576 | 65536 | 290304 | 0/0 | 252329496 | 499056640 | 10 |
| head | self_composite | 256 | 16 | insert | 0 | 26.870 | 6.911 | 4352 | 67584 | 4352 | 35424 | 0/0 | 199755416 | 500072448 | 5 |
| head | self_composite | 256 | 16 | insert | 1 | 9.998 | 5.993 | 4352 | 67584 | 4352 | 35424 | 0/0 | 226742928 | 500629504 | 3 |
| head | self_composite | 256 | 16 | insert | 2 | 5.540 | 4.885 | 4352 | 67584 | 4352 | 35424 | 0/0 | 246840528 | 500891648 | 3 |
| head | self_composite | 256 | 16 | load | 0 | 4.723 | 4.617 | 4352 | 67584 | 4352 | 35424 | 0/0 | 265947776 | 501202944 | 2 |
| head | self_composite | 256 | 16 | load | 1 | 4.441 | 4.147 | 4352 | 67584 | 4352 | 35424 | 0/0 | 209896888 | 501497856 | 2 |
| head | self_composite | 256 | 16 | load | 2 | 5.821 | 4.691 | 4352 | 67584 | 4352 | 35424 | 0/0 | 228084176 | 501694464 | 3 |
| head | self_composite | 256 | 16 | local | 0 | 21.196 | 4.968 | 4352 | 67584 | 4352 | 35424 | 0/0 | 253737872 | 502022144 | 4 |
| head | self_composite | 256 | 16 | local | 1 | 27.716 | 5.030 | 4352 | 67584 | 4352 | 35424 | 0/0 | 274883112 | 502366208 | 5 |
| head | self_composite | 256 | 16 | local | 2 | 62.774 | 12.814 | 4352 | 67584 | 4352 | 35424 | 0/0 | 215011760 | 505233408 | 7 |
| head | self_composite | 256 | 64 | insert | 0 | 18.357 | 5.180 | 16640 | 264192 | 16640 | 138480 | 0/0 | 250126000 | 507117568 | 4 |
| head | self_composite | 256 | 64 | insert | 1 | 9.718 | 5.047 | 16640 | 264192 | 16640 | 138480 | 0/0 | 274172152 | 507363328 | 3 |
| head | self_composite | 256 | 64 | insert | 2 | 26.018 | 9.002 | 16640 | 264192 | 16640 | 138480 | 0/0 | 223652944 | 507527168 | 5 |
| head | self_composite | 256 | 64 | load | 0 | 16.061 | 4.422 | 16640 | 264192 | 16640 | 138480 | 0/0 | 254359640 | 507772928 | 4 |
| head | self_composite | 256 | 64 | load | 1 | 26.892 | 13.925 | 16640 | 264192 | 16640 | 138480 | 0/0 | 278563576 | 508035072 | 6 |
| head | self_composite | 256 | 64 | load | 2 | 9.998 | 6.042 | 16640 | 264192 | 16640 | 138480 | 0/0 | 236605320 | 508280832 | 3 |
| head | self_composite | 256 | 64 | local | 0 | 34.900 | 5.186 | 16640 | 264192 | 16640 | 138480 | 0/0 | 276609552 | 511426560 | 6 |
| head | self_composite | 256 | 64 | local | 1 | 98.028 | 9.306 | 16640 | 264192 | 16640 | 138480 | 0/0 | 235759976 | 511950848 | 9 |
| head | self_composite | 256 | 64 | local | 2 | 31.792 | 11.946 | 16640 | 264192 | 16640 | 138480 | 0/0 | 261756544 | 512049152 | 6 |
| head | self_composite | 256 | 256 | insert | 0 | 33.099 | 5.796 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 225378688 | 512344064 | 5 |
| head | self_composite | 256 | 256 | insert | 1 | 44.422 | 24.110 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 267425576 | 512933888 | 7 |
| head | self_composite | 256 | 256 | insert | 2 | 33.385 | 5.332 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 246441168 | 513196032 | 5 |
| head | self_composite | 256 | 256 | load | 0 | 66.609 | 15.416 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 332326920 | 534282240 | 8 |
| head | self_composite | 256 | 256 | load | 1 | 43.032 | 5.578 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 282947200 | 535674880 | 6 |
| head | self_composite | 256 | 256 | load | 2 | 56.742 | 7.427 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 324081048 | 536281088 | 8 |
| head | self_composite | 256 | 256 | local | 0 | 111.820 | 5.059 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 259312848 | 536952832 | 11 |
| head | self_composite | 256 | 256 | local | 1 | 85.165 | 13.234 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 295154024 | 537247744 | 10 |
| head | self_composite | 256 | 256 | local | 2 | 52.996 | 6.095 | 65792 | 1050624 | 65792 | 550704 | 0/0 | 261752960 | 537477120 | 7 |
| head | ordinary | 4096 | 16 | insert | 0 | 3.623 | 4.552 | 4096 | 32768 | 4096 | 16417 | 0/0 | 250905696 | 540688384 | 2 |
| head | ordinary | 4096 | 16 | insert | 1 | 6.963 | 5.404 | 4096 | 32768 | 4096 | 16417 | 0/0 | 261460104 | 540704768 | 3 |
| head | ordinary | 4096 | 16 | insert | 2 | 4.871 | 5.715 | 4096 | 32768 | 4096 | 16417 | 0/0 | 273060752 | 540721152 | 3 |
| head | ordinary | 4096 | 16 | load | 0 | 5.717 | 5.418 | 4096 | 32768 | 4096 | 16417 | 0/0 | 230826784 | 540753920 | 3 |
| head | ordinary | 4096 | 16 | load | 1 | 5.119 | 5.632 | 4096 | 32768 | 4096 | 16417 | 0/0 | 223439280 | 540753920 | 3 |
| head | ordinary | 4096 | 16 | load | 2 | 6.693 | 4.597 | 4096 | 32768 | 4096 | 16417 | 0/0 | 234256328 | 540786688 | 3 |
| head | ordinary | 4096 | 16 | local | 0 | 25.140 | 13.143 | 4096 | 32768 | 4096 | 16417 | 0/0 | 246236512 | 540803072 | 4 |
| head | ordinary | 4096 | 16 | local | 1 | 20.455 | 4.060 | 4096 | 32768 | 4096 | 16417 | 0/0 | 257719904 | 540852224 | 4 |
| head | ordinary | 4096 | 16 | local | 2 | 26.127 | 5.018 | 4096 | 32768 | 4096 | 16417 | 0/0 | 269200648 | 540868608 | 4 |
| head | ordinary | 4096 | 64 | insert | 0 | 8.353 | 5.565 | 4096 | 32768 | 4096 | 16417 | 0/0 | 280288560 | 540884992 | 3 |
| head | ordinary | 4096 | 64 | insert | 1 | 8.248 | 3.422 | 4096 | 32768 | 4096 | 16417 | 0/0 | 213435032 | 540917760 | 3 |
| head | ordinary | 4096 | 64 | insert | 2 | 11.253 | 5.399 | 4096 | 32768 | 4096 | 16417 | 0/0 | 228581960 | 540917760 | 3 |
| head | ordinary | 4096 | 64 | load | 0 | 7.135 | 4.563 | 4096 | 32768 | 4096 | 16417 | 0/0 | 239764032 | 540934144 | 3 |
| head | ordinary | 4096 | 64 | load | 1 | 8.526 | 4.883 | 4096 | 32768 | 4096 | 16417 | 0/0 | 251078840 | 540934144 | 3 |
| head | ordinary | 4096 | 64 | load | 2 | 6.854 | 6.278 | 4096 | 32768 | 4096 | 16417 | 0/0 | 262270744 | 540983296 | 3 |
| head | ordinary | 4096 | 64 | local | 0 | 30.013 | 11.840 | 4096 | 32768 | 4096 | 16417 | 0/0 | 274842608 | 541130752 | 6 |
| head | ordinary | 4096 | 64 | local | 1 | 34.673 | 14.081 | 4096 | 32768 | 4096 | 16417 | 0/0 | 284743312 | 541229056 | 5 |
| head | ordinary | 4096 | 64 | local | 2 | 21.644 | 5.098 | 4096 | 32768 | 4096 | 16417 | 0/0 | 225805632 | 541278208 | 4 |
| head | ordinary | 4096 | 256 | insert | 0 | 12.926 | 5.763 | 4096 | 32768 | 4096 | 16417 | 0/0 | 240922688 | 541343744 | 3 |
| head | ordinary | 4096 | 256 | insert | 1 | 13.928 | 5.846 | 4096 | 32768 | 4096 | 16417 | 0/0 | 256998208 | 541425664 | 4 |
| head | ordinary | 4096 | 256 | insert | 2 | 13.235 | 5.750 | 4096 | 32768 | 4096 | 16417 | 0/0 | 276982776 | 541523968 | 3 |
| head | ordinary | 4096 | 256 | load | 0 | 17.729 | 6.738 | 4096 | 32768 | 4096 | 16417 | 0/0 | 287176096 | 541622272 | 4 |
| head | ordinary | 4096 | 256 | load | 1 | 10.899 | 10.293 | 4096 | 32768 | 4096 | 16417 | 0/0 | 238260440 | 541687808 | 4 |
| head | ordinary | 4096 | 256 | load | 2 | 13.297 | 6.563 | 4096 | 32768 | 4096 | 16417 | 0/0 | 252653064 | 541786112 | 4 |
| head | ordinary | 4096 | 256 | local | 0 | 36.344 | 4.917 | 4096 | 32768 | 4096 | 16417 | 0/0 | 267608360 | 541802496 | 5 |
| head | ordinary | 4096 | 256 | local | 1 | 24.368 | 11.845 | 4096 | 32768 | 4096 | 16417 | 0/0 | 282432192 | 541835264 | 5 |
| head | ordinary | 4096 | 256 | local | 2 | 46.896 | 6.519 | 4096 | 32768 | 4096 | 16417 | 0/0 | 297443752 | 541949952 | 7 |
| head | auto_pk | 4096 | 16 | insert | 0 | 6.073 | 4.524 | 65536 | 524288 | 65536 | 262672 | 0/0 | 226852616 | 541999104 | 3 |
| head | auto_pk | 4096 | 16 | insert | 1 | 6.942 | 4.509 | 65536 | 524288 | 65536 | 262672 | 0/0 | 239157856 | 542048256 | 3 |
| head | auto_pk | 4096 | 16 | insert | 2 | 7.054 | 4.865 | 65536 | 524288 | 65536 | 262672 | 0/0 | 251470224 | 542113792 | 3 |
| head | auto_pk | 4096 | 16 | load | 0 | 5.356 | 9.256 | 65536 | 524288 | 65536 | 262672 | 0/0 | 264655680 | 542162944 | 3 |
| head | auto_pk | 4096 | 16 | load | 1 | 7.857 | 5.511 | 65536 | 524288 | 65536 | 262672 | 0/0 | 277550624 | 542162944 | 3 |
| head | auto_pk | 4096 | 16 | load | 2 | 10.272 | 6.255 | 65536 | 524288 | 65536 | 262672 | 0/0 | 290960400 | 542195712 | 3 |
| head | auto_pk | 4096 | 16 | local | 0 | 25.277 | 7.236 | 65536 | 524288 | 65536 | 262672 | 0/0 | 302884160 | 542294016 | 5 |
| head | auto_pk | 4096 | 16 | local | 1 | 15.552 | 6.310 | 65536 | 524288 | 65536 | 262672 | 0/0 | 231477112 | 542359552 | 4 |
| head | auto_pk | 4096 | 16 | local | 2 | 18.842 | 5.062 | 65536 | 524288 | 65536 | 262672 | 0/0 | 245362736 | 542425088 | 4 |
| head | auto_pk | 4096 | 64 | insert | 0 | 7.659 | 4.860 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 260276896 | 542441472 | 3 |
| head | auto_pk | 4096 | 64 | insert | 1 | 9.800 | 4.904 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 275247744 | 542457856 | 3 |
| head | auto_pk | 4096 | 64 | insert | 2 | 7.981 | 6.719 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 290204392 | 542507008 | 3 |
| head | auto_pk | 4096 | 64 | load | 0 | 10.505 | 11.379 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 305854096 | 542523392 | 4 |
| head | auto_pk | 4096 | 64 | load | 1 | 9.872 | 7.077 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 230759016 | 542556160 | 3 |
| head | auto_pk | 4096 | 64 | load | 2 | 7.196 | 5.754 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 245885912 | 542900224 | 3 |
| head | auto_pk | 4096 | 64 | local | 0 | 63.024 | 24.564 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 263375496 | 542982144 | 9 |
| head | auto_pk | 4096 | 64 | local | 1 | 61.106 | 17.718 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 283000424 | 543129600 | 8 |
| head | auto_pk | 4096 | 64 | local | 2 | 78.187 | 18.073 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 302707344 | 543244288 | 9 |
| head | auto_pk | 4096 | 256 | insert | 0 | 36.613 | 5.767 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 315236872 | 543293440 | 6 |
| head | auto_pk | 4096 | 256 | insert | 1 | 20.894 | 6.994 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 258717176 | 543293440 | 4 |
| head | auto_pk | 4096 | 256 | insert | 2 | 19.013 | 6.784 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 283998888 | 543309824 | 4 |
| head | auto_pk | 4096 | 256 | load | 0 | 31.640 | 13.841 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 309898032 | 543326208 | 6 |
| head | auto_pk | 4096 | 256 | load | 1 | 28.462 | 5.463 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 240677232 | 543408128 | 5 |
| head | auto_pk | 4096 | 256 | load | 2 | 23.561 | 5.074 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 265859656 | 543506432 | 4 |
| head | auto_pk | 4096 | 256 | local | 0 | 44.411 | 9.011 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 299648984 | 543604736 | 7 |
| head | auto_pk | 4096 | 256 | local | 1 | 39.898 | 9.094 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 311564008 | 543637504 | 6 |
| head | auto_pk | 4096 | 256 | local | 2 | 27.321 | 4.909 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 260671960 | 543686656 | 5 |
| head | auto_nonpk | 4096 | 16 | insert | 0 | 8.980 | 4.521 | 65536 | 524288 | 65536 | 262672 | 0/0 | 273963488 | 543735808 | 3 |
| head | auto_nonpk | 4096 | 16 | insert | 1 | 5.088 | 4.545 | 65536 | 524288 | 65536 | 262672 | 0/0 | 286315960 | 543752192 | 3 |
| head | auto_nonpk | 4096 | 16 | insert | 2 | 6.681 | 4.526 | 65536 | 524288 | 65536 | 262672 | 0/0 | 298886192 | 543768576 | 3 |
| head | auto_nonpk | 4096 | 16 | load | 0 | 28.408 | 5.199 | 65536 | 524288 | 65536 | 262672 | 0/0 | 311079720 | 543784960 | 4 |
| head | auto_nonpk | 4096 | 16 | load | 1 | 6.564 | 3.972 | 65536 | 524288 | 65536 | 262672 | 0/0 | 235629976 | 543784960 | 3 |
| head | auto_nonpk | 4096 | 16 | load | 2 | 11.506 | 4.817 | 65536 | 524288 | 65536 | 262672 | 0/0 | 249101744 | 543784960 | 3 |
| head | auto_nonpk | 4096 | 16 | local | 0 | 13.362 | 4.990 | 65536 | 524288 | 65536 | 262672 | 0/0 | 262151096 | 543801344 | 3 |
| head | auto_nonpk | 4096 | 16 | local | 1 | 16.572 | 5.210 | 65536 | 524288 | 65536 | 262672 | 0/0 | 275383712 | 543834112 | 4 |
| head | auto_nonpk | 4096 | 16 | local | 2 | 15.168 | 5.202 | 65536 | 524288 | 65536 | 262672 | 0/0 | 288477880 | 543866880 | 4 |
| head | auto_nonpk | 4096 | 64 | insert | 0 | 8.944 | 5.459 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 304150784 | 543916032 | 3 |
| head | auto_nonpk | 4096 | 64 | insert | 1 | 11.265 | 5.629 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 315241856 | 544047104 | 3 |
| head | auto_nonpk | 4096 | 64 | insert | 2 | 8.999 | 3.718 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 247067472 | 544079872 | 3 |
| head | auto_nonpk | 4096 | 64 | load | 0 | 13.497 | 4.625 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 262442232 | 544112640 | 3 |
| head | auto_nonpk | 4096 | 64 | load | 1 | 7.852 | 5.545 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 277765000 | 544161792 | 3 |
| head | auto_nonpk | 4096 | 64 | load | 2 | 8.536 | 4.500 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 293141288 | 544194560 | 3 |
| head | auto_nonpk | 4096 | 64 | local | 0 | 21.693 | 4.534 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 308745136 | 544210944 | 4 |
| head | auto_nonpk | 4096 | 64 | local | 1 | 31.412 | 4.014 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 320430056 | 544358400 | 5 |
| head | auto_nonpk | 4096 | 64 | local | 2 | 20.927 | 5.169 | 262144 | 2097152 | 262144 | 1050688 | 0/0 | 244786976 | 544407552 | 4 |
| head | auto_nonpk | 4096 | 256 | insert | 0 | 31.336 | 5.546 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 271030832 | 544456704 | 5 |
| head | auto_nonpk | 4096 | 256 | insert | 1 | 22.637 | 5.767 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 296461536 | 544489472 | 4 |
| head | auto_nonpk | 4096 | 256 | insert | 2 | 39.143 | 6.292 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 323348520 | 544522240 | 6 |
| head | auto_nonpk | 4096 | 256 | load | 0 | 23.303 | 6.066 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 260265208 | 544587776 | 5 |
| head | auto_nonpk | 4096 | 256 | load | 1 | 25.668 | 5.086 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 285565112 | 544669696 | 5 |
| head | auto_nonpk | 4096 | 256 | load | 2 | 26.512 | 5.016 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 311461000 | 544702464 | 5 |
| head | auto_nonpk | 4096 | 256 | local | 0 | 53.609 | 8.791 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 331630904 | 546291712 | 8 |
| head | auto_nonpk | 4096 | 256 | local | 1 | 31.183 | 12.038 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 267892464 | 546291712 | 6 |
| head | auto_nonpk | 4096 | 256 | local | 2 | 28.826 | 3.879 | 1048576 | 8388608 | 1048576 | 4202752 | 0/0 | 293236016 | 546324480 | 5 |
| head | auto_composite | 4096 | 16 | insert | 0 | 9.219 | 4.773 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 306857496 | 546390016 | 3 |
| head | auto_composite | 4096 | 16 | insert | 1 | 41.658 | 5.673 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 320218440 | 546455552 | 6 |
| head | auto_composite | 4096 | 16 | insert | 2 | 7.885 | 4.534 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 241187160 | 546488320 | 3 |
| head | auto_composite | 4096 | 16 | load | 0 | 8.954 | 5.209 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 255914936 | 546537472 | 3 |
| head | auto_composite | 4096 | 16 | load | 1 | 6.159 | 4.956 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 269783736 | 546537472 | 3 |
| head | auto_composite | 4096 | 16 | load | 2 | 6.981 | 4.584 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 283902184 | 546603008 | 3 |
| head | auto_composite | 4096 | 16 | local | 0 | 22.239 | 14.548 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 298120776 | 546603008 | 5 |
| head | auto_composite | 4096 | 16 | local | 1 | 38.141 | 5.880 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 310521136 | 546619392 | 6 |
| head | auto_composite | 4096 | 16 | local | 2 | 16.553 | 4.907 | 65536 | 1048576 | 65536 | 265600 | 0/0 | 253515088 | 546652160 | 4 |
| head | auto_composite | 4096 | 64 | insert | 0 | 14.170 | 6.540 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 272097600 | 546783232 | 4 |
| head | auto_composite | 4096 | 64 | insert | 1 | 10.580 | 6.491 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 290991960 | 546816000 | 3 |
| head | auto_composite | 4096 | 64 | insert | 2 | 10.934 | 5.767 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 309469272 | 546816000 | 3 |
| head | auto_composite | 4096 | 64 | load | 0 | 11.204 | 5.761 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 244627576 | 546848768 | 3 |
| head | auto_composite | 4096 | 64 | load | 1 | 28.722 | 5.351 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 263281592 | 546914304 | 5 |
| head | auto_composite | 4096 | 64 | load | 2 | 10.216 | 5.034 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 281973480 | 546947072 | 3 |
| head | auto_composite | 4096 | 64 | local | 0 | 18.769 | 4.879 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 301061424 | 547028992 | 4 |
| head | auto_composite | 4096 | 64 | local | 1 | 55.864 | 5.687 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 320216576 | 547045376 | 7 |
| head | auto_composite | 4096 | 64 | local | 2 | 28.106 | 8.348 | 262144 | 4194304 | 262144 | 1062400 | 0/0 | 253655192 | 547045376 | 5 |
| head | auto_composite | 4096 | 256 | insert | 0 | 41.883 | 5.728 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 291955648 | 547110912 | 6 |
| head | auto_composite | 4096 | 256 | insert | 1 | 70.403 | 4.684 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 346380640 | 547192832 | 9 |
| head | auto_composite | 4096 | 256 | insert | 2 | 64.696 | 4.004 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 321120056 | 550060032 | 7 |
| head | auto_composite | 4096 | 256 | load | 0 | 49.615 | 10.680 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 358878704 | 549797888 | 8 |
| head | auto_composite | 4096 | 256 | load | 1 | 36.023 | 5.013 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 277135768 | 549863424 | 6 |
| head | auto_composite | 4096 | 256 | load | 2 | 37.032 | 7.508 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 314839232 | 550125568 | 6 |
| head | auto_composite | 4096 | 256 | local | 0 | 126.009 | 12.212 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 326991560 | 550158336 | 14 |
| head | auto_composite | 4096 | 256 | local | 1 | 225.590 | 12.479 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 303705560 | 550273024 | 20 |
| head | auto_composite | 4096 | 256 | local | 2 | 76.296 | 50.407 | 1048576 | 16777216 | 1048576 | 4249600 | 0/0 | 324491944 | 554909696 | 12 |
| head | self_composite | 4096 | 16 | insert | 0 | 18.803 | 4.328 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 263854496 | 714407936 | 4 |
| head | self_composite | 4096 | 16 | insert | 1 | 14.913 | 4.986 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 296800440 | 716865536 | 4 |
| head | self_composite | 4096 | 16 | insert | 2 | 15.165 | 6.107 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 293747064 | 719142912 | 4 |
| head | self_composite | 4096 | 16 | load | 0 | 18.245 | 8.268 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 257016136 | 721207296 | 4 |
| head | self_composite | 4096 | 16 | load | 1 | 15.622 | 5.645 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 272882152 | 723042304 | 4 |
| head | self_composite | 4096 | 16 | load | 2 | 21.902 | 7.099 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 260454000 | 724860928 | 4 |
| head | self_composite | 4096 | 16 | local | 0 | 34.785 | 10.044 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 259366152 | 726843392 | 6 |
| head | self_composite | 4096 | 16 | local | 1 | 34.001 | 4.975 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 259915352 | 729071616 | 5 |
| head | self_composite | 4096 | 16 | local | 2 | 49.161 | 11.443 | 69632 | 1081344 | 69632 | 542401 | 0/0 | 258628280 | 731529216 | 7 |
| head | self_composite | 4096 | 64 | insert | 0 | 56.875 | 12.974 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 299084744 | 734953472 | 8 |
| head | self_composite | 4096 | 64 | insert | 1 | 87.666 | 4.998 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 319452600 | 736428032 | 10 |
| head | self_composite | 4096 | 64 | insert | 2 | 49.611 | 6.929 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 288652936 | 738017280 | 7 |
| head | self_composite | 4096 | 64 | load | 0 | 86.583 | 6.607 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 318780528 | 739819520 | 10 |
| head | self_composite | 4096 | 64 | load | 1 | 82.854 | 14.787 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 318913232 | 742686720 | 10 |
| head | self_composite | 4096 | 64 | load | 2 | 83.533 | 8.650 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 306131584 | 749043712 | 11 |
| head | self_composite | 4096 | 64 | local | 0 | 70.331 | 8.950 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 289089176 | 750256128 | 9 |
| head | self_composite | 4096 | 64 | local | 1 | 111.591 | 4.992 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 320317976 | 751910912 | 13 |
| head | self_composite | 4096 | 64 | local | 2 | 72.524 | 4.195 | 266240 | 4227072 | 266240 | 2120353 | 0/0 | 289040840 | 753205248 | 9 |
| head | self_composite | 4096 | 256 | insert | 0 | 276.038 | 7.454 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 359157936 | 756482048 | 28 |
| head | self_composite | 4096 | 256 | insert | 1 | 270.224 | 6.567 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 334369504 | 758890496 | 27 |
| head | self_composite | 4096 | 256 | insert | 2 | 275.648 | 6.690 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 344078920 | 760119296 | 28 |
| head | self_composite | 4096 | 256 | load | 0 | 270.913 | 5.884 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 349139280 | 761053184 | 26 |
| head | self_composite | 4096 | 256 | load | 1 | 386.453 | 20.249 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 355876848 | 762101760 | 37 |
| head | self_composite | 4096 | 256 | load | 2 | 464.742 | 25.447 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 353602800 | 764887040 | 42 |
| head | self_composite | 4096 | 256 | local | 0 | 419.249 | 15.341 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 359171096 | 765673472 | 38 |
| head | self_composite | 4096 | 256 | local | 1 | 431.083 | 14.221 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 361361880 | 766738432 | 42 |
| head | self_composite | 4096 | 256 | local | 2 | 636.342 | 74.441 | 1052672 | 16809984 | 1052672 | 8432161 | 0/0 | 364784280 | 774176768 | 45 |

## 锁等待与取消原始样本

只有公共 mo_locks() 的实际 waiter 才算 barrier；未等待时不伪造取消延迟。writer 从 SQL 入口开始计时，trigger→completion 从 holder commit / KILL QUERY 发出前开始；不包含无意义的人工持锁 sleep。

| variant | shape | action | trial | observed wait | outcome | writer total ms | trigger→completion ms | error |
| --- | --- | --- | ---: | --- | --- | ---: | --- | --- |
| base | ordinary | release | 0 | False | committed | 10.805 | 无等待，不适用 |  |
| base | ordinary | cancel | 0 | False | committed | 15.848 | 无等待，不适用 |  |
| base | ordinary | cancel_child | 0 | False | committed | 20.233 | 无等待，不适用 |  |
| base | auto_pk | release | 0 | False | committed | 8.294 | 无等待，不适用 |  |
| base | auto_pk | cancel | 0 | False | committed | 7.190 | 无等待，不适用 |  |
| base | auto_pk | cancel_child | 0 | False | committed | 17.147 | 无等待，不适用 |  |
| base | auto_nonpk | release | 0 | False | committed | 7.091 | 无等待，不适用 |  |
| base | auto_nonpk | cancel | 0 | False | committed | 6.085 | 无等待，不适用 |  |
| base | auto_nonpk | cancel_child | 0 | False | committed | 12.854 | 无等待，不适用 |  |
| base | auto_composite | release | 0 | False | committed | 8.242 | 无等待，不适用 |  |
| base | auto_composite | cancel | 0 | False | committed | 6.045 | 无等待，不适用 |  |
| base | auto_composite | cancel_child | 0 | False | committed | 13.244 | 无等待，不适用 |  |
| base | ordinary | release | 1 | False | committed | 10.509 | 无等待，不适用 |  |
| base | ordinary | cancel | 1 | False | committed | 12.843 | 无等待，不适用 |  |
| base | ordinary | cancel_child | 1 | False | committed | 21.188 | 无等待，不适用 |  |
| base | auto_pk | release | 1 | False | committed | 6.181 | 无等待，不适用 |  |
| base | auto_pk | cancel | 1 | False | committed | 7.213 | 无等待，不适用 |  |
| base | auto_pk | cancel_child | 1 | False | committed | 12.545 | 无等待，不适用 |  |
| base | auto_nonpk | release | 1 | False | committed | 5.870 | 无等待，不适用 |  |
| base | auto_nonpk | cancel | 1 | False | committed | 5.130 | 无等待，不适用 |  |
| base | auto_nonpk | cancel_child | 1 | False | committed | 19.578 | 无等待，不适用 |  |
| base | auto_composite | release | 1 | False | committed | 5.667 | 无等待，不适用 |  |
| base | auto_composite | cancel | 1 | False | committed | 6.304 | 无等待，不适用 |  |
| base | auto_composite | cancel_child | 1 | False | committed | 32.694 | 无等待，不适用 |  |
| base | ordinary | release | 2 | False | committed | 7.094 | 无等待，不适用 |  |
| base | ordinary | cancel | 2 | False | committed | 6.275 | 无等待，不适用 |  |
| base | ordinary | cancel_child | 2 | False | committed | 10.548 | 无等待，不适用 |  |
| base | auto_pk | release | 2 | False | committed | 13.903 | 无等待，不适用 |  |
| base | auto_pk | cancel | 2 | False | committed | 7.216 | 无等待，不适用 |  |
| base | auto_pk | cancel_child | 2 | False | committed | 40.837 | 无等待，不适用 |  |
| base | auto_nonpk | release | 2 | False | committed | 6.125 | 无等待，不适用 |  |
| base | auto_nonpk | cancel | 2 | False | committed | 10.488 | 无等待，不适用 |  |
| base | auto_nonpk | cancel_child | 2 | False | committed | 18.932 | 无等待，不适用 |  |
| base | auto_composite | release | 2 | False | committed | 6.385 | 无等待，不适用 |  |
| base | auto_composite | cancel | 2 | False | committed | 8.739 | 无等待，不适用 |  |
| base | auto_composite | cancel_child | 2 | False | committed | 12.682 | 无等待，不适用 |  |
| head | ordinary | release | 0 | False | committed | 12.183 | 无等待，不适用 |  |
| head | ordinary | cancel | 0 | False | committed | 11.197 | 无等待，不适用 |  |
| head | ordinary | cancel_child | 0 | False | committed | 17.589 | 无等待，不适用 |  |
| head | auto_pk | release | 0 | True | committed | 62.716 | 55.862 |  |
| head | auto_pk | cancel | 0 | True | error | 6.607 | 2.736 | (1317, 'context canceled') |
| head | auto_pk | cancel_child | 0 | True | error | 19.661 | 1.416 | (1317, 'context canceled') |
| head | auto_nonpk | release | 0 | True | committed | 45.484 | 26.883 |  |
| head | auto_nonpk | cancel | 0 | True | error | 27.340 | 3.846 | (1317, 'context canceled') |
| head | auto_nonpk | cancel_child | 0 | True | error | 27.601 | 3.399 | (1317, 'context canceled') |
| head | auto_composite | release | 0 | True | committed | 51.483 | 33.214 |  |
| head | auto_composite | cancel | 0 | True | error | 4.379 | 4.707 | (1317, 'context canceled') |
| head | auto_composite | cancel_child | 0 | True | error | 31.139 | 2.723 | (1317, 'context canceled') |
| head | ordinary | release | 1 | False | committed | 10.089 | 无等待，不适用 |  |
| head | ordinary | cancel | 1 | False | committed | 11.774 | 无等待，不适用 |  |
| head | ordinary | cancel_child | 1 | False | committed | 31.882 | 无等待，不适用 |  |
| head | auto_pk | release | 1 | True | committed | 21.231 | 16.701 |  |
| head | auto_pk | cancel | 1 | True | error | 5.796 | 2.541 | (1317, 'context canceled') |
| head | auto_pk | cancel_child | 1 | True | error | 19.249 | 2.296 | (1317, 'context canceled') |
| head | auto_nonpk | release | 1 | True | committed | 42.822 | 31.170 |  |
| head | auto_nonpk | cancel | 1 | True | error | 17.742 | 1.569 | (1317, 'context canceled') |
| head | auto_nonpk | cancel_child | 1 | True | error | 36.172 | 1.310 | (1317, 'context canceled') |
| head | auto_composite | release | 1 | True | committed | 49.220 | 24.525 |  |
| head | auto_composite | cancel | 1 | True | error | 13.932 | 0.439 | (1317, 'context canceled') |
| head | auto_composite | cancel_child | 1 | True | error | 14.335 | 0.406 | (1317, 'context canceled') |
| head | ordinary | release | 2 | False | committed | 11.072 | 无等待，不适用 |  |
| head | ordinary | cancel | 2 | False | committed | 6.985 | 无等待，不适用 |  |
| head | ordinary | cancel_child | 2 | False | committed | 10.194 | 无等待，不适用 |  |
| head | auto_pk | release | 2 | True | committed | 27.302 | 14.438 |  |
| head | auto_pk | cancel | 2 | True | error | 2.681 | 0.452 | (1317, 'context canceled') |
| head | auto_pk | cancel_child | 2 | True | error | 15.785 | 0.424 | (1317, 'context canceled') |
| head | auto_nonpk | release | 2 | True | committed | 14.127 | 12.025 |  |
| head | auto_nonpk | cancel | 2 | True | error | 21.666 | 0.907 | (1317, 'context canceled') |
| head | auto_nonpk | cancel_child | 2 | True | error | 15.840 | 0.415 | (1317, 'context canceled') |
| head | auto_composite | release | 2 | True | committed | 15.175 | 12.971 |  |
| head | auto_composite | cancel | 2 | True | error | 2.478 | 0.382 | (1317, 'context canceled') |
| head | auto_composite | cancel_child | 2 | True | error | 22.330 | 4.196 | (1317, 'context canceled') |
