# syntax=docker/dockerfile:1.7

FROM matrixorigin/golang:1.26.4-ubuntu22.04 AS build-base

ENV GOWORK=off

WORKDIR /go/src/github.com/matrixorigin/matrixone

# Native dependencies change much less frequently than Go sources. Keeping
# them in an independent stage lets BuildKit's registry cache reuse the pinned
# ONNX download and compiled C/C++ libraries across source-only commits.
FROM build-base AS native

COPY thirdparties thirdparties
RUN make -C thirdparties

COPY cgo cgo
RUN make -C cgo

# The Iceberg adapter is a nested module and is invisible to the root module
# download. Isolate its cache from the production builder: this stage is only
# referenced by connector-builder, so ordinary runtime images never pay for
# adapter dependencies.
FROM build-base AS connector-modules

ARG GOPROXY="https://proxy.golang.org,direct"
ARG GO_MOD_DOWNLOAD_TIMEOUT=0
RUN go env -w GOPROXY=${GOPROXY}

COPY go.mod go.mod
COPY go.sum go.sum
COPY pkg/iceberg/adapter/iceberggo/go.mod pkg/iceberg/adapter/iceberggo/go.mod
COPY pkg/iceberg/adapter/iceberggo/go.sum pkg/iceberg/adapter/iceberggo/go.sum
RUN --mount=type=cache,id=matrixone-go-modules,target=/tmp/gomod-cache,sharing=locked \
    nested_module_files="$(sha256sum pkg/iceberg/adapter/iceberggo/go.mod pkg/iceberg/adapter/iceberggo/go.sum)" && \
    mkdir -p /go/pkg/mod /tmp/gomod-cache && \
    cp -a /go/pkg/mod/. /tmp/gomod-cache/ && \
    (cd pkg/iceberg/adapter/iceberggo && timeout "$GO_MOD_DOWNLOAD_TIMEOUT" env GOMODCACHE=/tmp/gomod-cache go mod download) && \
    cp -a /tmp/gomod-cache/. /go/pkg/mod/ && \
    if [ "$nested_module_files" != "$(sha256sum pkg/iceberg/adapter/iceberggo/go.mod pkg/iceberg/adapter/iceberggo/go.sum)" ]; then \
        echo "nested go mod download changed go.mod/go.sum"; \
        exit 1; \
    fi

FROM build-base AS builder

# GOPROXY affects Go modules, not native dependencies. Keeping it out of the
# native stage preserves that expensive cache when build environments differ.
ARG GOPROXY="https://proxy.golang.org,direct"
ARG GO_MOD_DOWNLOAD_TIMEOUT=0
RUN go env -w GOPROXY=${GOPROXY}

# Typecheck: enable type checking for ToSliceNoTypeCheck and ToSliceNoTypeCheck2
# Default: 0 (disabled), set to 1 to enable
ARG TYPECHECK=0
ARG GOBUILD_OPT=""

COPY go.mod go.mod
COPY go.sum go.sum

# Cache dependencies before copying source so ordinary source changes do not
# invalidate them. The cache mount retains completed module downloads when a
# transient failure aborts this layer; copying it back into /go/pkg/mod after
# success makes the same cache exportable to a fresh GHA runner.
RUN --mount=type=cache,id=matrixone-go-modules,target=/tmp/gomod-cache,sharing=locked \
    module_files="$(sha256sum go.mod go.sum)" && \
    mkdir -p /go/pkg/mod /tmp/gomod-cache && \
    cp -a /go/pkg/mod/. /tmp/gomod-cache/ && \
    timeout "$GO_MOD_DOWNLOAD_TIMEOUT" env GOMODCACHE=/tmp/gomod-cache go mod download && \
    cp -a /tmp/gomod-cache/. /go/pkg/mod/ && \
    if [ "$module_files" != "$(sha256sum go.mod go.sum)" ]; then \
        echo "go mod download changed go.mod/go.sum; run make mod-tidy"; \
        exit 1; \
    fi

COPY . .

# Host native artifacts are excluded by .dockerignore. Restore only the
# verified outputs produced by the cacheable native stage.
COPY --from=native /go/src/github.com/matrixorigin/matrixone/thirdparties/install thirdparties/install
COPY --from=native /go/src/github.com/matrixorigin/matrixone/cgo/libmo.so cgo/libmo.so
COPY --from=native /go/src/github.com/matrixorigin/matrixone/cgo/libmo.a cgo/libmo.a

RUN --mount=type=cache,target=/root/.cache/go-build \
    if [ "$TYPECHECK" = "1" ]; then \
		make build-with-prebuilt-native TYPECHECK=1 GOBUILD_OPT="${GOBUILD_OPT}"; \
	else \
		make build-with-prebuilt-native GOBUILD_OPT="${GOBUILD_OPT}"; \
	fi

# Keep connector validation out of the production image's default dependency
# chain. CI explicitly targets connector-ci, while ordinary image builds still
# end at the runtime stage below.
FROM builder AS connector-builder

ARG SOURCE_REVISION=unknown
COPY --from=connector-modules /go/pkg/mod /go/pkg/mod
ENV CGO_CFLAGS="-I/go/src/github.com/matrixorigin/matrixone/cgo -I/go/src/github.com/matrixorigin/matrixone/thirdparties/install/include"
ENV CGO_LDFLAGS="-L/go/src/github.com/matrixorigin/matrixone/cgo -lmo -L/go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib -Wl,-rpath,/go/src/github.com/matrixorigin/matrixone/cgo -Wl,-rpath,/go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib -fopenmp"
ENV LD_LIBRARY_PATH="/go/src/github.com/matrixorigin/matrixone/cgo:/go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib"

RUN --mount=type=cache,target=/root/.cache/go-build \
    go test -count=1 ./test/mongodb ./test/iceberg && \
    (cd pkg/iceberg/adapter/iceberggo && go test -tags iceberggo -count=1 ./...) && \
    go test ./pkg/iceberg/metadata -run '^TestGoldenVectorProvenanceArtifacts$' -count=1 && \
    mkdir -p /connector-bin && \
    go build -o /connector-bin/mongodb-e2e ./test/mongodb && \
    go build -o /connector-bin/iceberg-e2e ./test/iceberg && \
    printf '%s\n' "$SOURCE_REVISION" >/connector-source-revision

FROM matrixorigin/ubuntu:22.04 AS runtime-base

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib/*.so /usr/local/lib
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/cgo/*.so /usr/local/lib
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/pkg/monlp/tokenizer/dict /usr/local/share/jieba
ENV MO_JIEBA_DICT_DIR=/usr/local/share/jieba

# ldconfig and run mo-service to check if the shared library is found. A
# coverage-instrumented binary requires a writable GOCOVERDIR even for -h.
RUN mkdir -p /tmp/coverage && ldconfig && GOCOVERDIR=/tmp/coverage /mo-service -h

WORKDIR /

EXPOSE 6001

FROM runtime-base AS connector-ci

ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}"

COPY --from=connector-builder /connector-bin /connector-bin
COPY --from=connector-builder /connector-source-revision /connector-source-revision
COPY --from=connector-builder /go/src/github.com/matrixorigin/matrixone/test/mongodb/fixture_manifest.json /test/mongodb/fixture_manifest.json

RUN test -x /connector-bin/mongodb-e2e && \
    test -x /connector-bin/iceberg-e2e && \
    test "$(cat /connector-source-revision)" = "$SOURCE_REVISION"

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]

FROM runtime-base AS runtime

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
