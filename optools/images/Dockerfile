FROM matrixorigin/golang:1.26.4-ubuntu22.04 AS build-base

ENV GOWORK=off

WORKDIR /go/src/github.com/matrixorigin/matrixone

# Native dependencies change much less frequently than Go sources. Keeping
# them in an independent stage lets BuildKit's registry cache reuse the pinned
# ONNX download and compiled C/C++ libraries across source-only commits.
FROM build-base AS native

COPY thirdparties thirdparties
# This branch image is only used for non-ONNX regression validation. Build the
# native dependencies those workloads need without fetching ONNX Runtime.
RUN make -C thirdparties init usearch xxhash croaring

COPY cgo cgo
RUN make -C cgo

FROM build-base AS builder

# GOPROXY affects Go modules, not native dependencies. Keeping it out of the
# native stage preserves that expensive cache when build environments differ.
ARG GOPROXY="https://proxy.golang.org,direct"
RUN go env -w GOPROXY=${GOPROXY}

# Typecheck: enable type checking for ToSliceNoTypeCheck and ToSliceNoTypeCheck2
# Default: 0 (disabled), set to 1 to enable
ARG TYPECHECK=0
ARG GOBUILD_OPT=""

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN module_files="$(sha256sum go.mod go.sum)" && \
    go mod download && \
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

RUN if [ "$TYPECHECK" = "1" ]; then \
		make build-with-prebuilt-native TYPECHECK=1 GOBUILD_OPT="${GOBUILD_OPT}"; \
	else \
		make build-with-prebuilt-native GOBUILD_OPT="${GOBUILD_OPT}"; \
	fi

FROM matrixorigin/ubuntu:22.04

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

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
