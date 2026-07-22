# syntax=docker/dockerfile:1

FROM matrixorigin/golang:1.26.4-ubuntu22.04 AS builder

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
ENV GOWORK=off
RUN go env -w GOPROXY=${GOPROXY}

# Typecheck: enable type checking for ToSliceNoTypeCheck and ToSliceNoTypeCheck2
# Default: 0 (disabled), set to 1 to enable
ARG TYPECHECK=0

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

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

# Native dependencies change much less frequently than the Go source. Keep
# them in independent layers so an external BuildKit cache can reuse them.
COPY thirdparties thirdparties
RUN make -C thirdparties

COPY cgo cgo
RUN make -C cgo

COPY . .

# CI can provide version metadata explicitly and omit .git from the build
# context. Local builds keep the existing behavior and derive it from .git.
ARG BUILD_BRANCH_NAME
ARG BUILD_COMMIT_ID
ARG BUILD_VERSION

RUN --mount=type=cache,id=mo-go-build,target=/root/.cache/go-build \
    branch_name="${BUILD_BRANCH_NAME:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)}" && \
    commit_id="${BUILD_COMMIT_ID:-$(git rev-parse --short HEAD 2>/dev/null || true)}" && \
    mo_version="${BUILD_VERSION:-$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match 2>/dev/null || true)}" && \
    if [ "$TYPECHECK" = "1" ]; then \
        make build TYPECHECK=1 BRANCH_NAME="$branch_name" LAST_COMMIT_ID="$commit_id" MO_VERSION="$mo_version"; \
    else \
        make build BRANCH_NAME="$branch_name" LAST_COMMIT_ID="$commit_id" MO_VERSION="$mo_version"; \
    fi

FROM matrixorigin/ubuntu:22.04

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib/*.so /usr/local/lib
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/cgo/*.so /usr/local/lib
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/pkg/monlp/tokenizer/dict /usr/local/share/jieba
ENV MO_JIEBA_DICT_DIR=/usr/local/share/jieba

# ldconfig and run mo-service to check if the shared library is found
RUN ldconfig && /mo-service -h

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
