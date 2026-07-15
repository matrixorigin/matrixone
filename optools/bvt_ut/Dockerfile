FROM matrixorigin/tester:go1.26.4-jdk8

ARG GOPROXY="https://proxy.golang.org,direct"
ENV GOWORK=off

ENV UT_PARALLEL=1
ENV LC_ALL="C.UTF-8"
ENV LAUNCH="launch"
ENV ENABLE_UT="false"

RUN go env -w GOPROXY=$GOPROXY

RUN mkdir -p /matrixone-test

WORKDIR /matrixone-test

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

ENTRYPOINT [ "/matrixone-test/optools/bvt_ut/entrypoint.sh" ]
