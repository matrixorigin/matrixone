FROM  matrixorigin/tester:go1.19-java8

ARG GOPROXY="https://proxy.golang.org,direct"

ENV UT_PARALLEL=1
ENV LC_ALL="C.UTF-8"
ENV LAUNCH="launch-tae-logservice"
ENV ENABLE_UT="false"

RUN go env -w GOPROXY=$GOPROXY

RUN mkdir -p /matrixone-test

WORKDIR /matrixone-test

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

ENTRYPOINT [ "/matrixone-test/optools/bvt_ut/entrypoint.sh" ]
