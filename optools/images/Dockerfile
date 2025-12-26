FROM matrixorigin/golang:1.25-ubuntu22.04 AS builder

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
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
RUN go mod download

COPY . .

RUN if [ "$TYPECHECK" = "1" ]; then \
        make clean && make build TYPECHECK=1; \
    else \
        make clean && make build; \
    fi

FROM matrixorigin/ubuntu:22.04

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib/*.so /usr/local/lib

# ldconfig and run mo-service to check if the shared library is found
RUN ldconfig && /mo-service -h

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
