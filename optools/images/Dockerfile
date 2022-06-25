FROM golang:1.18-buster as builder

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN make config && make build

FROM ubuntu:latest

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-server /mo-server
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/system_vars_config.toml /system_vars_config.toml

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-server", "/system_vars_config.toml"]
