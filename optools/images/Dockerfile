FROM golang:1.17.3-buster as builder

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

RUN go env -w GOPROXY=https://goproxy.cn,direct

COPY . .

RUN make config && make build


FROM ubuntu:latest


COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-server /mo-server
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/system_vars_config.toml /system_vars_config.toml

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-server", "/system_vars_config.toml"]