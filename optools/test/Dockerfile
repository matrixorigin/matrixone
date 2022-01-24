FROM golang:1.17.3-alpine3.14 as builder

RUN apk add --no-cache \
    wget \
    make \
    git \
    gcc \
    binutils-gold \
    musl-dev

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone
WORKDIR /go/src/github.com/matrixorigin/matrixone

RUN go env -w GOPROXY=https://goproxy.cn,direct && go env -w GO111MODULE=on

COPY . .
RUN make config && make build


FROM alpine

RUN mkdir config

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-server /opt/matrixone/mo-server
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/dumb-init
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/system_vars_config.toml /opt/matrixone/system_vars_config.toml

WORKDIR /opt/matrixone

EXPOSE 6001-6010

ENTRYPOINT ["/usr/local/bin/dumb-init", "./mo-server", "./system_vars_config.toml"]