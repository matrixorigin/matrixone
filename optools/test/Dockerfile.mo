FROM golang:1.18-alpine3.14 as builder

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

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN make config && make build


FROM alpine

RUN apk add --no-cache bash

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-server /mo-server
COPY --from=builder /usr/local/bin/dumb-init /usr/local/bin/dumb-init
COPY /optools/test/config.toml /system_vars_config.toml
COPY --chmod=755 /optools/test/entrypoint.sh /entrypoint.sh

WORKDIR /

EXPOSE 6001

ENTRYPOINT ["/entrypoint.sh"]
