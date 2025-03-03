FROM ubuntu:22.04 AS builder


# Install some utilities used for debugging or by startup script
RUN apt-get update && apt-get install -y \
    golang-1.23 \
    build-essential \
    dnsutils \
    curl \
    git \
    cmake \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH ${PATH}:/usr/lib/go-1.23/bin/

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
RUN go env -w GOPROXY=${GOPROXY}

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN make build

FROM ubuntu:22.04

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib/*.so /usr/local/lib

# Install some utilities used for debugging or by startup script
RUN apt-get update && apt-get install -y \
    dnsutils \
    curl \
    git \
    cmake \
    libcurl4-openssl-dev libgomp1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ldconfig

# Debug purpose - run below command to check shared library not found
RUN /mo-service -h

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
