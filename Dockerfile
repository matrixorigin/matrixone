FROM --platform=$BUILDPLATFORM golang:1.19-buster as builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY . .

RUN cd cgo && make
RUN make clean && make config && make build

FROM pingcap/alpine-glibc

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service

WORKDIR /

ENTRYPOINT [ "/mo-service"]
