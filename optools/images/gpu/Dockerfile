FROM golang:1.26.4-bookworm AS builder

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl git unzip wget && \
    rm -rf /var/lib/apt/lists/*

# Pixi is the only CUDA/RAPIDS build provider. Pin the installer result; the
# checked-in pixi.lock pins the environment installed by --frozen below.
ENV PIXI_HOME=/opt/pixi
ENV PATH="${PIXI_HOME}/bin:${PATH}"
RUN curl -fsSL https://pixi.sh/install.sh -o /tmp/pixi-install.sh && \
    PIXI_VERSION=v0.81.0 PIXI_HOME=${PIXI_HOME} PIXI_NO_PATH_UPDATE=1 \
    sh /tmp/pixi-install.sh && rm /tmp/pixi-install.sh

WORKDIR /matrixone
COPY go.mod go.sum ./

ARG GOPROXY="https://proxy.golang.org,direct"
ENV GOWORK=off
RUN go env -w GOPROXY=${GOPROXY}
RUN module_files="$(sha256sum go.mod go.sum)" && \
    go mod download && \
    test "$module_files" = "$(sha256sum go.mod go.sum)"

COPY . .

# Rootless container builds cannot restore archive owners outside their UID map.
# Pipe jobserver tokens also work with CMake's older system gmake.
RUN cd optools/gpu && pixi run --frozen env TAR_OPTIONS=--no-same-owner \
    make --jobserver-style=pipe -C ../.. MO_CL_CUDA=1 -j8

# Stage user-space libraries without dangling cross-directory Pixi symlinks or
# silent basename collisions. Driver stubs are link-only and never packaged.
RUN sh optools/images/gpu/stage-runtime-libs.sh \
    optools/gpu/.pixi/envs/default /gpu-runtime

FROM ubuntu:24.04

# Pixi's Tk library depends on the OS X11 client library. Keep its closure in
# the final image without copying the Pixi build prefix or any driver stub.
RUN apt-get update && apt-get install -y --no-install-recommends libx11-6 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /matrixone/mo-service /mo-service
COPY --from=builder /matrixone/mocl_kernel64.fatbin /mocl_kernel64.fatbin
COPY --from=builder /matrixone/etc /etc
COPY --from=builder /matrixone/thirdparties/install/lib/*.so /usr/local/lib/
COPY --from=builder /matrixone/cgo/*.so /usr/local/lib/
COPY --from=builder /matrixone/pkg/monlp/tokenizer/dict /usr/local/share/jieba
COPY --from=builder /gpu-runtime/ /usr/local/lib/

ENV MO_JIEBA_DICT_DIR=/usr/local/share/jieba
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /
EXPOSE 6001
ENTRYPOINT ["/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
