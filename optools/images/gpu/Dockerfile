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
COPY . .

ARG GOPROXY="https://proxy.golang.org,direct"
ENV GOWORK=off
RUN go env -w GOPROXY=${GOPROXY}
RUN module_files="$(sha256sum go.mod go.sum)" && \
    go mod download && \
    test "$module_files" = "$(sha256sum go.mod go.sum)"

RUN cd optools/gpu && pixi run --frozen make -C ../.. MO_CL_CUDA=1 -j8

# Stage user-space libraries from the selected Pixi environment. Only the
# top-level CUDA target library directory is copied: its stubs/ subdirectory
# is link-only and must never enter the runtime image.
RUN mkdir -p /gpu-runtime && \
    cp -a optools/gpu/.pixi/envs/default/lib/*.so* /gpu-runtime/ && \
    cp -a optools/gpu/.pixi/envs/default/targets/x86_64-linux/lib/*.so* /gpu-runtime/ && \
    test -z "$(find /gpu-runtime -maxdepth 1 \
      \( -name 'libcuda.so*' -o -name 'libnvidia-*' \) -print -quit)"

FROM ubuntu:24.04

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
