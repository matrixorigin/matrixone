FROM nvidia/cuda:13.0.2-cudnn-devel-ubuntu24.04 AS builder
RUN apt-get update
RUN apt-get install -y locales
RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG=en_US.utf8
RUN export LANG=en_US.utf8

ARG DEBIAN_FRONTEND=noninteractive
ENV MOHOME=/app/matrixone
ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/cuda/lib64/stubs:${MOHOME}/thirdparties/install/lib:${LD_LIBRARY_PATH}"

WORKDIR /app
COPY matrixone matrixone
COPY cuvs cuvs
COPY go.work .
COPY go.work.sum .

##################################################
#           install dependecies                  #
##################################################
RUN apt-get update && apt-get install -y wget git build-essential nano unzip curl 
RUN apt-get update && apt-get install -y g++

##################################################
#           install conda                        #
##################################################
ENV CONDA_PLUGINS_AUTO_ACCEPT_TOS=true

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
RUN bash miniconda.sh -b -p /root/miniconda
RUN rm miniconda.sh
ENV PATH="/root/miniconda/bin:${PATH}"
RUN /root/miniconda/bin/conda init bash

RUN /root/miniconda/bin/conda env create --name go -f /app/matrixone/optools/images/gpu/go_cuda-130_arch-x86_64.yaml
RUN echo "source activate go" >> ~/.bashrc

ENV CONDA_PREFIX=/root/miniconda/envs/go
ENV PATH="${CONDA_PREFIX}/bin:${PATH}"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${CONDA_PREFIX}/lib"

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
RUN go env -w GOPROXY=${GOPROXY}

RUN cd matrixone && go mod download

ENV MO_CL_CUDA=1
RUN cd matrixone && make clean && make

#ENTRYPOINT ["bash"]

FROM nvidia/cuda:13.0.2-cudnn-runtime-ubuntu24.04

COPY --from=builder /app/matrixone/mo-service /mo-service
COPY --from=builder /app/matrixone/etc /etc
COPY --from=builder /app/matrixone/thirdparties/install/lib/*.so /usr/local/lib
COPY --from=builder /root/miniconda/envs/go/lib /root/miniconda/envs/go/lib
#COPY --from=builder /root/.bashrc /root/

ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/cuda/lib64:/usr/local/cuda/lib64/stubs:${LD_LIBRARY_PATH}"
ENV CONDA_PREFIX=/root/miniconda/envs/go
ENV PATH="${CONDA_PREFIX}/bin:${PATH}"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${CONDA_PREFIX}/lib"


# ldconfig and run mo-service to check if the shared library is found
# disable the check because build don't have gpu runtime access
# RUN /bin/bash -c ldconfig && /mo-service -h

WORKDIR /

EXPOSE 6001

#ENTRYPOINT ["bash"]
ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]

