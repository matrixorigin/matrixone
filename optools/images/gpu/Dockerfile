FROM nvidia/cuda:13.0.2-cudnn-devel-ubuntu24.04
RUN apt-get update
RUN apt-get install -y locales
RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG=en_US.utf8
RUN export LANG=en_US.utf8

ENV DEBIAN_FRONTEND=noninteractive
ENV MOHOME=/app/matrixone
ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/cuda/lib64/stubs:${MOHOME}/thirdparties/install/lib:${LD_LIBRARY_PATH}"

RUN chsh -s /bin/bash

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
#RUN echo 'export PATH="/root/miniconda/bin:${PATH}"' >> ~/.bashrc
#RUN /root/miniconda/bin/conda config --set auto_activate_base true
RUN /root/miniconda/bin/conda init bash

RUN /root/miniconda/bin/conda env create --name go -f /app/matrixone/optools/images/gpu/go_cuda-130_arch-x86_64.yaml
RUN echo "source activate go" >> ~/.bashrc

ENV CONDA_PREFIX=/root/miniconda/envs/go
ENV PATH="${CONDA_PREFIX}/bin:${PATH}"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${CONDA_PREFIX}/lib"

RUN echo $CONDA_PREFIX

ENV MO_CL_CUDA=1
RUN cd matrixone && make clean && make

#RUN /miniconda/bin/conda
ENTRYPOINT ["bash"]
