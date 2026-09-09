# run from root path for debug:
# docker build -t mo-python-udf-runtime -f ./optools/images/pythonserver/Dockerfile ./pkg/udf/pythonruntime/pyserver/
# docker run -d --network host mo-python-udf-runtime

FROM python:3.12-bookworm

RUN useradd -m -s /bin/bash mo

WORKDIR /home/mo

# only for debug
#RUN cp /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list.d/ali.sources \
#    && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/ali.sources \
#    && apt-get update \
#    && apt-get install -y vim

COPY requirements.txt worker.py ./

RUN pip install --no-cache-dir -r requirements.txt

USER mo

EXPOSE 50051

CMD [ "python", "-u", "worker.py", "--address=grpc://0.0.0.0:50051" ]
