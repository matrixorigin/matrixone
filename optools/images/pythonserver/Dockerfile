# run from root path for debug:
# docker build -t mo-python-udf-server -f ./optools/images/pythonserver/Dockerfile ./pkg/udf/pythonservice/pyserver/
# docker run -d --network host mo-python-udf-server

FROM python:3.9-bookworm

RUN useradd -m -s /bin/bash mo

WORKDIR /home/mo

# only for debug
#RUN cp /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list.d/ali.sources \
#    && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/ali.sources \
#    && apt-get update \
#    && apt-get install -y vim

COPY . .

RUN pip install -r requirements/base.txt
RUN pip install -r requirements/pytorch.txt

USER mo

EXPOSE 50051

CMD [ "/bin/bash", "-c", "python -u server.py >> server.log 2>&1" ]