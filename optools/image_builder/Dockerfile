# STAGE 1: building the executable
FROM centos:centos7 AS builder
 
# add a user here because addgroup and adduser are not available in scratch
RUN export http_proxy="http://172.27.0.17:18080" \
    && export https_proxy="http://172.27.0.17:18080" \
    && echo '185.199.108.133 raw.githubusercontent.com' >> /etc/hosts \
    && echo '140.82.114.3 github.com' >> /etc/hosts \
	&& yum install -y make gcc sudo git \
	&& curl -o /tmp/go1.17.linux-amd64.tar.gz -L https://golang.org/dl/go1.17.linux-amd64.tar.gz \
	&& tar -C /usr/local -xzf /tmp/go1.17.linux-amd64.tar.gz \
	&& echo 'export PATH="$PATH:/usr/local/go/bin"' >> /etc/profile \
	&& source /etc/profile \
	&& go version \
	&& git clone -b main https://github.com/matrixorigin/matrixone.git \
	&& cd matrixone \
	&& make config \
	&& make build \
	&& sed -i 's/host = "localhost"/host = "0.0.0.0"/' system_vars_config.toml \
	&& mkdir -p /usr/local/matrixone \
	&& cp mo-server /usr/local/matrixone/ \
	&& cp system_vars_config.toml /usr/local/matrixone/
WORKDIR /usr/local/matrixone/
COPY entrypoint.sh .

# STAGE 2: build a small image
FROM centos:centos7
RUN mkdir /opt/matrixone \
    && yum install -y cronie \
	&& yum clean all 
WORKDIR /opt/matrixone
COPY --from=builder /usr/local/matrixone/ ./
ENTRYPOINT ["./entrypoint.sh"]

