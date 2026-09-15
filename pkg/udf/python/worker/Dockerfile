# run from root path for debug:
# docker build -t mo-python-udf-worker -f ./pkg/udf/python/worker/Dockerfile ./pkg/udf/python/worker/
# docker run -d --network host mo-python-udf-worker

# Keep the worker's IANA database identical to the MatrixOne runtime image.
# The Python and Go base images are allowed to differ in language/runtime
# versions, but an IANA version mismatch would make the capability handshake
# reject the worker or, worse, let an IANA timezone resolve differently on
# the two sides of the boundary.
FROM matrixorigin/ubuntu:22.04 AS matrixone-tzdata

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim-bookworm

COPY --from=matrixone-tzdata /usr/share/zoneinfo/ /usr/share/zoneinfo/

RUN useradd -m -s /bin/bash mo

WORKDIR /home/mo

# only for debug
#RUN cp /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list.d/ali.sources \
#    && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/ali.sources \
#    && apt-get update \
#    && apt-get install -y vim

COPY requirements.txt worker.py watchdog.py ./

RUN pip install --no-cache-dir -r requirements.txt

# Fail the image build if the shared tzdata source was not copied into the
# worker image.  The Go side performs the same version check at capability
# admission; this makes a broken worker image fail at build time as well.
RUN python -c 'from pathlib import Path; import worker; version = Path("/usr/share/zoneinfo/tzdata.zi").read_text(encoding="utf-8").splitlines()[0].removeprefix("# version ").strip(); assert version and version == worker.TIMEZONE_DATABASE_VERSION, (version, worker.TIMEZONE_DATABASE_VERSION)'

USER mo

EXPOSE 50051

CMD [ "python", "-u", "worker.py", "--address=grpc://0.0.0.0:50051" ]
