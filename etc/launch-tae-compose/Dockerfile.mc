FROM matrixorigin/ubuntu@sha256:ef875f006822760d06746979b68e441559e847e804168ab2205e16a4d85bd7c9

ADD https://github.com/minio/mc/releases/download/RELEASE.2023-10-30T18-43-32Z/mc.linux-amd64.RELEASE.2023-10-30T18-43-32Z /usr/bin/mc
RUN echo '9819284e9387c9e3d553541dec9e6f55fbd258a8f9a2073660b048538a886fdb  /usr/bin/mc' | sha256sum -c - \
    && chmod +x /usr/bin/mc

ENTRYPOINT ["/usr/bin/mc"]
