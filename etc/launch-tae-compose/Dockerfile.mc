FROM matrixorigin/ubuntu@sha256:ef875f006822760d06746979b68e441559e847e804168ab2205e16a4d85bd7c9 AS unpack

COPY pinned-mc/mc.RELEASE.2023-10-30T18-43-32Z.linux-amd64.gz /tmp/mc.gz
RUN gzip -dc /tmp/mc.gz > /usr/bin/mc \
    && echo '9819284e9387c9e3d553541dec9e6f55fbd258a8f9a2073660b048538a886fdb  /usr/bin/mc' | sha256sum -c - \
    && chmod +x /usr/bin/mc

FROM matrixorigin/ubuntu@sha256:ef875f006822760d06746979b68e441559e847e804168ab2205e16a4d85bd7c9
COPY --from=unpack /usr/bin/mc /usr/bin/mc
COPY pinned-mc/mc.LICENSE pinned-mc/README.md /usr/share/doc/mc/

ENTRYPOINT ["/usr/bin/mc"]
