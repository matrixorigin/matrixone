# Pinned MinIO client for Compose

`mc.RELEASE.2023-10-30T18-43-32Z.linux-amd64.gz` is the gzip-compressed
upstream Linux amd64 binary from
<https://github.com/minio/mc/releases/tag/RELEASE.2023-10-30T18-43-32Z>.
Its decompressed SHA-256 is
`9819284e9387c9e3d553541dec9e6f55fbd258a8f9a2073660b048538a886fdb`.
`Dockerfile.mc` checks that hash during the image build. The upstream license
is in `mc.LICENSE`; upstream publishes source at the release tag above. The
binary reports revision `9f2fb2b6a9f86684cbea0628c5926dafcff7de28` and
`vcs.modified=true`; the exact build-time modifications are not identified by
the release metadata.

Keeping this test dependency in the build context removes the GitHub release
download from Compose image builds. The MinIO server image has a separate
download and is not covered by this asset.
