# `server-build-receipt.json`

Artifact: {"name":"server-build-receipt.json","bytes":631,"sha256":"d117d4defd8610de82352c4cbd17b5faa9f33e1168c84c09522df3b634934b38","encoding":"utf-8","stored_bytes":631}

````json
{
  "variant": "baseline",
  "head": "94faf0209b9ac3254d31906f92e34405f1988203",
  "command": [
    "make",
    "build-with-prebuilt-native",
    "GOBUILD_OPT=-p=1 -gcflags=github.com/matrixorigin/matrixone/...=-dwarf=false",
    "GOLDFLAGS=-ldflags=\"-w -extldflags '-L$(CGO_DIR) -lmo -L$(THIRDPARTIES_INSTALL_DIR)/lib -Wl,-rpath,@executable_path/lib' $(VERSION_INFO)\""
  ],
  "cwd": "/private/tmp/mo-issue30995",
  "started_epoch": 1792802246.2204108,
  "finished_epoch": 1792802339.925267,
  "exit": 0,
  "source_clean_before_build": true,
  "binary_sha256": "bbd27b9a8a693a8ab0543e9473a5d1eaea6cffeddd5307bf3f311a58ed34773a"
}
````
