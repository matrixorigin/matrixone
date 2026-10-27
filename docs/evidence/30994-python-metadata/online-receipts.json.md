# `online-receipts.json`

Artifact: {"name":"online-receipts.json","bytes":1156,"sha256":"5e480a785acfbcdd59d5edf6d5933caa5fe37ee4c21fea1a3f269635f7466d53","encoding":"utf-8","stored_bytes":1156}

````json
[
  {
    "name": "online",
    "command": [
      "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python",
      "-m",
      "pytest",
      "tests/online/test_metadata_online.py",
      "-o",
      "addopts=",
      "-vv",
      "--tb=short",
      "--junitxml=/Users/xupeng/.codex/issue-to-pr/matrixone/30994/online.xml"
    ],
    "cwd": "/private/tmp/mo-pr30994/clients/python",
    "started_epoch": 1792805740.304972,
    "finished_epoch": 1792805744.8987331,
    "exit": 0,
    "source_head": "ea208d535537e6adb5307500e2d1b606fee39aae",
    "log_sha256": "4420f03929762f4f9b627806784827b914d331ce7846dcdb99ebd84fdbc913a9"
  },
  {
    "name": "async-challenge",
    "command": [
      "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python",
      "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/async_challenge.py"
    ],
    "cwd": "/private/tmp/mo-pr30994/clients/python",
    "started_epoch": 1792805744.955445,
    "finished_epoch": 1792805745.349516,
    "exit": 0,
    "source_head": "ea208d535537e6adb5307500e2d1b606fee39aae",
    "log_sha256": "967a1f38267c871b67f2b7f2c05a24cecf49a0900122414fe6e03a738b363b1e"
  }
]
````
