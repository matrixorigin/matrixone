# `checks-receipt.json`

Artifact: {"name":"checks-receipt.json","bytes":1806,"sha256":"b1dcf3248ad186044603ac463676f76bcab643ed2e904db072b0f92f48283f64","encoding":"utf-8","stored_bytes":1806}

````json
{
  "source_head": "ea208d535537e6adb5307500e2d1b606fee39aae",
  "source_identity_sha256": "3e5df95204ae99e2a15faba6a959adc0f28160712433d6b8536b53bdcf7deeb7",
  "cwd": "/private/tmp/mo-pr30994/clients/python",
  "offline": {
    "command": [
      "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python",
      "-m",
      "pytest",
      "tests/offline/test_metadata_offline.py",
      "-o",
      "addopts=",
      "-vv",
      "--tb=short",
      "--junitxml=/Users/xupeng/.codex/issue-to-pr/matrixone/30994/offline.xml"
    ],
    "exit": 0,
    "passed": 26,
    "log_sha256": "d833854c7df9316647af7a3da233478d2cdacd25d07573297e64db7fa4576cc0"
  },
  "static": {
    "commands": [
      [
        "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python",
        "-m",
        "flake8",
        "matrixone/metadata.py",
        "tests/offline/test_metadata_offline.py",
        "tests/online/test_metadata_online.py",
        "--select=E9,F63,F7,F82",
        "--show-source",
        "--statistics"
      ],
      [
        "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python",
        "-m",
        "py_compile",
        "matrixone/metadata.py",
        "tests/offline/test_metadata_offline.py",
        "tests/online/test_metadata_online.py"
      ]
    ],
    "exit_codes": [
      0,
      0
    ],
    "scope": "complete delivery Python delta; shared metadata helper and both owning test files; no imported interface or dependency changes"
  },
  "race": {
    "applicable": false,
    "reason": "Python-only result aggregation, no Go source or new shared mutable state. No repository-supported CPython race detector; retained bounded async gather, independent sessions, propagated errors and zero checked-out connections validate the actual async path."
  }
}
````
