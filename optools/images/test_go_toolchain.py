#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

"""Keep source-build Docker toolchains compatible with the module minimum."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


class GoToolchainTest(unittest.TestCase):
    def test_source_builds_install_module_toolchain_before_download(self):
        version = re.search(r"^go ([0-9.]+)$", (ROOT / "go.mod").read_text(), re.M).group(1)
        for name in ("Dockerfile", "Dockerfile.ci"):
            with self.subTest(dockerfile=name):
                content = (ROOT / "optools/images" / name).read_text()
                self.assertIn(f"FROM golang:{version}-bookworm AS go-toolchain", content)
                install = "COPY --from=go-toolchain /usr/local/go /usr/local/go"
                self.assertIn(install, content)
                self.assertLess(content.index(install), content.index("go mod download"))
                # COPY merges directories; remove the old GOROOT first so a
                # patch update cannot retain files deleted by the new release.
                self.assertIn("RUN rm -rf /usr/local/go\n" + install, content)
                self.assertIn("GOTOOLCHAIN=local", content)


if __name__ == "__main__":
    unittest.main()
