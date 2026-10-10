# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest

import sirius_campaign_v2 as campaign


def provenance():
    return {"matrixone_revision": "a"*40, "sirius_revision": "b"*40, "sdk_sha256": "c"*64,
            "query_sha256": {f"Q{q}": "d"*64 for q in range(1, 23)},
            "datasets": {"SF1": "e"*64, "SF10": "f"*64}, "hardware": {}, "config": {}}


def output(root, name, values, *, oid=23, nullable=False):
    schema = [{"name": "key", "sql_type": "BIGINT", "nullable": nullable, "nullable_known": True,
               "precision": 0, "scale": 0, "decimal_known": False}]
    bound = [{"oid": oid, "precision": 64, "scale": -1, "nullable": nullable}]
    digest = hashlib.sha256(b"sirius-mo-output-schema-v1\0")
    digest.update(struct.pack("<IiiBI", oid, 64, -1, nullable, 3))
    digest.update(b"key")
    data = b"".join(campaign.canonical([{"type": "int", "value": str(value)}]) + b"\n" for value in values)
    path = root/name
    path.write_bytes(data)
    path.chmod(0o600)
    return {"output_schema": bound, "output_schema_sha256": digest.hexdigest(),
            "result": {"public_schema": schema, "row_count": len(values), "spool": name,
                       "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}}


class CampaignV2Test(unittest.TestCase):
    def test_schedule_counts_and_sampling_separation(self):
        timing = campaign.build_campaign(provenance())
        resources = campaign.build_campaign(provenance(), resource=True)
        self.assertEqual(744, len(timing["schedule"]))
        self.assertEqual(264, len(resources["schedule"]))
        self.assertEqual(440, sum(x["measured"] for x in timing["schedule"]))
        self.assertFalse(any(x["measured"] for x in resources["schedule"]))
        self.assertEqual({"mo-native", "embedded-mo"}, {x["route"] for x in timing["schedule"]})
        self.assertEqual((0, 2, 1, 3), campaign.ORDER_KEYS[2])

    def test_full_value_duplicate_and_empty_comparison(self):
        for a, b, keys, ok in (([], [], [], True), ([1], [1], [], True), ([1, 2, 1], [2, 1, 1], [], True),
                               ([1, 1, 2], [1, 2, 2], [], False), ([1, 2], [2, 1], [0], False)):
            with self.subTest(a=a,b=b), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);root.chmod(0o700)
                left, right=output(root,"a",a),output(root,"b",b)
                scratch=campaign.Scratch(root)
                if ok:
                    campaign.compare_results(left,right,keys,scratch)
                else:
                    with self.assertRaises(campaign.CampaignError):
                        campaign.compare_results(left,right,keys,scratch)
                scratch.close()
                self.assertEqual([],list(root.iterdir()))

    def test_corruption_and_private_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700)
            left,right=output(root,"a",[1]),output(root,"b",[1])
            right["result"]["sha256"]="f"*64
            scratch=campaign.Scratch(root)
            with self.assertRaisesRegex(campaign.CampaignError,"digest"):
                campaign.compare_results(left,right,[],scratch)
            self.assertEqual([],list(root.iterdir()))
            source=output(root,"source",[1])
            for name in ("../source",str(root/"source"),"source/nested"):
                forged=copy.deepcopy(source["result"]);forged["spool"]=name
                with self.assertRaises(campaign.CampaignError):scratch.admit(forged)
            os.symlink(root/"source",root/"link")
            forged=copy.deepcopy(source["result"]);forged["spool"]="link"
            with self.assertRaises(OSError):scratch.admit(forged)
            file=scratch.admit(source["result"]);file.close()
            with self.assertRaises(campaign.CampaignError):scratch.admit(source["result"])
            scratch.close()

    def test_quota_and_schema_physical_width(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700)
            left,right=output(root,"a",[1]),output(root,"b",[1],oid=24)
            scratch=campaign.Scratch(root,limit=1)
            with self.assertRaisesRegex(campaign.CampaignError,"quota"):scratch.admit(left["result"])
            with self.assertRaisesRegex(campaign.CampaignError,"schema"):
                campaign.compare_results(left,right,[],scratch)

    def test_external_runs_preserve_duplicates_and_remove_temporaries(self):
        import unittest.mock
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700)
            left,right=output(root,"a",list(range(30))*2),output(root,"b",list(reversed(range(30)))*2)
            scratch=campaign.Scratch(root)
            with unittest.mock.patch.object(campaign,"CHUNK_BYTES",128):
                campaign.compare_results(left,right,[],scratch)
            self.assertEqual(0,scratch.used)
            self.assertEqual([],list(root.iterdir()))

    def test_bounded_child_pipes_are_not_retained(self):
        executor=campaign.BoundedExecutor([sys.executable,"-c","import sys;sys.stdout.write('x'*2000000)"],5)
        with self.assertRaisesRegex(campaign.CampaignError,"bound"):
            executor({"sequence":1})
        self.assertFalse(campaign.legacy._direct_child_pids())


if __name__ == "__main__":
    unittest.main()
