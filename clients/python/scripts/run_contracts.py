#!/usr/bin/env python3
"""Run a nonempty SDK contract selection and record reproducible provenance.

No credentials are written. Explicit server jobs fail on skips or wrong identity;
pytest's green status alone cannot certify an unconfigured integration target.
"""

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', choices=('4.2.5', 'current-source'))
    parser.add_argument('--output', default='contract-results')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.server:
        required = [
            'MO_HOST',
            'MO_PORT',
            'MO_USER',
            'MO_PASSWORD',
            'MO_SDK_EXPECT_VERSION',
            'MO_SDK_EXPORT_DIR',
            'MO_SDK_EXPORT_LOCAL_DIR',
        ]
        if args.server == 'current-source':
            required.append('MO_SDK_EXPECT_REVISION')
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            parser.error('Required target configuration: ' + ', '.join(missing))
        if args.server == '4.2.5' and os.environ['MO_SDK_EXPECT_VERSION'] != '4.2.5':
            parser.error('The release job must assert version 4.2.5')
        os.environ['MO_SDK_ONLINE'] = '1'
        selection = [
            'tests/online/test_sdk_quality_contracts.py',
            'tests/online/test_sdk_manager_contracts.py',
            'tests/online/test_sdk_search_contracts.py',
            'tests/online/test_sdk_wave2_contracts.py',
        ]
    else:
        selection = ['tests/offline', 'tests/contracts']
    versions = {}
    for package in ('matrixone-python-sdk', 'SQLAlchemy', 'PyMySQL', 'aiomysql', 'pytest', 'pyarrow'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    command = [
        sys.executable,
        '-m',
        'pytest',
        *selection,
        '-o',
        'addopts=',
        '-q',
        '--tb=short',
        '--junitxml=' + str(output / 'junit.xml'),
    ]
    record = dict(
        revision=revision,
        python=sys.version,
        dependencies=versions,
        selection=selection,
        server=args.server,
        expected_version=os.environ.get('MO_SDK_EXPECT_VERSION'),
        expected_revision=os.environ.get('MO_SDK_EXPECT_REVISION'),
        command=command,
    )
    result = subprocess.run(command, cwd=root)
    record['exit_code'] = result.returncode
    path = output / 'junit.xml'
    if path.exists():
        cases = list(ET.parse(path).getroot().iter('testcase'))
        counts = {name: sum(case.find(name) is not None for case in cases) for name in ('failure', 'error', 'skipped')}
        counts['tests'] = len(cases)
        record['counts'] = counts
        valid = counts['tests'] > 0 and not any(counts[name] for name in ('failure', 'error', 'skipped'))
    else:
        valid = False
    (output / 'provenance.json').write_text(json.dumps(record, indent=2) + '\n')
    return result.returncode or (0 if valid else 1)


if __name__ == '__main__':
    sys.exit(main())
