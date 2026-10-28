#!/usr/bin/env python3
"""Run a nonempty SDK contract selection and record reproducible provenance.

No credentials are written. Explicit server jobs fail on skips or wrong identity;
pytest's green status alone cannot certify an unconfigured integration target.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

FULL_SELECTION = [
    'tests/online/test_sdk_quality_contracts.py',
    'tests/online/test_sdk_manager_contracts.py',
    'tests/online/test_sdk_search_contracts.py',
    'tests/online/test_sdk_wave2_contracts.py',
    'tests/online/test_sdk_release_contracts.py',
]
CORE_SELECTION = [
    'tests/online/test_sdk_quality_contracts.py::test_public_query_contracts',
    'tests/online/test_sdk_quality_contracts.py::test_async_public_contracts_and_pre_ping',
    'tests/online/test_sdk_quality_contracts.py::test_type_metadata_and_values',
    'tests/online/test_sdk_quality_contracts.py::test_capabilities_are_observations',
    'tests/online/test_sdk_quality_contracts.py::test_decimal_binding_preserves_exact_mutation_scope',
    'tests/online/test_sdk_search_contracts.py::test_vector_metric_filters_and_live_schema',
    'tests/online/test_sdk_release_contracts.py::test_cached_snapshot_select_public_owners',
    'tests/online/test_sdk_release_contracts.py::test_external_cache_warmed_before_adoption',
    'tests/online/test_sdk_release_contracts.py::test_admin_name_and_password_roundtrip',
    'tests/online/test_sdk_release_contracts.py::test_native_metadata_boolean_matches_structured',
    'tests/online/test_sdk_release_contracts.py::test_fulltext_zero_limit_native_results',
    'tests/online/test_sdk_release_contracts.py::test_clone_verified_success_rolls_back_with_later_error',
    'tests/online/test_sdk_release_contracts.py::test_clone_existing_target_is_noop',
    'tests/online/test_sdk_release_contracts.py::test_clone_snapshot_convenience_single_connection_pool',
    'tests/online/test_sdk_release_contracts.py::test_query_failure_retains_source_and_pool_reuse',
    'tests/online/test_sdk_release_contracts.py::test_stage_comment_native_outcome_is_preserved',
]


def release_target(value):
    if value == 'current-source' or re.fullmatch(r'(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)', value):
        return value
    raise argparse.ArgumentTypeError('Expected an exact release (for example 4.1.6) or current-source')


def validate_server_identity(version, revision, expected_version, expected_revision=None):
    """Compare actual release identities exactly; source banners need a revision."""
    if re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', expected_version):
        match = re.search(r'(?:^|-)MatrixOne-v([0-9]+\.[0-9]+\.[0-9]+)(?:$|[-+])', version)
        if match is None or match[1] != expected_version:
            raise ValueError('Observed MatrixOne release differs from the selected target')
    elif not expected_revision or expected_version not in version:
        raise ValueError('Source identity requires the expected banner and git revision')
    if expected_revision and not revision.startswith(expected_revision):
        raise ValueError('Observed MatrixOne git revision differs from the selected target')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=release_target)
    parser.add_argument('--suite', choices=('full', 'core'), default='full')
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
        ]
        if args.suite == 'full':
            required.extend(('MO_SDK_EXPORT_DIR', 'MO_SDK_EXPORT_LOCAL_DIR'))
            if os.environ.get('MO_SDK_DISPOSABLE_SERVER') != '1':
                parser.error('Full server contracts require MO_SDK_DISPOSABLE_SERVER=1 and exclusive tenant access')
        if args.server == 'current-source':
            required.append('MO_SDK_EXPECT_REVISION')
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            parser.error('Required target configuration: ' + ', '.join(missing))
        if args.server != 'current-source' and os.environ['MO_SDK_EXPECT_VERSION'] != args.server:
            parser.error('The release job must assert the exact selected version')
        os.environ['MO_SDK_ONLINE'] = '1'
        os.environ['MO_SDK_TARGET'] = args.server
        selection = FULL_SELECTION if args.suite == 'full' else CORE_SELECTION
    else:
        if args.suite != 'full':
            parser.error('--suite core requires an explicit server target')
        selection = ['tests/offline']
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
        '-o',
        'junit_family=legacy',
        '-q',
        '--tb=short',
        '--junitxml=' + str(output / 'junit.xml'),
    ]

    def source_manifest():
        # HEAD alone does not identify an editable checkout. Include dirty
        # source and fixtures, excluding caches and generated reports.
        inputs = set((root / 'matrixone').rglob('*.py'))
        inputs.update((root / 'tests').rglob('*.py'))
        inputs.update(root / name for name in ('scripts/run_contracts.py', 'pyproject.toml', 'pytest.ini', 'setup.py'))
        return {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(inputs)
            if path.is_file()
        }

    source_sha256 = source_manifest()
    record = dict(
        source_sha256=source_sha256,
        worktree_status=subprocess.check_output(
            ['git', 'status', '--porcelain', '--untracked-files=all', '--', '.'], cwd=root, text=True
        ).splitlines(),
        revision=revision,
        python=sys.version,
        dependencies=versions,
        selection=selection,
        server=args.server,
        suite=args.suite,
        expected_version=os.environ.get('MO_SDK_EXPECT_VERSION'),
        expected_revision=os.environ.get('MO_SDK_EXPECT_REVISION'),
        command=command,
    )
    result = subprocess.run(command, cwd=root)
    record['exit_code'] = result.returncode
    record['source_unchanged'] = source_sha256 == source_manifest()
    path = output / 'junit.xml'
    if path.exists():
        cases = list(ET.parse(path).getroot().iter('testcase'))
        counts = {name: sum(case.find(name) is not None for case in cases) for name in ('failure', 'error', 'skipped')}
        counts['tests'] = len(cases)
        record['counts'] = counts
        record['observed_server'] = {
            item.attrib['name']: item.attrib['value']
            for item in ET.parse(path).getroot().iter('property')
            if item.attrib.get('name') in ('matrixone.version', 'matrixone.revision')
        }
        record['feature_outcomes'] = [
            dict(case=case.attrib.get('name'), feature=item.attrib['name'], outcome=item.attrib['value'])
            for case in cases
            for item in case.iter('property')
            if item.attrib.get('name', '').startswith('matrixone.feature.')
        ]
        valid = counts['tests'] > 0 and not any(counts[name] for name in ('failure', 'error', 'skipped'))
    else:
        valid = False
    (output / 'provenance.json').write_text(json.dumps(record, indent=2) + '\n')
    return result.returncode or (0 if valid and record['source_unchanged'] else 1)


if __name__ == '__main__':
    sys.exit(main())
