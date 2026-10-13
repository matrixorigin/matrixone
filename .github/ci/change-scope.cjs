// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
'use strict';

// This allowlist is deliberately narrow: test helpers, fixtures, configuration,
// dependencies, CI code and unknown paths all retain the complete test suite.
function category(path) {
  if (typeof path !== 'string' || path.split('/').some(p => !p || p === '.' || p === '..')) return 'full';
  if (/^(README(?:_CN)?|CONTRIBUTING|CHANGELOG)\.md$/.test(path) ||
      /^docs\/.*\.(md|png|jpg|jpeg|gif|svg)$/.test(path)) return 'docs';
  if (/^pkg\/.*_test\.go$/.test(path)) return 'ut';
  if (/^test\/distributed\/cases\/.*\.(sql|test|result)$/.test(path)) return 'bvt';
  return 'full';
}

function classify(files, expectedCount, forceFull = false) {
  if (forceFull || !Array.isArray(files) || files.length === 0 ||
      files.length !== expectedCount || files.length >= 3000) return 'full';
  const kinds = new Set();
  const names = new Set();
  for (const file of files) {
    if (!file || !['added', 'modified', 'removed', 'renamed'].includes(file.status) ||
        typeof file.filename !== 'string' || names.has(file.filename)) return 'full';
    names.add(file.filename);
    kinds.add(category(file.filename));
    // Moving production code into a test/document path must still run full CI.
    if (file.status === 'renamed') {
      if (!file.previous_filename) return 'full';
      kinds.add(category(file.previous_filename));
    }
  }
  if (kinds.has('full')) return 'full';
  kinds.delete('docs');
  if (kinds.size === 0) return 'docs';
  return kinds.size === 1 ? [...kinds][0] : 'full';
}

async function resolveScope({ github, context, forceFull }) {
  const eventPR = context.payload.pull_request;
  if (!eventPR) throw new Error('Missing pull request');
  const params = { ...context.repo, pull_number: eventPR.number };
  const sameRevision = pr => pr.head.sha === eventPR.head.sha && pr.base.sha === eventPR.base.sha;
  const before = (await github.rest.pulls.get(params)).data;
  if (!sameRevision(before)) throw new Error('PR revision changed; use the newer CI run');
  let scope = 'full';
  try {
    const files = await github.paginate(github.rest.pulls.listFiles, { ...params, per_page: 100 });
    scope = classify(files, before.changed_files, forceFull);
  } catch {
    // An unavailable or incomplete file list must never grant a test exemption.
    scope = 'full';
  }
  const after = (await github.rest.pulls.get(params)).data;
  if (!sameRevision(after)) throw new Error('PR revision changed while classifying');
  return scope;
}

function verifyResults(scope, needs) {
  if (needs.preflight?.result !== 'success' ||
      needs.preflight?.outputs?.pr_valid !== 'true') {
    throw new Error('PR validation or scope planning did not succeed');
  }
  const required = {
    docs: ['docs-check'],
    ut: ['matrixone-ci'],
    bvt: ['matrixone-compose-ci', 'matrixone-standalone-ci'],
    full: ['matrixone-ci', 'matrixone-ut-coverage',
      'matrixone-compose-ci', 'matrixone-standalone-ci', 'matrixone-coverage-merge'],
  }[scope];
  if (!required) throw new Error(`Invalid CI scope: ${scope}`);
  if (scope === 'full' || scope === 'bvt') {
    const { compose_group, launch_group, generation } = needs.preflight.outputs;
    if (!['0', '1'].includes(compose_group) || !['0', '1'].includes(launch_group) ||
        compose_group === launch_group || !/^[1-9][0-9]*-[1-9][0-9]*$/.test(generation || '')) {
      throw new Error('Missing or invalid complementary BVT plan');
    }
  }
  for (const job of required) {
    if (needs[job]?.result !== 'success') throw new Error(`${job}: ${needs[job]?.result || 'missing'}`);
  }
  if (required.includes('matrixone-ut-coverage') &&
      needs['matrixone-ut-coverage']?.outputs?.coverage_ready !== 'true') {
    throw new Error('UT coverage was not eligible to execute');
  }
  return `CI scope ${scope}: all required jobs passed; other suites intentionally omitted.` +
    (scope === 'full' ? '' : ' Coverage was not evaluated for this test/document-only change.');
}

module.exports = { category, classify, resolveScope, verifyResults };
