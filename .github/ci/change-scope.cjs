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

// Ordinary documentation and SDK changes have their own checks; fixtures,
// shared helpers, build/CI configuration and unknown paths retain kernel CI.
function validPath(path) {
  return typeof path === 'string' && !path.split('/').some(p => !p || p === '.' || p === '..');
}

const sdkControlPaths = ['.github/workflows/python-sdk.yml',
  '.github/ci/change-scope.cjs', '.github/workflows/entrypoint.yaml'];
function category(path) {
  if (!validPath(path)) return 'full';
  if (path.startsWith('clients/python/')) return 'sdk';
  if (/(^|\/)(testdata|fixtures)\//i.test(path)) return 'full';
  const extension = path.slice(path.lastIndexOf('.')).toLowerCase();
  if (extension === '.md' || extension === '.rst' ||
      /^docs\/.*\.(png|jpg|jpeg|gif|svg)$/.test(path)) return 'docs';
  if (/^pkg\/.*_test\.go$/.test(path)) return 'ut';
  if (/^test\/distributed\/cases\/.*\.(sql|test|result)$/.test(path)) return 'bvt';
  return 'full';
}

function classify(files, expectedCount, forceFull = false) {
  if (forceFull || !Array.isArray(files) || files.length === 0 ||
      files.length !== expectedCount || files.length >= 3000) return { scope: 'full', sdk_required: true };
  const kinds = new Set();
  const names = new Set();
  let sdkRequired = false;
  for (const file of files) {
    if (!file || !['added', 'modified', 'removed', 'renamed'].includes(file.status) ||
        !validPath(file.filename) || names.has(file.filename)) return { scope: 'full', sdk_required: true };
    names.add(file.filename);
    const paths = [file.filename];
    // Both kernel scope and SDK impact follow both sides of a rename.
    if (file.status === 'renamed') {
      if (!validPath(file.previous_filename)) return { scope: 'full', sdk_required: true };
      paths.push(file.previous_filename);
    }
    for (const path of paths) {
      const kind = category(path);
      kinds.add(kind);
      sdkRequired ||= kind === 'sdk' || sdkControlPaths.includes(path);
    }
  }
  kinds.delete('docs');
  const scope = kinds.size === 0 ? 'docs' :
    kinds.size === 1 ? [...kinds][0] : 'full';
  return { scope, sdk_required: sdkRequired };
}

async function resolveScope({ github, context, forceFull }) {
  const eventPR = context.payload.pull_request;
  if (!eventPR) throw new Error('Missing pull request');
  const params = { ...context.repo, pull_number: eventPR.number };
  const sameRevision = pr => pr.head.sha === eventPR.head.sha && pr.base.sha === eventPR.base.sha;
  const before = (await github.rest.pulls.get(params)).data;
  if (!sameRevision(before)) throw new Error('PR revision changed; use the newer CI run');
  let plan = { scope: 'full', sdk_required: true };
  try {
    const files = await github.paginate(github.rest.pulls.listFiles, { ...params, per_page: 100 });
    plan = classify(files, before.changed_files, forceFull);
  } catch {
    // An unavailable or incomplete file list must never grant a test exemption.
    plan = { scope: 'full', sdk_required: true };
  }
  const after = (await github.rest.pulls.get(params)).data;
  if (!sameRevision(after)) throw new Error('PR revision changed while classifying');
  return plan;
}

function verifyResults(scope, needs) {
  if (needs.preflight?.result !== 'success' ||
      needs.preflight?.outputs?.pr_valid !== 'true') {
    throw new Error('PR validation or scope planning did not succeed');
  }
  const sdkRequired = needs.preflight.outputs.sdk_required;
  if (!['true', 'false'].includes(sdkRequired) ||
      (scope === 'sdk' && sdkRequired !== 'true') ||
      (['docs', 'ut', 'bvt'].includes(scope) && sdkRequired !== 'false')) {
    throw new Error('Missing or inconsistent SDK requirement');
  }
  const required = {
    docs: ['docs-check'],
    sdk: ['python-sdk'],
    ut: ['matrixone-ci'],
    bvt: ['matrixone-compose-ci', 'matrixone-standalone-ci'],
    full: ['matrixone-ci', 'matrixone-ut-coverage',
      'matrixone-compose-ci', 'matrixone-standalone-ci', 'matrixone-coverage-merge'],
  }[scope];
  if (sdkRequired === 'true' && scope === 'full') required.push('python-sdk');
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
    (scope === 'full' ? '' : scope === 'sdk' ?
      ' Kernel coverage was intentionally not evaluated for this SDK-only change.' :
      ' Coverage was not evaluated for this test/document-only change.');
}

// Shared by the analyzer and BVT consumers, after checkout. No API pagination,
// mutable branch comparison, or remembered plan can change the tested revision.
function immutableChanges({ cwd = process.cwd(), env = process.env, git } = {}) {
  const fs = require('node:fs');
  const { execFileSync } = require('node:child_process');
  git ||= args => execFileSync('git', args, {
    cwd, encoding: 'utf8', timeout: 30000, maxBuffer: 8 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if (env.GITHUB_ACTIONS !== 'true' || !env.GITHUB_EVENT_PATH ||
      !['pull_request', 'pull_request_target'].includes(env.GITHUB_EVENT_NAME)) return null;
  try {
    const pr = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8')).pull_request;
    const base = pr?.base?.sha;
    const head = pr?.head?.sha;
    if (![base, head].every(sha => /^[0-9a-f]{40}$/.test(sha || '')) ||
        git(['rev-parse', 'HEAD']).trim() !== head) return null;
    // Local edits cannot use a committed-input exemption. Include untracked
    // inputs too; ignored native outputs and runner reports are not source.
    if (git(['status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none']).trim()) return null;
    try {
      git(['cat-file', '-e', `${base}^{commit}`]);
    } catch {
      // Fetch the immutable object only. Do not move a checkout or local ref.
      if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY || '')) return null;
      git(['fetch', '--no-tags', '--depth=1',
        `https://github.com/${env.GITHUB_REPOSITORY}.git`, base]);
    }
    // Tree comparison also works when a shallow checkout lacks ancestry. Both
    // sides of a rename are roots; do not hide removals behind rename matching.
    const raw = git(['diff', '--no-ext-diff', '--no-renames', '--ignore-submodules=none', '--name-status', '-z', base, head]);
    const fields = raw.split('\0');
    if (fields.pop() !== '' || fields.length === 0 || fields.length % 2) return null;
    const files = [];
    for (let i = 0; i < fields.length; i += 2) {
      if (!['A', 'M', 'D', 'T'].includes(fields[i]) || !validPath(fields[i + 1])) return null;
      files.push({ status: fields[i], filename: fields[i + 1] });
    }
    return files;
  } catch {
    return null;
  }
}

module.exports = { category, classify, resolveScope, verifyResults, immutableChanges };
