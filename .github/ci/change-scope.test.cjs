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
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { cpSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } = require('node:fs');
const { spawnSync } = require('node:child_process');
const { tmpdir } = require('node:os');
const { join } = require('node:path');
const { runInNewContext } = require('node:vm');
const { classify, resolveScope, verifyResults } = require('./change-scope.cjs');

const modified = filename => ({ filename, status: 'modified' });
test('only narrow test and documentation paths receive exemptions', () => {
  for (const [paths, expected] of [
    [['README.md', 'docs/design/a.md'], 'docs'],
    [['pkg/sql/plan/a_test.go'], 'ut'],
    [['pkg/tests/upgrade/a_test.go', 'docs/a.md'], 'ut'],
    [['test/distributed/cases/a.test', 'test/distributed/cases/a.result'], 'bvt'],
    [['test/distributed/cases/a.sql', 'README_CN.md'], 'bvt'],
    [['pkg/a_test.go', 'test/distributed/cases/a.test'], 'full'],
    [['pkg/a_test.go', 'pkg/a.go'], 'full'],
    [['pkg/tests/testutils/helper.go'], 'full'],
    [['pkg/embed/testing.go'], 'full'],
    [['pkg/a/testdata/input.json'], 'full'],
    [['go.mod'], 'full'], [['Makefile'], 'full'],
    [['.github/workflows/entrypoint.yaml'], 'full'],
    [['test/distributed/run.sh'], 'full'],
    [['docs/example.sh'], 'full'], [['etc/cn.toml'], 'full'],
    [['pkg/../main_test.go'], 'full'],
  ]) assert.equal(classify(paths.map(modified), paths.length), expected, paths.join(','));
});

test('renames consider the old path, including production-to-doc and production-to-test', () => {
  for (const filename of ['docs/a.md', 'pkg/a_test.go']) {
    assert.equal(classify([{ filename, previous_filename: 'pkg/a.go', status: 'renamed' }], 1), 'full');
  }
  assert.equal(classify([{ filename: 'pkg/b_test.go', previous_filename: 'pkg/a_test.go', status: 'renamed' }], 1), 'ut');
  assert.equal(classify([{ filename: 'pkg/a_test.go', status: 'removed' }], 1), 'ut');
});

test('empty, capped, incomplete, duplicate and malformed lists fail closed', () => {
  const file = modified('pkg/a_test.go');
  for (const [files, count] of [
    [[], 0], [[file], 2], [[file, file], 2], [null, 1],
    [[{ ...file, status: 'renamed' }], 1], [[{ ...file, status: 'unknown' }], 1],
    [[null], 1], [[modified(null)], 1],
    [Array.from({ length: 3000 }, (_, i) => modified(`pkg/a${i}_test.go`)), 3000],
  ]) assert.equal(classify(files, count), 'full');
  assert.equal(classify([file], 1, true), 'full');
});

function client({ files = [modified('pkg/a_test.go')], revisions, error = false } = {}) {
  const pr = { number: 1, head: { sha: 'head' }, base: { sha: 'base' }, changed_files: 1 };
  let calls = 0;
  return {
    context: { repo: { owner: 'owner', repo: 'repo' }, payload: { pull_request: pr } },
    github: {
      rest: { pulls: { get: async () => ({ data: revisions ? revisions[calls++] : pr }), listFiles: 'listFiles' } },
      paginate: async (method, params) => {
        assert.equal(method, 'listFiles');
        assert.equal(params.per_page, 100);
        if (error) throw new Error('API unavailable');
        return files;
      },
    },
    pr,
  };
}

test('API failure falls back to full and both sides of pagination check the revision', async () => {
  assert.equal(await resolveScope(client()), 'ut');
  assert.equal(await resolveScope(client({ error: true })), 'full');
  const { pr } = client();
  const movedHead = { ...pr, head: { sha: 'new' } };
  const movedBase = { ...pr, base: { sha: 'new' } };
  for (const revisions of [[movedHead], [pr, movedHead], [pr, movedBase]]) {
    await assert.rejects(resolveScope(client({ revisions })), /revision changed/);
  }
});

function results() {
  const needs = Object.fromEntries(['change-scope', 'docs-check', 'bvt-group-plan', 'matrixone-ci',
    'matrixone-ut-coverage', 'matrixone-compose-ci', 'matrixone-standalone-ci',
    'matrixone-coverage-merge'].map(job => [job, { result: 'success' }]));
  needs['check-pr-valid'] = { result: 'success', outputs: { pr_valid: 'true' } };
  needs['matrixone-ut-coverage'].outputs = { coverage_ready: 'true' };
  return needs;
}

function entrypointJobs() {
  const workflow = readFileSync(`${__dirname}/../workflows/entrypoint.yaml`, 'utf8');
  return Object.fromEntries([...workflow.matchAll(/^  ([\w-]+):\n([\s\S]*?)(?=^  [\w-]+:\n|$(?![\s\S]))/gm)]
    .map(match => [match[1], match[2]]));
}

function checkoutSpec(block) {
  const lines = block.split('\n');
  const checkoutIndex = lines.findIndex(line => /^      - uses: actions\/checkout@/.test(line));
  assert.notEqual(checkoutIndex, -1, 'job must contain the trusted checkout');
  assert.equal(lines[checkoutIndex + 1].trim(), 'with:');
  const spec = {};
  for (const line of lines.slice(checkoutIndex + 2)) {
    if (/^      (?:- |#)/.test(line) || /^  [\w-]+:/.test(line)) break;
    const match = line.match(/^\s{10}([\w-]+):\s*(.+)$/);
    if (match) spec[match[1]] = match[2];
  }
  return spec;
}

function selectFixtureCheckout(block, context, refs) {
  const spec = checkoutSpec(block);
  const expressions = {
    'github.repository': context.repository,
    'github.workflow_sha': context.workflowSha,
    'github.event.pull_request.base.sha': context.baseSha,
    'github.event.pull_request.head.sha': context.headSha,
  };
  const resolve = value => {
    const match = value.match(/^\$\{\{\s*(.+?)\s*\}\}$/);
    return match ? expressions[match[1]] : value;
  };
  const repository = resolve(spec.repository);
  const ref = resolve(spec.ref);
  assert.equal(repository, context.repository, 'checkout must stay in the target repository');
  const refName = Object.entries({
    base: context.baseSha,
    workflow: context.workflowSha,
    head: context.headSha,
  }).find(([, value]) => value === ref)?.[0];
  assert.ok(refName, `unexpected checkout ref: ${ref}`);
  return { repository, ref, refName, path: refs[refName] };
}

function provenanceScript(block) {
  const lines = block.split('\n');
  const nameIndex = lines.findIndex(line => line.includes('name: Record CI provenance'));
  const runIndex = lines.findIndex((line, index) => index > nameIndex && line.trim() === 'run: |');
  assert.ok(nameIndex >= 0 && runIndex > nameIndex, 'job must have a provenance run step');
  const script = [];
  for (const line of lines.slice(runIndex + 1)) {
    if (/^      (?:- |#)/.test(line) || /^  [\w-]+:/.test(line)) break;
    if (line.trim() === '') {
      script.push('');
    } else {
      assert.match(line, /^          /, 'provenance script indentation must be valid');
      script.push(line.slice(10));
    }
  }
  return script.join('\n');
}

test('gates accept intentional omissions but reject failed, cancelled, skipped or missing required work', () => {
  const jobs = {
    docs: ['docs-check'], ut: ['matrixone-ci'],
    bvt: ['bvt-group-plan', 'matrixone-compose-ci', 'matrixone-standalone-ci'],
    full: ['bvt-group-plan', 'matrixone-ci', 'matrixone-ut-coverage', 'matrixone-compose-ci',
      'matrixone-standalone-ci', 'matrixone-coverage-merge'],
  };
  for (const [scope, required] of Object.entries(jobs)) {
    const needs = results();
    for (const job of Object.keys(needs)) {
      if (!['change-scope', 'check-pr-valid', ...required].includes(job)) needs[job].result = 'skipped';
    }
    assert.match(verifyResults(scope, needs), /passed/);
    for (const job of ['change-scope', 'check-pr-valid', ...required]) {
      for (const result of ['failure', 'cancelled', 'skipped', undefined]) {
        assert.throws(() => verifyResults(scope, { ...needs, [job]: { result } }));
      }
    }
  }
  assert.throws(() => verifyResults('', results()));
  const needs = results();
  needs['check-pr-valid'].outputs.pr_valid = 'false';
  assert.throws(() => verifyResults('docs', needs));
});

test('a successful eligibility job cannot hide skipped UT coverage', () => {
  for (const scope of ['full']) {
    for (const ready of ['false', '', undefined]) {
      const needs = results();
      needs['matrixone-ut-coverage'].outputs.coverage_ready = ready;
      assert.throws(() => verifyResults(scope, needs), /not eligible/);
    }
  }
});

test('entrypoint routes each scope through one required check', () => {
  const blocks = entrypointJobs();
  const routed = ['docs-check', 'bvt-group-plan', 'matrixone-shared-build', 'matrixone-ci',
    'matrixone-ut-coverage', 'matrixone-upgrade-ci', 'matrixone-compose-ci',
    'matrixone-standalone-ci', 'matrixone-coverage-merge'];
  const expected = {
    docs: ['docs-check'], ut: ['matrixone-ci'],
    bvt: ['bvt-group-plan', 'matrixone-shared-build', 'matrixone-compose-ci', 'matrixone-standalone-ci'],
    full: routed.filter(job => job !== 'docs-check'),
  };
  for (const [scope, wanted] of Object.entries(expected)) {
    const needs = results();
    needs['change-scope'].outputs = { scope };
    needs['matrixone-shared-build'] = { result: 'success' };
    const selected = routed.filter(job => {
      const expression = blocks[job].match(/^    if: \$\{\{ (.*) \}\}$/m)[1]
        .replace(/needs\.([\w-]+)/g, "needs['$1']");
      return runInNewContext(expression, {
        needs, github: { base_ref: 'main' }, cancelled: () => false,
        contains: (values, value) => values.includes(value), fromJSON: JSON.parse,
      });
    });
    assert.deepEqual(selected, wanted, scope);
  }
  const gates = blocks['ci-required'];
  assert.match(gates, /if: \$\{\{ always\(\)/);
  assert.match(gates, /^    name: CI Required$/m);
  assert.doesNotMatch(gates, /^    strategy:/m);
  const gateNeeds = gates.match(/^    needs: \[(.*)\]$/m)[1].split(', ');
  assert.deepEqual(gateNeeds.slice().sort(), Object.keys(results()).sort());
  for (const job of ['matrixone-ci', 'matrixone-ut-coverage', 'matrixone-compose-ci',
    'matrixone-standalone-ci', 'matrixone-coverage-merge']) {
    assert.match(blocks[job], /name: .* execution\n/);
    assert.ok(gates.match(/^    needs: (.*)$/m)[1].includes(job));
  }
  for (const job of ['change-scope', 'ci-required']) {
    assert.match(blocks[job], /repository: \$\{\{ github.repository \}\}/);
    assert.match(blocks[job], /ref: \$\{\{ github.workflow_sha \}\}/);
    assert.doesNotMatch(blocks[job], /ref: \$\{\{ github.event.pull_request.base.sha \}\}/);
    assert.match(blocks[job], /persist-credentials: false/);
    assert.match(blocks[job], /WORKFLOW_SHA: \$\{\{ github.workflow_sha \}\}/);
    assert.match(blocks[job], /PR_BASE_SHA: \$\{\{ github.event.pull_request.base.sha \}\}/);
    assert.match(blocks[job], /PR_HEAD_SHA: \$\{\{ github.event.pull_request.head.sha \}\}/);
    assert.doesNotMatch(blocks[job], /ref:\s*main/);
    const provenance = blocks[job].indexOf('name: Record CI provenance');
    const checkout = blocks[job].indexOf('uses: actions/checkout@');
    assert.ok(provenance >= 0 && provenance < checkout, `${job} records provenance before checkout`);
    const provenanceBlock = blocks[job].slice(provenance, checkout);
    assert.match(provenanceBlock, /GITHUB_STEP_SUMMARY/);
    assert.match(provenanceBlock, /echo "- Workflow SHA: \$\{WORKFLOW_SHA\}"/);
    assert.match(provenanceBlock, /echo "- PR base SHA: \$\{PR_BASE_SHA\}"/);
    assert.match(provenanceBlock, /echo "- PR head SHA: \$\{PR_HEAD_SHA\}"/);
  }
});

test('provenance remains available when helper loading or checkout fails', () => {
  const root = mkdtempSync(join(tmpdir(), 'matrixone-ci-provenance-summary-'));
  try {
    const values = {
      WORKFLOW_SHA: 'workflow-sha',
      PR_BASE_SHA: 'base-sha',
      PR_HEAD_SHA: 'head-sha',
    };
    for (const job of ['change-scope', 'ci-required']) {
      const summaryPath = join(root, `${job}.md`);
      const result = spawnSync('/bin/bash', ['-eu', '-c', provenanceScript(entrypointJobs()[job])], {
        env: { ...process.env, ...values, GITHUB_STEP_SUMMARY: summaryPath },
        encoding: 'utf8',
      });
      assert.equal(result.status, 0, result.stderr);
      assert.match(readFileSync(summaryPath, 'utf8'), /Workflow SHA: workflow-sha/);
      assert.throws(() => { throw new Error('checkout failed before helper load'); }, /checkout failed/);
      assert.match(readFileSync(summaryPath, 'utf8'), /PR head SHA: head-sha/);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

function loadFixtureHelper(path) {
  delete require.cache[require.resolve(path)];
  return require(path);
}

test('workflow provenance selects both helpers from the actual checkout config', async () => {
  const root = mkdtempSync(join(tmpdir(), 'matrixone-ci-provenance-'));
  const refs = Object.fromEntries(['base', 'workflow', 'head'].map(ref => {
    const dir = join(root, ref);
    mkdirSync(join(dir, '.github', 'ci'), { recursive: true });
    return [ref, dir];
  }));
  try {
    cpSync(join(__dirname, 'change-scope.cjs'), join(refs.workflow, '.github', 'ci', 'change-scope.cjs'));
    writeFileSync(
      join(refs.head, '.github', 'ci', 'change-scope.cjs'),
      "module.exports = { resolveScope: async () => 'docs', verifyResults: () => { throw new Error('untrusted helper selected'); } };\n",
    );

    const context = {
      repository: 'matrixorigin/matrixone',
      workflowSha: 'workflow-sha',
      baseSha: 'base-sha',
      headSha: 'head-sha',
    };
    const blocks = entrypointJobs();
    const changeScopeCheckout = selectFixtureCheckout(blocks['change-scope'], context, refs);
    const ciRequiredCheckout = selectFixtureCheckout(blocks['ci-required'], context, refs);
    assert.equal(changeScopeCheckout.refName, 'workflow');
    assert.deepEqual(ciRequiredCheckout, changeScopeCheckout);

    const basePath = join(refs.base, '.github', 'ci', 'change-scope.cjs');
    const workflowPath = changeScopeCheckout.path + '/.github/ci/change-scope.cjs';
    const headPath = join(refs.head, '.github', 'ci', 'change-scope.cjs');
    assert.throws(() => require(basePath), /Cannot find module/);

    const workflowHelper = loadFixtureHelper(workflowPath);
    const headHelper = loadFixtureHelper(headPath);
    assert.equal(await workflowHelper.resolveScope(client({ files: [modified('pkg/a.go')] })), 'full');
    assert.match(workflowHelper.verifyResults('full', results()), /all required jobs passed/);
    assert.equal(await headHelper.resolveScope(client({ files: [modified('pkg/a.go')] })), 'docs');
    assert.throws(() => headHelper.verifyResults('full', results()), /untrusted helper selected/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('entrypoint runs the complete race-UT suite on one runner', () => {
  const caller = entrypointJobs()['matrixone-ci'];
  assert.match(caller, /^    uses: matrixorigin\/CI\/\.github\/workflows\/ci\.yaml@main$/m);
  assert.match(caller, /^    with:\n      ut_parallel: 6\n      ut_sharded: false$/m);
});
