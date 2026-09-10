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
const { readFileSync } = require('node:fs');
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
    assert.match(blocks[job], /ref: \$\{\{ github.event.pull_request.base.sha \}\}/);
    assert.match(blocks[job], /persist-credentials: false/);
  }
});

test('entrypoint runs the complete race-UT suite on one runner', () => {
  const caller = entrypointJobs()['matrixone-ci'];
  assert.match(caller, /^    uses: matrixorigin\/CI\/\.github\/workflows\/ci\.yaml@main$/m);
  assert.match(caller, /^    with:\n      ut_parallel: 6\n      ut_sharded: false$/m);
});
