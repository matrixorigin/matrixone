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
test('only narrow kernel test, documentation and SDK paths receive exemptions', () => {
  for (const [paths, expected] of [
    [['README.md', 'docs/design/a.md'], 'docs'],
    [['BUILD.md', 'pkg/common/moerr/error_handling.md', 'etc/DEV_README.md'], 'docs'],
    [['.agents/skills/mo-dev/SKILL.md', '.claude/skills/mo-dev/SKILL.md', '.github/copilot-instructions.md'], 'docs'],
    [['docs/cn/source/index.rst', 'pkg/sql/README.MD', 'optools/guide.RST'], 'docs'],
    [['fixtures.md', 'pkg/fixtures-notes/README.md'], 'docs'],
    [['docs/design/a.svg'], 'docs'],
    [['pkg/sql/plan/a_test.go'], 'ut'],
    [['pkg/tests/upgrade/a_test.go', 'pkg/sql/README.md'], 'ut'],
    [['test/distributed/cases/a.test', 'test/distributed/cases/a.result'], 'bvt'],
    [['test/distributed/cases/a.sql', 'BUILD.md'], 'bvt'],
    [['pkg/a_test.go', 'test/distributed/cases/a.test'], 'full'],
    [['pkg/a_test.go', 'pkg/a.go'], 'full'],
    [['pkg/tests/testutils/helper.go'], 'full'],
    [['pkg/embed/testing.go'], 'full'],
    [['pkg/a/testdata/input.json'], 'full'],
    [['pkg/a/testdata/input.md'], 'full'],
    [['pkg/Fixtures/input.rst'], 'full'],
    [['pkg/fixtures/a_test.go'], 'full'],
    [['docs/fixtures/example.svg'], 'full'],
    [['pkg/a/README.md', 'pkg/a.go'], 'full'],
    [['docs/example.mdx'], 'full'], [['docs/input.txt'], 'full'],
    [['docs/example.html'], 'full'], [['pkg/a.svg'], 'full'],
    [['pkg/input.md\n'], 'full'], [['docs/input.rst\r\n'], 'full'],
    [['go.mod'], 'full'], [['Makefile'], 'full'],
    [['.github/workflows/entrypoint.yaml'], 'full'],
    [['test/distributed/run.sh'], 'full'],
    [['docs/example.sh'], 'full'], [['etc/cn.toml'], 'full'],
    [['pkg/../main_test.go'], 'full'],
    [['clients/python/matrixone/client.py'], 'sdk'],
    [['clients/python/tests/offline/a.py', 'pkg/sql/README.md'], 'sdk'],
    [['clients/python/tests/fixtures/input.md'], 'sdk'],
    [['clients/python/pyproject.toml', 'clients/python/requirements.txt'], 'sdk'],
    [['clients/python/client.py', 'pkg/a_test.go'], 'full'],
    [['clients/python/client.py', 'test/distributed/cases/a.sql'], 'full'],
  ]) assert.equal(classify(paths.map(modified), paths.length).scope, expected, paths.join(','));
});

test('renames consider the old path, including production-to-doc and production-to-test', () => {
  for (const filename of ['docs/a.md', 'pkg/README.md', 'BUILD.rst', 'pkg/a_test.go']) {
    assert.deepEqual(classify([{ filename, previous_filename: 'pkg/a.go', status: 'renamed' }], 1), { scope: 'full', sdk_required: false });
  }
  assert.deepEqual(classify([{ filename: 'pkg/b_test.go', previous_filename: 'pkg/a_test.go', status: 'renamed' }], 1), { scope: 'ut', sdk_required: false });
  assert.deepEqual(classify([{ filename: 'pkg/a_test.go', status: 'removed' }], 1), { scope: 'ut', sdk_required: false });
  assert.deepEqual(classify([{ filename: 'pkg/README.md', previous_filename: 'BUILD.rst', status: 'renamed' }], 1), { scope: 'docs', sdk_required: false });
  for (const [filename, previous_filename] of [
    ['pkg/README.md', 'pkg/testdata/input.md'],
    ['pkg/fixtures/input.rst', 'BUILD.rst'],
  ]) assert.deepEqual(classify([{ filename, previous_filename, status: 'renamed' }], 1), { scope: 'full', sdk_required: false });
  for (const status of ['added', 'removed']) {
    assert.deepEqual(classify([{ filename: 'pkg/README.md', status }], 1), { scope: 'docs', sdk_required: false });
  }
});

test('empty, capped, incomplete, duplicate and malformed lists fail closed', () => {
  const file = modified('pkg/a_test.go');
  for (const [files, count] of [
    [[], 0], [[file], 2], [[file, file], 2], [null, 1],
    [[{ ...file, status: 'renamed' }], 1], [[{ ...file, status: 'unknown' }], 1],
    [[null], 1], [[modified(null)], 1],
    [Array.from({ length: 3000 }, (_, i) => modified(`pkg/a${i}_test.go`)), 3000],
  ]) assert.deepEqual(classify(files, count), { scope: 'full', sdk_required: true });
  assert.deepEqual(classify([file], 1, true), { scope: 'full', sdk_required: true });
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
  assert.deepEqual(await resolveScope(client()), { scope: 'ut', sdk_required: false });
  assert.deepEqual(await resolveScope(client({ error: true })), { scope: 'full', sdk_required: true });
  const { pr } = client();
  const movedHead = { ...pr, head: { sha: 'new' } };
  const movedBase = { ...pr, base: { sha: 'new' } };
  for (const revisions of [[movedHead], [pr, movedHead], [pr, movedBase]]) {
    await assert.rejects(resolveScope(client({ revisions })), /revision changed/);
  }
});

function results() {
  const needs = Object.fromEntries(['docs-check', 'python-sdk', 'matrixone-ci',
    'matrixone-ut-coverage', 'matrixone-compose-ci', 'matrixone-standalone-ci',
    'matrixone-coverage-merge'].map(job => [job, { result: 'success' }]));
  needs.preflight = { result: 'success', outputs: {
    pr_valid: 'true', sdk_required: 'false', compose_group: '0', launch_group: '1', generation: '123-1',
  } };
  needs['matrixone-ut-coverage'].outputs = { coverage_ready: 'true' };
  return needs;
}

function entrypointJobs() {
  const workflow = readFileSync(`${__dirname}/../workflows/entrypoint.yaml`, 'utf8');
  return Object.fromEntries([...workflow.matchAll(/^  ([\w-]+):\n([\s\S]*?)(?=^  [\w-]+:\n|$(?![\s\S]))/gm)]
    .map(match => [match[1], match[2]]));
}

test('actual preflight script publishes the authoritative plan as strict string outputs', async () => {
  const block = entrypointJobs().preflight;
  const script = block.match(/          script: \|\n([\s\S]*?)(?=      - id: plan)/)[1]
    .split('\n').map(line => line.slice(12)).join('\n');
  for (const [paths, expected] of [
    [['BUILD.md', 'pkg/sql/README.md', 'docs/cn/source/index.rst'], { scope: 'docs', sdk_required: 'false' }],
    [['clients/python/pyproject.toml'], { scope: 'sdk', sdk_required: 'true' }],
    [['clients/python/client.py', '.agents/skills/mo-dev/SKILL.md'], { scope: 'sdk', sdk_required: 'true' }],
    [['clients/python/client.py', 'pkg/a.go'], { scope: 'full', sdk_required: 'true' }],
    [['pkg/a_test.go', 'BUILD.md'], { scope: 'ut', sdk_required: 'false' }],
    [['test/distributed/cases/a.sql', 'pkg/sql/README.md'], { scope: 'bvt', sdk_required: 'false' }],
    [['pkg/testdata/input.md'], { scope: 'full', sdk_required: 'false' }],
    [['pkg/a.go'], { scope: 'full', sdk_required: 'false' }],
  ]) {
    const fixture = client({ files: paths.map(modified) });
    fixture.pr.changed_files = paths.length;
    const outputs = {};
    const summary = { addHeading() { return this; }, addRaw() { return this; }, async write() {} };
    await runInNewContext(`(async () => { ${script} })()`, {
      ...fixture,
      require(path) {
        assert.equal(path, './.github/ci/change-scope.cjs');
        return { resolveScope };
      },
      process: { env: {} },
      core: { setOutput: (name, value) => { outputs[name] = value; }, summary },
    });
    assert.deepEqual(outputs, expected);
  }
  assert.match(block, /sdk_required: \$\{\{ steps\.scope\.outputs\.sdk_required \}\}/);
});

function checkoutSpec(block) {
  const lines = block.split('\n');
  const checkoutIndex = lines.findIndex(line => /^      - uses: actions\/checkout@/.test(line));
  assert.notEqual(checkoutIndex, -1, 'job must contain the trusted checkout');
  const withIndex = checkoutIndex + (lines[checkoutIndex + 1].trim().startsWith('if:') ? 2 : 1);
  assert.equal(lines[withIndex].trim(), 'with:');
  const spec = {};
  for (const line of lines.slice(withIndex + 1)) {
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
    docs: ['docs-check'], sdk: ['python-sdk'], ut: ['matrixone-ci'],
    bvt: ['matrixone-compose-ci', 'matrixone-standalone-ci'],
    full: ['matrixone-ci', 'matrixone-ut-coverage', 'matrixone-compose-ci',
      'matrixone-standalone-ci', 'matrixone-coverage-merge'],
  };
  for (const [scope, required] of Object.entries(jobs)) {
    const needs = results();
    needs.preflight.outputs.sdk_required = scope === 'sdk' ? 'true' : 'false';
    for (const job of Object.keys(needs)) {
      if (!['preflight', ...required].includes(job)) needs[job].result = 'skipped';
    }
    assert.match(verifyResults(scope, needs), /passed/);
    for (const job of ['preflight', ...required]) {
      for (const result of ['failure', 'cancelled', 'skipped', undefined]) {
        assert.throws(() => verifyResults(scope, { ...needs, [job]: { result } }));
      }
    }
  }
  assert.throws(() => verifyResults('', results()));
  const needs = results();
  needs['preflight'].outputs.pr_valid = 'false';
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

test('successful preflight cannot hide a missing or non-complementary BVT plan', () => {
  for (const scope of ['bvt', 'full']) {
    for (const override of [
      { compose_group: undefined }, { launch_group: '' }, { launch_group: '0' },
      { compose_group: '2' }, { generation: '' }, { generation: '123' },
    ]) {
      const needs = results();
      Object.assign(needs.preflight.outputs, override);
      assert.throws(() => verifyResults(scope, needs), /BVT plan/);
    }
  }
  for (const scope of ['docs', 'ut']) {
    const needs = results();
    needs.preflight.outputs = { pr_valid: 'true', sdk_required: 'false' };
    assert.match(verifyResults(scope, needs), /passed/);
  }
});

test('preflight runs classification only for valid modern PRs and plans only BVT scopes', () => {
  const block = entrypointJobs().preflight;
  const steps = block.split(/^      - /m).slice(1);
  const checkout = steps.find(step => step.startsWith('uses: actions/checkout@'));
  const scopeStep = steps.find(step => /id: scope\n/.test(step));
  const plan = steps.find(step => step.startsWith('id: plan\n'));
  const selected = (step, scope, valid, base) => {
    const expression = step.match(/^        if: \$\{\{ (.*) \}\}$/m)[1];
    return runInNewContext(expression, {
      steps: { check_pr_valid: { outputs: { pull_valid: valid } }, scope: { outputs: { scope } } },
      github: { base_ref: base }, fromJSON: JSON.parse, contains: (values, value) => values.includes(value),
    });
  };
  for (const scope of ['docs', 'ut', 'bvt', 'full']) {
    for (const base of ['main', '4.2-dev', '3.0-dev']) {
      for (const valid of ['true', 'false', '']) {
        const modernValid = valid === 'true' && base !== '3.0-dev';
        assert.equal(selected(checkout, scope, valid, base), modernValid);
        assert.equal(selected(scopeStep, scope, valid, base), modernValid);
        assert.equal(selected(plan, scope, valid, base), modernValid && ['bvt', 'full'].includes(scope));
      }
    }
  }
  assert.match(block, /pr_valid: \$\{\{ steps.check_pr_valid.outputs.pull_valid \}\}/);
  for (const output of ['compose_group', 'launch_group', 'generation']) {
    assert.ok(block.includes(`${output}: \u0024{{ steps.plan.outputs.${output} }}`));
  }
  for (const [job, jobBlock] of Object.entries(entrypointJobs())) {
    assert.doesNotMatch(jobBlock, /needs[.:].*(?:check-pr-valid|change-scope|bvt-group-plan)/, job);
    if (job.endsWith('-30')) {
      assert.match(jobBlock, /needs: preflight/);
      assert.match(jobBlock, /needs.preflight.outputs.pr_valid == 'true' && github.base_ref == '3.0-dev'/);
    }
  }
});

test('trusted control jobs share the configurable runner while PR diff and test jobs stay separate', () => {
  const blocks = entrypointJobs();
  const controller = readFileSync(join(__dirname, '../workflows/ci-cancellation.yml'), 'utf8');
  for (const block of [blocks.preflight, blocks['ci-required'], controller]) {
    const expression = block.match(/^    runs-on: \$\{\{ (.*) \}\}$/m)[1];
    for (const label of ['', undefined, 'ci-control-linux']) {
      assert.equal(runInNewContext(expression, { vars: { CI_CONTROL_RUNNER_LABEL: label } }), label || 'ubuntu-latest');
    }
    assert.match(block, /timeout-minutes: [56]/);
  }
  assert.equal(Object.keys(blocks).filter(job => /^(?:preflight|check-pr-valid|change-scope|bvt-group-plan)$/.test(job)).length, 1);
  assert.doesNotMatch(blocks['docs-check'], /CI_CONTROL_RUNNER_LABEL/);
  assert.doesNotMatch(blocks['matrixone-ci'], /CI_CONTROL_RUNNER_LABEL/);
});

test('entrypoint routes each scope through one required check', () => {
  const blocks = entrypointJobs();
  const routed = ['docs-check', 'matrixone-shared-build', 'matrixone-ci',
    'matrixone-ut-coverage', 'matrixone-upgrade-ci', 'matrixone-compose-ci',
    'matrixone-standalone-ci', 'matrixone-coverage-merge', 'matrixone-utils-ci', 'python-sdk'];
  const cases = [
    ['docs', false, ['docs-check', 'matrixone-utils-ci']],
    ['ut', false, ['matrixone-ci', 'matrixone-utils-ci']],
    ['bvt', false, ['matrixone-shared-build', 'matrixone-compose-ci', 'matrixone-standalone-ci', 'matrixone-utils-ci']],
    ['sdk', true, ['python-sdk']],
    ['full', false, routed.filter(job => !['docs-check', 'python-sdk'].includes(job))],
    ['full', true, routed.filter(job => job !== 'docs-check')],
  ];
  for (const [scope, sdkRequired, wanted] of cases) {
    const needs = results();
    needs.preflight.outputs.scope = scope;
    needs.preflight.outputs.sdk_required = String(sdkRequired);
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
  for (const job of ['preflight', 'ci-required']) {
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
    for (const job of ['preflight', 'ci-required']) {
      const summaryPath = join(root, `${job}.md`);
      const result = spawnSync('/bin/bash', [
        '-eu',
        '-c',
        `${provenanceScript(entrypointJobs()[job])}\n# Simulated checkout failure\nexit 42`,
      ], {
        env: { ...process.env, ...values, GITHUB_STEP_SUMMARY: summaryPath },
        encoding: 'utf8',
      });
      assert.equal(result.status, 42, result.stderr);
      assert.match(readFileSync(summaryPath, 'utf8'), /Workflow SHA: workflow-sha/);
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
    const changeScopeCheckout = selectFixtureCheckout(blocks['preflight'], context, refs);
    const ciRequiredCheckout = selectFixtureCheckout(blocks['ci-required'], context, refs);
    assert.equal(changeScopeCheckout.refName, 'workflow');
    assert.deepEqual(ciRequiredCheckout, changeScopeCheckout);

    const basePath = join(refs.base, '.github', 'ci', 'change-scope.cjs');
    const workflowPath = changeScopeCheckout.path + '/.github/ci/change-scope.cjs';
    const headPath = join(refs.head, '.github', 'ci', 'change-scope.cjs');
    assert.throws(() => require(basePath), /Cannot find module/);

    const workflowHelper = loadFixtureHelper(workflowPath);
    const headHelper = loadFixtureHelper(headPath);
    assert.deepEqual(await workflowHelper.resolveScope(client({ files: [modified('pkg/a.go')] })), { scope: 'full', sdk_required: false });
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
  assert.match(caller, /^    with:\n      ut_parallel: 8\n      ut_sharded: false$/m);
});

test('SDK impact is retained across mixed changes, control files and both rename paths', () => {
  for (const [files, expected] of [
    [[modified('clients/python/README.md')], { scope: 'sdk', sdk_required: true }],
    [[{ filename: 'clients/python/requirements.txt', status: 'removed' }], { scope: 'sdk', sdk_required: true }],
    [[modified('clients/python/pyproject.toml'), modified('README.md')], { scope: 'sdk', sdk_required: true }],
    [[modified('pkg/a.go'), modified('clients/python/a.py')], { scope: 'full', sdk_required: true }],
    [[modified('pkg/a.go')], { scope: 'full', sdk_required: false }],
    [[modified('.github/workflows/python-sdk.yml')], { scope: 'full', sdk_required: true }],
    [[modified('.github/ci/change-scope.cjs')], { scope: 'full', sdk_required: true }],
    [[modified('.github/workflows/entrypoint.yaml')], { scope: 'full', sdk_required: true }],
    [[{ filename: 'clients/python/a.py', previous_filename: 'pkg/a.go', status: 'renamed' }], { scope: 'full', sdk_required: true }],
    [[{ filename: 'pkg/a.go', previous_filename: 'clients/python/a.py', status: 'renamed' }], { scope: 'full', sdk_required: true }],
    [[{ filename: 'clients/python/b.py', previous_filename: 'clients/python/a.py', status: 'renamed' }], { scope: 'sdk', sdk_required: true }],
    [[{ filename: 'pkg/README.md', previous_filename: 'clients/python/README.md', status: 'renamed' }], { scope: 'sdk', sdk_required: true }],
    [[{ filename: 'clients/python/README.md', previous_filename: 'BUILD.rst', status: 'renamed' }], { scope: 'sdk', sdk_required: true }],
    [[modified('clients/python//a.py')], { scope: 'full', sdk_required: true }],
    [[{ filename: 'clients/python/a.py', previous_filename: 'pkg/../a.go', status: 'renamed' }], { scope: 'full', sdk_required: true }],
  ]) assert.deepEqual(classify(files, files.length), expected, JSON.stringify(files));
  assert.deepEqual(classify([modified('clients/python/a.py')], 1, true), { scope: 'full', sdk_required: true });
});

test('the required check enforces SDK flags and SDK results for mixed full runs', () => {
  for (const value of [undefined, true, false, '', 'yes', 'TRUE']) {
    const needs = results();
    needs.preflight.outputs.sdk_required = value;
    assert.throws(() => verifyResults('full', needs), /SDK requirement/);
  }
  for (const scope of ['docs', 'ut', 'bvt']) {
    const needs = results();
    needs.preflight.outputs.sdk_required = 'true';
    assert.throws(() => verifyResults(scope, needs), /SDK requirement/);
  }
  assert.throws(() => verifyResults('sdk', results()), /SDK requirement/);
  const needs = results();
  needs.preflight.outputs.sdk_required = 'true';
  assert.match(verifyResults('full', needs), /passed/);
  for (const result of ['failure', 'cancelled', 'skipped', undefined]) {
    assert.throws(() => verifyResults('full', { ...needs, 'python-sdk': { result } }), /python-sdk/);
  }
  const missing = { ...needs };
  delete missing['python-sdk'];
  assert.throws(() => verifyResults('full', missing), /python-sdk/);
  needs['matrixone-ci'].result = 'failure';
  assert.throws(() => verifyResults('full', needs), /matrixone-ci/);
});

test('SDK routing rejects invalid preflight, cancellations and legacy branches', () => {
  const block = entrypointJobs()['python-sdk'];
  const expression = block.match(/^    if: \$\{\{ (.*) \}\}$/m)[1]
    .replace(/needs\.([\w-]+)/g, "needs['$1']");
  for (const [overrides, cancelled, base, expected] of [
    [{}, false, 'main', true],
    [{ result: 'failure' }, false, 'main', false],
    [{ outputs: { pr_valid: 'false', sdk_required: 'true' } }, false, 'main', false],
    [{}, true, 'main', false],
    [{}, false, '3.0-dev', false],
    [{ outputs: { pr_valid: 'true', sdk_required: 'false' } }, false, 'main', false],
  ]) {
    const preflight = { result: 'success', outputs: { pr_valid: 'true', sdk_required: 'true' }, ...overrides };
    assert.equal(runInNewContext(expression, {
      needs: { preflight }, github: { base_ref: base }, cancelled: () => cancelled,
    }), expected);
  }
  assert.match(block, /^    uses: \.\/\.github\/workflows\/python-sdk.yml$/m);
  assert.match(block, /^    permissions:\n      contents: read$/m);
  assert.doesNotMatch(block, /secrets:|environment:/);
});

test('the reusable SDK owner preserves the matrix and isolates untrusted checkout', () => {
  const sdk = readFileSync(join(__dirname, '../workflows/python-sdk.yml'), 'utf8');
  const sections = Object.fromEntries([...sdk.matchAll(/^  ([\w-]+):\n([\s\S]*?)(?=^  [\w-]+:\n|$(?![\s\S]))/gm)]
    .map(match => [match[1], match[2]]));
  assert.match(sdk, /^  workflow_call:$/m);
  assert.doesNotMatch(sdk, /^  pull_request(?:_target)?:$/m);
  assert.match(sdk, /branches: \[main, develop\]/);
  const offline = sections.offline;
  assert.match(offline, /timeout-minutes: 30/);
  assert.match(offline, /runs-on: ubuntu-latest/);
  assert.doesNotMatch(offline, /secrets\.|environment:|actions\/cache/);
  assert.doesNotMatch(sdk, /continue-on-error:/);
  const checkout = checkoutSpec(offline);
  assert.equal(checkout.repository, '${{ github.event.pull_request.head.repo.full_name || github.repository }}');
  assert.equal(checkout.ref, '${{ github.event.pull_request.head.sha || github.sha }}');
  assert.equal(checkout['persist-credentials'], 'false');
  assert.equal(checkout['allow-unsafe-pr-checkout'], "${{ github.event_name == 'pull_request_target' }}");
  assert.match(offline, /actions\/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1/);
  assert.ok(offline.indexOf('Record SDK provenance') < offline.indexOf('Install package'));
  assert.match(offline, /WORKFLOW_SHA: \$\{\{ github.workflow_sha \}\}/);
  assert.match(offline, /flake8 matrixone --select=E9,F63,F7,F82/);
  assert.match(offline, /if: \$\{\{ matrix.python == '3.12' && matrix.sqlalchemy == '>=2.0,<2.1' \}\}/);
  assert.match(offline, /fail-fast: false/);
  const pythons = ['3.10', '3.12'];
  const sqlalchemy = ['>=1.4,<1.5', '>=2.0,<2.1', '>=2.1,<3'];
  for (const python of pythons) assert.ok(offline.includes(`'${python}'`));
  for (const range of sqlalchemy) assert.ok(offline.includes(`'${range}'`));
  assert.match(offline, /exclude:\n          - python: '3.10'\n            sqlalchemy: '>=2.1,<3'/);
  assert.match(offline, /include:[\s\S]*?- python: '3.11'\n            sqlalchemy: '>=2.1,<3'/);
  assert.match(offline, /name: python-sdk-\$\{\{ github.event.pull_request.head.sha \|\| github.sha \}\}-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}-\$\{\{ strategy.job-index \}\}/);
  assert.match(offline, /if: \$\{\{ !cancelled\(\) \}\}/);
  const onlineExpression = sections.online.match(/^    if: (.*)$/m)[1];
  for (const [event, enabled, expected] of [
    ['pull_request_target', true, false], ['workflow_call', true, false],
    ['push', true, false], ['workflow_dispatch', false, false], ['workflow_dispatch', true, true],
  ]) assert.equal(runInNewContext(onlineExpression, { github: { event_name: event }, inputs: { online: enabled } }), expected);
  assert.match(sections.online, /environment: matrixone-sdk-integration/);
  const group = sdk.match(/^  group: (.*)$/m)[1];
  assert.ok(group.startsWith('python-sdk/'));
  assert.ok(group.includes('github.event.pull_request.head.repo.full_name || github.repository'));
  assert.ok(group.includes('github.event.pull_request.head.ref || github.ref'));
  assert.notEqual(group, readFileSync(join(__dirname, '../workflows/entrypoint.yaml'), 'utf8').match(/^  group: (.*)$/m)[1]);
  assert.match(readFileSync(join(__dirname, '../workflows/ci-scope-check.yml'), 'utf8'), /'\.github\/workflows\/python-sdk.yml'/);
});
