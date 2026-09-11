// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { releaseSupersededRuns, isSuperseded } = require('./cancel-superseded.cjs');

const newer = () => ({ id: 200, workflow_id: 42, event: 'pull_request_target',
  head_branch: 'fix/test', head_repository: { id: 7 }, run_attempt: 1,
  created_at: '2026-09-11T08:13:51Z', run_started_at: '2026-09-11T08:13:51Z', status: 'pending' });
const older = (overrides = {}) => ({ ...newer(), id: 100, status: 'queued',
  created_at: '2026-09-11T07:46:44Z', run_started_at: '2026-09-11T07:46:44Z', ...overrides });

function harness({ candidates = [older()], cancel, force, tick, get } = {}) {
  const runs = new Map([newer(), ...candidates].map(run => [run.id, { ...run }]));
  const calls = [];
  let elapsed = 0;
  const actions = {
    getWorkflow: async args => {
      assert.equal(args.workflow_id, 'entrypoint.yaml');
      return { data: { id: 42 } };
    },
    getWorkflowRun: async args => {
      if (get) get(runs, args.run_id, calls);
      return { data: { ...runs.get(args.run_id) } };
    },
    listWorkflowRuns: async args => {
      assert.equal(args.workflow_id, 42);
      assert.equal(args.branch, 'fix/test');
      assert.equal(args.event, 'pull_request_target');
      const rows = candidates.filter(run => run.status === args.status);
      return { data: { total_count: rows.length, workflow_runs: rows } };
    },
    cancelWorkflowRun: async args => {
      calls.push(['cancel', args.run_id, elapsed]);
      if (cancel) await cancel(runs, args.run_id);
    },
  };
  const core = { info: () => {}, summary: {
    addHeading() { return this; }, addRaw() { return this; }, async write() {},
  } };
  const options = { github: { rest: { actions }, request: async (route, args) => {
    assert.equal(route, 'POST /repos/{owner}/{repo}/actions/runs/{run_id}/force-cancel');
    calls.push(['force', args.run_id, elapsed]);
    if (force) await force(runs, args.run_id);
  } }, core, context: { repo: { owner: 'matrixorigin', repo: 'matrixone' } }, runId: '200',
  wait: async ms => { elapsed += ms; if (tick) tick(runs, elapsed); } };
  return { runs, calls, options, run: () => releaseSupersededRuns(options) };
}

test('queued always gate is force-cancelled only after ordinary cancellation and shared grace', async () => {
  const h = harness({ candidates: [older(), older({ id: 101 })] });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 100, 0], ['cancel', 101, 0],
    ['force', 100, 120000], ['force', 101, 120000]]);
});

test('normal teardown completes without escalation', async () => {
  const h = harness({ tick(runs) { runs.get(100).status = 'completed'; } });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 100, 0]]);
});

test('different forks, branches, workflows, events, newer IDs and reruns are excluded', async () => {
  for (const override of [
    { head_repository: { id: 8 } }, { head_branch: 'another' }, { workflow_id: 43 },
    { event: 'push' }, { id: 200 }, { id: 201 }, { status: 'completed' },
    { run_started_at: '2026-09-11T08:20:00Z', run_attempt: 2 },
    { run_started_at: undefined }, { head_repository: null },
  ]) {
    assert.equal(isSuperseded(older(override), newer()), false, JSON.stringify(override));
    const h = harness({ candidates: [older(override)] });
    await h.run();
    assert.deepEqual(h.calls, [], JSON.stringify(override));
  }
});

test('a delayed controller does nothing after its successor is cancelled', async () => {
  const h = harness();
  h.runs.get(200).status = 'completed';
  await h.run();
  assert.deepEqual(h.calls, []);
});

test('successor cancellation during grace prevents escalation', async () => {
  const h = harness({ tick(runs) { runs.get(200).status = 'completed'; } });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 100, 0]]);
});

test('rerunning either successor or target during grace prevents stale escalation', async () => {
  for (const id of [100, 200]) {
    const h = harness({ tick(runs) { runs.get(id).run_attempt = 2; } });
    await h.run();
    assert.deepEqual(h.calls, [['cancel', 100, 0]]);
  }
});

test('target state is refreshed before initial cancellation', async () => {
  const h = harness({ get(runs, id) { if (id === 100) runs.get(id).status = 'completed'; } });
  await h.run();
  assert.deepEqual(h.calls, []);
});

test('completion racing cancellation is harmless and already-cancelling runs still receive grace', async () => {
  const completed = harness({ cancel(runs, id) {
    runs.get(id).status = 'completed';
    throw Object.assign(new Error('conflict'), { status: 409 });
  } });
  await completed.run();
  assert.deepEqual(completed.calls, [['cancel', 100, 0]]);
  const active = harness({ cancel() { throw Object.assign(new Error('conflict'), { status: 409 }); } });
  await active.run();
  assert.deepEqual(active.calls, [['cancel', 100, 0], ['force', 100, 120000]]);
});

test('permission and transport errors do not turn into successful cancellation', async () => {
  for (const status of [403, 500]) {
    const h = harness({ cancel() { throw Object.assign(new Error('API failure'), { status }); } });
    await assert.rejects(h.run(), /API failure/);
    assert.deepEqual(h.calls, [['cancel', 100, 0]]);
  }
});

test('force-cancel errors propagate, while a completion race is accepted', async () => {
  const failed = harness({ force() { throw Object.assign(new Error('force failed'), { status: 403 }); } });
  await assert.rejects(failed.run(), /force failed/);
  const conflict = harness({ force() { throw Object.assign(new Error('force conflict'), { status: 409 }); } });
  await assert.rejects(conflict.run(), /force conflict/);
  const completed = harness({ force(runs, id) {
    runs.get(id).status = 'completed';
    throw Object.assign(new Error('completed'), { status: 409 });
  } });
  await completed.run();
});

test('manual recovery rejects arbitrary workflows and invalid IDs', async () => {
  for (const runId of ['', '-1', '1e2', '200; exit 0', '9007199254740992']) {
    const h = harness();
    h.options.runId = runId;
    await assert.rejects(h.run(), /Invalid successor/);
    assert.deepEqual(h.calls, []);
  }
  const h = harness();
  h.runs.get(200).workflow_id = 99;
  await assert.rejects(h.run(), /must be a MatrixOne ALL CI/);
});

test('unusually large discovery fails rather than claiming all runs were inspected', async () => {
  const h = harness({ candidates: Array.from({ length: 101 }, (_, id) => older({ id: id + 1 })) });
  await assert.rejects(h.run(), /Too many active/);
  assert.deepEqual(h.calls, []);
});

test('controller stays outside producer concurrency and uses only trusted code with bounded execution', () => {
  const workflow = readFileSync(join(__dirname, '../workflows/ci-cancellation.yml'), 'utf8');
  assert.match(workflow, /workflow_run:\n    workflows: \['MatrixOne ALL CI'\]\n    types: \[requested\]/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.doesNotMatch(workflow, /^\s*concurrency:/m);
  assert.match(workflow, /actions: write/);
  assert.doesNotMatch(workflow, /checks: write|secrets: inherit|download-artifact/);
  assert.match(workflow, /ref: \$\{\{ github.workflow_sha \}\}/);
  assert.match(workflow, /github.ref == format\('refs\/heads\/\{0\}', github.event.repository.default_branch\)/);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /timeout-minutes: 6/);
  assert.doesNotMatch(workflow, /ref:.*head|ref:.*inputs/);
  const entrypoint = readFileSync(join(__dirname, '../workflows/entrypoint.yaml'), 'utf8');
  assert.match(entrypoint, /cancel-in-progress: true/);
  const gate = entrypoint.split('  ci-required:')[1].split('  matrixone-ci-30:')[0];
  assert.match(gate, /if: \$\{\{ always\(\)/);
  assert.match(gate, /timeout-minutes: 5/);
});
