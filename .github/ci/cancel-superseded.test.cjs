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
const { releaseSupersededRuns, releaseClosedPullRequestRuns, releaseCompletedCoverageWorkers,
  isSuperseded } = require('./cancel-superseded.cjs');

const newer = () => ({ id: 200, workflow_id: 42, event: 'pull_request_target',
  head_branch: 'fix/test', head_repository: { id: 7 }, run_attempt: 1,
  created_at: '2026-09-11T08:13:51Z', run_started_at: '2026-09-11T08:13:51Z', status: 'pending' });
const older = (overrides = {}) => ({ ...newer(), id: 100, status: 'queued',
  created_at: '2026-09-11T07:46:44Z', run_started_at: '2026-09-11T07:46:44Z', ...overrides });

function harness({ candidates = [older()], cancel, force, tick, get, pr, prGet, openPRs = [] } = {}) {
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
  const options = { github: { rest: { actions, pulls: { list: async args => {
    assert.equal(args.head, 'author:fix/test');
    assert.equal(args.state, 'open');
    return { data: openPRs };
  }, get: async () => {
    if (prGet) prGet(pr, calls);
    return { data: structuredClone(pr) };
  } } }, request: async (route, args) => {
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
  assert.match(workflow, /workflow_run:\n    workflows: \['MatrixOne ALL CI'\]\n    types: \[requested, completed\]/);
  assert.match(workflow, /releaseCompletedCoverageWorkers/);
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /pull_request_target:\n    types: \[closed\]/);
  assert.match(workflow, /pull-requests: read/);
  assert.doesNotMatch(workflow, /^\s*concurrency:/m);
  assert.match(workflow, /actions: write/);
  assert.doesNotMatch(workflow, /checks: write|secrets: inherit|download-artifact/);
  assert.match(workflow, /ref: \$\{\{ github.event_name == 'pull_request_target' && github.event.repository.default_branch \|\| github.workflow_sha \}\}/);
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


function closedHarness(options = {}) {
  const pr = { number: 123, state: 'closed', closed_at: '2026-09-11T08:30:00Z',
    head: { ref: 'fix/test', repo: { id: 7, owner: { login: 'author' } } }, ...options.pr };
  const h = harness({ ...options, pr });
  h.options.context.payload = { action: 'closed', pull_request: structuredClone(pr) };
  h.run = () => releaseClosedPullRequestRuns(h.options);
  return h;
}

test('closing a PR cancels all active matching runs, then escalates with the shared grace', async () => {
  const h = closedHarness({ candidates: [older(), older({ id: 101, status: 'in_progress' })] });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 100, 0], ['cancel', 101, 0],
    ['force', 100, 120000], ['force', 101, 120000]]);
});

test('closed PR discovery excludes other identities, completed runs and post-close reruns', async () => {
  for (const override of [
    { head_repository: { id: 8 } }, { head_branch: 'another' }, { workflow_id: 43 },
    { event: 'push' }, { status: 'completed' }, { head_repository: null },
    { created_at: '2026-09-11T08:31:00Z' }, { run_started_at: '2026-09-11T08:31:00Z' },
    { run_started_at: undefined },
  ]) {
    const h = closedHarness({ candidates: [older(override)] });
    await h.run();
    assert.deepEqual(h.calls, [], JSON.stringify(override));
  }
});

test('reopened PRs, missing forks and stale close events never cancel runs', async () => {
  for (const update of [
    pr => { pr.state = 'open'; },
    pr => { pr.closed_at = '2026-09-11T08:32:00Z'; },
    pr => { pr.head.repo = null; },
    pr => { pr.head.ref = 'other'; },
  ]) {
    const h = closedHarness({ prGet: update });
    await h.run();
    assert.deepEqual(h.calls, []);
  }
});

test('reopening during grace or rerunning a target prevents stale force cancellation', async () => {
  for (const reopen of [true, false]) {
    const h = closedHarness({ tick(runs) { runs.get(100).run_attempt = 2; },
      prGet(pr, calls) { if (reopen && calls.length) pr.state = 'open'; } });
    await h.run();
    assert.deepEqual(h.calls, [['cancel', 100, 0]]);
  }
});

test('closed PR cancellation handles normal completion, API errors and malformed events', async () => {
  const completed = closedHarness({ cancel(runs, id) { runs.get(id).status = 'completed'; } });
  await completed.run();
  assert.deepEqual(completed.calls, [['cancel', 100, 0]]);
  const failed = closedHarness({ cancel() { throw Object.assign(new Error('denied'), { status: 403 }); } });
  await assert.rejects(failed.run(), /denied/);
  const shared = closedHarness({ openPRs: [{ number: 124 }] });
  await shared.run();
  assert.deepEqual(shared.calls, [], 'another open PR still needs the same branch CI');
  const invalid = closedHarness();
  invalid.options.context.payload.action = 'opened';
  await assert.rejects(invalid.run(), /Expected a closed/);
  assert.deepEqual(invalid.calls, []);
});

function coverageHarness({ children, parentOverride = {}, tick, receipts = [], listed = () => true } = {}) {
  const sha = 'a'.repeat(40);
  const parent = { id: 600, run_attempt: 2, workflow_id: 42, event: 'pull_request_target',
    path: '.github/workflows/entrypoint.yaml', repository: { full_name: 'matrixorigin/matrixone' },
    run_started_at: '2026-11-20T10:00:00Z', updated_at: '2026-11-20T11:00:00Z',
    head_sha: sha, status: 'completed', conclusion: 'cancelled', ...parentOverride };
  const child = (id, group, pool, overrides = {}) => ({ id, run_attempt: 1, workflow_id: 43,
    event: 'workflow_dispatch', path: '.github/workflows/coverage-standby-worker.yaml',
    repository: { full_name: 'matrixorigin/matrixone' }, head_branch: 'main', status: 'queued',
    display_title: `Coverage standby / 600/2/${group}/${sha}/${pool}/${'b'.repeat(64)}`,
    ...overrides });
  const rows = new Map((children || [child(700, 0, 'cs'), child(701, 1, 'idc', { status: 'in_progress' })])
    .map(run => [run.id, run]));
  const calls = [];
  let elapsed = 0;
  const actions = {
    getWorkflow: async ({ workflow_id }) => ({ data: { id: workflow_id === 'entrypoint.yaml' ? 42 : 43 } }),
    getWorkflowRunAttempt: async () => ({ data: { ...parent } }),
    listWorkflowRunArtifacts: async ({ run_id, page, per_page }) => {
      assert.equal(run_id, parent.id);
      assert.equal(per_page, 100);
      return { data: { total_count: receipts.length,
        artifacts: receipts.slice((page - 1) * per_page, page * per_page).map(name => ({ name })) } };
    },
    listWorkflowRuns: async ({ workflow_id, branch, event, status, created, page, per_page }) => {
      assert.equal(workflow_id, 43);
      assert.equal(branch, 'main');
      assert.equal(event, 'workflow_dispatch');
      assert.equal(per_page, 100);
      const [lo, hi] = created.split('..').map(Date.parse);
      assert.ok(lo >= Date.parse(parent.run_started_at) && hi <= Date.parse(parent.updated_at) + 60000);
      const found = [...rows.values()].filter(run => {
        const when = Date.parse(run.created_at || parent.run_started_at);
        return listed(run, elapsed) && run.status === status && lo <= when && when <= hi;
      });
      return { data: { total_count: found.length, workflow_runs: found.slice((page-1)*100, page*100) } };
    },
    getWorkflowRun: async ({ run_id }) => {
      if (!rows.has(run_id)) throw Object.assign(new Error('missing'), { status: 404 });
      return { data: { ...rows.get(run_id) } };
    },
    listJobsForWorkflowRunAttempt: async ({ run_id, attempt_number }) => {
      assert.equal(attempt_number, 1);
      const jobs = rows.get(run_id)?.fixtureJobs || [];
      return { data: { total_count: jobs.length, jobs } };
    },
    cancelWorkflowRun: async ({ run_id }) => { calls.push(['cancel', run_id, elapsed]); },
  };
  const core = { info() {}, summary: {
    addHeading() { return this; }, addRaw() { return this; }, async write() {},
  } };
  const context = { eventName: 'workflow_run', repo: { owner: 'matrixorigin', repo: 'matrixone' },
    payload: { action: 'completed', workflow_run: { id: parent.id, run_attempt: parent.run_attempt,
      head_sha: parent.head_sha } } };
  const options = { github: { rest: { actions }, request: async (route, { run_id }) => {
    assert.equal(route, 'POST /repos/{owner}/{repo}/actions/runs/{run_id}/force-cancel');
    calls.push(['force', run_id, elapsed]);
  } }, context, core, wait: async ms => { elapsed += ms; if (tick) tick(rows, parent, elapsed); } };
  return { rows, parent, child, calls, options, run: () => releaseCompletedCoverageWorkers(options) };
}

test('congested standby discovery paginates and only cancels the owned child', async () => {
  for (const n of [101, 1001]) {
    const h = coverageHarness({ children: [] });
    for (let i = 0; i < n; i++) {
      h.rows.set(i+1, h.child(i+1, 0, 'cs', { display_title: 'another parent',
        created_at: new Date(Date.parse(h.parent.run_started_at) + i*1000).toISOString() }));
    }
    h.rows.set(9900, h.child(9900, 1, 'idc'));
    h.rows.set(9901, h.child(9901, 1, 'cs', {created_at: '2020-01-01T00:00:00Z'}));
    await h.run();
    assert.deepEqual(h.calls.map(([kind,id]) => [kind,id]), [['cancel',9900],['force',9900]]);
  }
});

test('completed parent attempt releases only its queued and running standby children', async () => {
  const h = coverageHarness();
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 700, 10000], ['cancel', 701, 10000],
    ['force', 700, 130000], ['force', 701, 130000]]);
});

test('short worker names never authorize cleanup without the original exact identity', async () => {
  const h = coverageHarness({ children: [] });
  const title = h.child(700, 0, 'cs').display_title;
  for (const [id, pipeline, overrides] of [
    [700, title, {}],
    [701, title.replace(' / 600/', ' / 601/'), {}],
    [702, title, { run_id: 700 }],
    [703, title, { conclusion: 'success' }],
    [704, null, {}],
    [705, title.replace('/2/0/', '/1/0/'), {}],
    [706, title.replace('/2/0/', '/2/1/'), {}],
    [708, 'unexpanded marker', {}],
    [709, title, { run_attempt: 2 }],
  ]) {
    const run = h.child(id, 0, 'cs', { display_title: 'GROUP0' });
    run.fixtureJobs = pipeline ? [{ name: pipeline + ' / Run identity', run_id: id,
      run_attempt: 1, status: 'completed', conclusion: 'skipped', ...overrides }] : [];
    h.rows.set(id, run);
  }
  const group1 = h.child(707, 1, 'idc');
  h.rows.set(707, { ...group1, display_title: 'GROUP1', fixtureJobs: [{
    name: group1.display_title + ' / Run identity', run_id: 707,
    run_attempt: 1, status: 'completed', conclusion: 'skipped',
  }] });
  await assert.rejects(h.run(), err => {
    for (const id of [702, 703, 704, 706, 708, 709]) assert.match(err.message, new RegExp(id + ':'));
    for (const id of [700, 701, 705, 707]) assert.doesNotMatch(err.message, new RegExp(id + ':'));
    return true;
  });
  assert.deepEqual(h.calls.map(([kind, id]) => [kind, id]),
    [['cancel', 700], ['cancel', 707], ['force', 700], ['force', 707]]);
});

test('queued and running short workers wait for identity together, without delaying proven siblings', async () => {
  const h = coverageHarness({ children: [], tick(rows, _parent, elapsed) {
    if (elapsed !== 15000) return;
    for (const [id, group, pool] of [[710, 0, 'cs'], [711, 1, 'idc'], [712, 0, 'cs']]) {
      const title = h.child(id, group, pool).display_title;
      rows.get(id).fixtureJobs = [{ name: (id === 712 ? title.replace('600/2/', '601/2/') : title) +
        ' / Run identity', run_id: id, run_attempt: 1, status: 'completed', conclusion: 'skipped' }];
    }
  } });
  for (const [id, group, pool] of [[710, 0, 'cs'], [711, 1, 'idc'], [712, 0, 'cs']]) {
    h.rows.set(id, h.child(id, group, pool, { display_title: 'GROUP' + group,
      status: group ? 'in_progress' : 'queued' }));
  }
  h.rows.set(713, h.child(713, 1, 'cs'));
  await h.run();
  assert.deepEqual(h.calls.filter(([op]) => op === 'cancel'),
    [['cancel', 713, 10000], ['cancel', 710, 15000], ['cancel', 711, 15000]]);
  assert.deepEqual(h.calls.filter(([op]) => op === 'force').map(([, id]) => id), [713, 710, 711]);
});

test('permanently missing and ambiguous identities fail visibly without cancellation', async () => {
  const h = coverageHarness({ children: [] });
  const run = h.child(720, 0, 'cs');
  const marker = { name: run.display_title + ' / Run identity', run_id: 721, run_attempt: 1,
    status: 'completed', conclusion: 'skipped' };
  h.rows.set(720, { ...run, display_title: 'GROUP0' });
  h.rows.set(721, { ...run, id: 721, display_title: 'GROUP0', fixtureJobs: [marker, marker] });
  const actions = h.options.github.rest.actions, list = actions.listJobsForWorkflowRunAttempt;
  const reads = [];
  actions.listJobsForWorkflowRunAttempt = args => { reads.push(args.run_id); return list(args); };
  await assert.rejects(h.run(), err => {
    assert.match(err.message, /720:.*identity/i);
    assert.match(err.message, /721:.*identity/i);
    return true;
  });
  assert.deepEqual(reads, [720, 721, 720, 720]);
  assert.deepEqual(h.calls, []);
});

test('a previously verified short worker cannot be force-cancelled with a missing marker', async () => {
  const h = coverageHarness({ children: [], tick(rows) {
    if (h.calls.length) rows.get(722).fixtureJobs = [];
  } });
  const run = h.child(722, 1, 'idc');
  h.rows.set(722, { ...run, display_title: 'GROUP1', fixtureJobs: [{
    name: run.display_title + ' / Run identity', run_id: 722, run_attempt: 1,
    status: 'completed', conclusion: 'skipped',
  }] });
  await assert.rejects(h.run(), /722:.*identity/i);
  assert.deepEqual(h.calls, [['cancel', 722, 10000]]);
});

test('a temporary missing marker during grace does not fail a completed teardown', async () => {
  const h = coverageHarness({ children: [], tick(rows, _parent, elapsed) {
    if (elapsed === 20000) rows.get(724).fixtureJobs = [];
    if (elapsed === 30000) rows.get(724).status = 'completed';
  } });
  const run = h.child(724, 1, 'idc');
  h.rows.set(724, { ...run, display_title: 'GROUP1', fixtureJobs: [{
    name: run.display_title + ' / Run identity', run_id: 724, run_attempt: 1,
    status: 'completed', conclusion: 'skipped',
  }] });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 724, 10000]]);
});

test('a deferred short worker is not cancelled if the parent no longer matches', async () => {
  const h = coverageHarness({ children: [], tick(rows, parent, elapsed) {
    if (elapsed !== 15000) return;
    parent.head_sha = 'f'.repeat(40);
    rows.get(723).fixtureJobs[0].status = 'completed';
    rows.get(723).fixtureJobs[0].conclusion = 'skipped';
  } });
  const run = h.child(723, 0, 'cs');
  h.rows.set(723, { ...run, display_title: 'GROUP0', fixtureJobs: [{
    name: run.display_title + ' / Run identity', run_id: 723, run_attempt: 1,
    status: 'queued', conclusion: null,
  }] });
  await h.run();
  assert.deepEqual(h.calls, []);
});

test('admitted worker absent from all list scans is cancelled by its paginated receipt', async () => {
  const h = coverageHarness({ listed: (_run, elapsed) => elapsed >= 15000,
    receipts: [...Array(100).fill('irrelevant'), 'coverage-admit-701', 'coverage-admit-701'],
    tick(rows, _p, elapsed) { if (elapsed === 20000) rows.get(701).status = 'completed'; } });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 701, 10000]]);
});

test('receipt names cannot authorize cancellation of unrelated runs', async () => {
  const receipts = ['coverage-admit-404', 'coverage-admit-801', 'coverage-admit-802',
    'coverage-admit-803', 'coverage-admit-804', 'coverage-admit-0700', 'coverage-admit-700-extra',
    'coverage-admit-9007199254740992'];
  const h = coverageHarness({ receipts, listed: () => false });
  const base = h.child(700, 0, 'cs');
  for (const [id, change] of [[801, { display_title: base.display_title.replace('600/2/', '600/1/') }],
    [802, { workflow_id: 99 }], [803, { status: 'completed' }],
    [804, { display_title: base.display_title.replace('600/2/', '602/2/') }]]) {
    h.rows.set(id, { ...base, id, ...change });
  }
  await h.run();
  assert.deepEqual(h.calls, []);
});

test('receipt and listing identify the same child only once', async () => {
  const h = coverageHarness({ receipts: ['coverage-admit-701'] });
  await h.run();
  assert.equal(h.calls.filter(([action, id]) => action === 'cancel' && id === 701).length, 1);
});

test('an unreadable old receipt does not strand an admitted current worker', async () => {
  const h = coverageHarness({ listed: () => false,
    receipts: ['coverage-admit-800', 'coverage-admit-701'] });
  const actions = h.options.github.rest.actions;
  const get = actions.getWorkflowRun;
  actions.getWorkflowRun = async args => {
    if (args.run_id === 800) throw Object.assign(new Error('old receipt unavailable'), { status: 502 });
    return get(args);
  };
  await assert.rejects(h.run(), /800: old receipt unavailable/);
  assert.deepEqual(h.calls, [['cancel', 701, 10000], ['force', 701, 130000]]);
});

test('a worker API failure during cancellation, grace or escalation does not block its sibling', async () => {
  for (const stage of ['cancel', 'poll', 'force']) {
    const h = coverageHarness();
    const actions = h.options.github.rest.actions;
    const api = stage === 'force' ? h.options.github : actions;
    const method = { cancel: 'cancelWorkflowRun', poll: 'getWorkflowRun', force: 'request' }[stage];
    const original = api[method];
    api[method] = async (...args) => {
      if (args.at(-1).run_id === 700 && (stage !== 'poll' || h.calls.length > 0)) {
        throw new Error('worker API unavailable');
      }
      return original(...args);
    };
    await assert.rejects(h.run(), /worker API unavailable/);
    assert.deepEqual(h.calls.filter(([, id]) => id === 701),
      [['cancel', 701, 10000], ['force', 701, 130000]], stage);
    assert.ok(!h.calls.some(([kind, id]) => kind === 'force' && id === 700), stage);
  }
});

test('failure to verify the common parent stops all further cancellation', async () => {
  const h = coverageHarness();
  const actions = h.options.github.rest.actions;
  const parent = actions.getWorkflowRunAttempt;
  actions.getWorkflowRunAttempt = async args => {
    if (h.calls.length) throw new Error('cannot verify parent');
    return parent(args);
  };
  await assert.rejects(h.run(), /cannot verify parent/);
  assert.deepEqual(h.calls, [['cancel', 700, 10000]]);
});

test('failed discovery does not strand a verified child, or report successful cleanup', async () => {
  for (const api of ['listWorkflowRuns', 'listWorkflowRunArtifacts']) {
    const h = coverageHarness({ receipts: ['coverage-admit-701'] });
    h.options.github.rest.actions[api] = async () => { throw new Error('discovery unavailable'); };
    await assert.rejects(h.run(), /discovery unavailable/);
    assert.equal(h.calls.filter(([action, id]) => action === 'cancel' && id === 701).length, 1);
  }
});

test('standby cleanup excludes other parent attempts, heads, workflows and reruns', async () => {
  const h = coverageHarness({ children: [] });
  const base = h.child(700, 0, 'cs');
  const others = [
    { display_title: base.display_title.replace('/2/0/', '/1/0/') },
    { display_title: base.display_title.replace(' / 600/', ' / 601/') },
    { display_title: base.display_title.replace('a'.repeat(40), 'c'.repeat(40)) },
    { workflow_id: 99 }, { path: '.github/workflows/other.yaml' },
    { event: 'push' }, { head_branch: 'weilu-runner-test' }, { run_attempt: 2 },
    { status: 'completed' },
  ];
  others.forEach((override, index) => h.rows.set(800 + index, { ...base, id: 800 + index, ...override }));
  await h.run();
  assert.deepEqual(h.calls, []);
});

test('standby cleanup catches late-indexed child and avoids escalating finished child', async () => {
  const h = coverageHarness({ children: [], tick(rows, _parent, elapsed) {
    if (elapsed === 5000) rows.set(700, h.child(700, 0, 'cs'));
    if (elapsed === 20000) rows.get(700).status = 'completed';
  } });
  await h.run();
  assert.deepEqual(h.calls, [['cancel', 700, 10000]]);
});

test('standby cleanup fails closed for active or mismatched parent attempt', async () => {
  for (const override of [{ status: 'in_progress', conclusion: null },
    { event: 'push' }, { path: '.github/workflows/other.yaml' }]) {
    const h = coverageHarness({ parentOverride: override });
    await assert.rejects(h.run(), /identity mismatch/);
    assert.deepEqual(h.calls, []);
  }
  const stale = coverageHarness();
  stale.options.context.payload.workflow_run.run_attempt = 1;
  await assert.rejects(stale.run(), /identity mismatch/);
  assert.deepEqual(stale.calls, []);
});
