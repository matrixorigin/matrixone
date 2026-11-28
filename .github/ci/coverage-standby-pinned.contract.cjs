'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { readFileSync } = require('node:fs');
const { join, resolve } = require('node:path');
const test = require('node:test');
const { releaseCompletedCoverageWorkers } = require('./cancel-superseded.cjs');

test('pinned failed-group retry and completed-attempt cleanup isolate same-name workers', async () => {
  const dir = resolve(process.env.COVERAGE_COMPANION_DIR);
  const pin = execFileSync('git', ['-C', dir, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  assert.match(pin, /^[a-f0-9]{40}$/);
  const workflow = file => readFileSync(join(__dirname, '../workflows', file), 'utf8');
  for (const [file, reusable] of [
    ['entrypoint.yaml', 'coverage-ut-standby.yaml'],
    ['coverage-standby-worker.yaml', 'coverage-worker-cs.yaml'],
    ['coverage-standby-worker.yaml', 'coverage-worker-idc.yaml'],
  ]) {
    assert.ok(workflow(file).includes(`uses: matrixorigin/CI/.github/workflows/${reusable}@${pin}\n`),
      `${file} must use the checked companion pin for ${reusable}`);
  }
  assert.ok(workflow('coverage-standby-worker.yaml').includes('uses: ./.github/workflows/coverage-standby-identity.yaml'));

  const { parent, runs, jobs, artifacts, retry } = JSON.parse(execFileSync('python3',
    ['-I', join(__dirname, 'coverage-standby-pinned-fixture.py'), dir], { encoding: 'utf8' }));
  assert.deepEqual(retry, { child: 55, group: 0, attempt: 2, dispatch_count: 1 });
  assert.deepEqual([52, 53, 55].map(id => runs[id].display_title), ['GROUP0', 'GROUP0', 'GROUP0']);

  const markerReads = new Map(), cancelled = [];
  const actions = {
    getWorkflow: async ({ workflow_id }) => ({ data: {
      id: { 'entrypoint.yaml': 7, 'coverage-standby-worker.yaml': 8 }[workflow_id],
    } }),
    getWorkflowRunAttempt: async ({ run_id, attempt_number }) => {
      assert.equal(run_id, parent.id);
      assert.equal(attempt_number, 1); // cleanup the failed attempt, not the new attempt 2
      return { data: parent };
    },
    listWorkflowRuns: async ({ status }) => {
      const rows = Object.values(runs).filter(run => run.status === status);
      return { data: { workflow_runs: rows, total_count: rows.length } };
    },
    listWorkflowRunArtifacts: async () => ({ data: { artifacts } }),
    getWorkflowRun: async ({ run_id }) => ({ data: runs[run_id] }),
    listJobsForWorkflowRunAttempt: async ({ run_id, attempt_number }) => {
      assert.equal(attempt_number, 1); // the dedicated worker has its own attempt number
      const n = (markerReads.get(run_id) || 0) + 1;
      markerReads.set(run_id, n);
      const rows = run_id === 52 && n === 1 ? jobs[run_id].slice(1) : jobs[run_id];
      return { data: { jobs: rows, total_count: rows.length } };
    },
    cancelWorkflowRun: async ({ run_id }) => {
      cancelled.push(run_id);
      assert.equal(run_id, 52);
      assert.equal(markerReads.get(run_id), 2);
      runs[run_id] = { ...runs[run_id], status: 'completed', conclusion: 'cancelled' };
    },
  };
  const summary = { addHeading() { return this; }, addRaw() { return this; }, async write() {} };
  await releaseCompletedCoverageWorkers({
    github: { rest: { actions }, request: async () => assert.fail('unexpected force cancel') },
    context: { eventName: 'workflow_run', repo: { owner: 'matrixorigin', repo: 'matrixone' },
      payload: { action: 'completed', workflow_run: parent } },
    core: { info() {}, summary }, wait: async () => {},
  });
  assert.deepEqual(cancelled, [52]);
  for (const id of [53, 55]) assert.equal(runs[id].status, 'in_progress');
  assert.equal(runs[54].conclusion, 'success');
  console.log(JSON.stringify({ companion: pin, retry, cancelled, preserved: [53, 54, 55],
    queuedMarkerReads: markerReads.get(52) }));
});
