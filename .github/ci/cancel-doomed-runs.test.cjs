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
const {
  CI_WORKFLOWS,
  DOOMED_CONCLUSIONS,
  JOB_POLICIES,
  SUPPORTED_CI_REVISIONS,
  authorizeFailure,
  cancelDoomedRuns,
  failedRequiredJobs,
  parseRunMetadata,
  renderSummary,
} = require('./cancel-doomed-runs.cjs');
const {
  ALWAYS_REQUIRED_JOBS,
  REQUIRED_JOBS_BY_SCOPE,
  requiredJobUnion,
} = require('./change-scope.cjs');

const baseSha = 'a'.repeat(40);
const headSha = 'b'.repeat(40);
const supportedCI = SUPPORTED_CI_REVISIONS[0];

function run(overrides = {}) {
  return {
    id: 100,
    event: 'pull_request_target',
    path: '.github/workflows/entrypoint.yaml',
    status: 'in_progress',
    run_attempt: 2,
    head_sha: headSha,
    display_title: `CI_REQUIRED/v1 pr=42 base=main base_sha=${baseSha} head_sha=${headSha}`,
    html_url: 'https://github.example/runs/100',
    referenced_workflows: [
      { path: CI_WORKFLOWS.core, sha: supportedCI },
      { path: CI_WORKFLOWS.coverageUT, sha: supportedCI },
      { path: CI_WORKFLOWS.compose, sha: supportedCI },
      { path: CI_WORKFLOWS.standalone, sha: supportedCI },
      { path: CI_WORKFLOWS.coverageMerge, sha: supportedCI },
    ],
    ...overrides,
  };
}

function job(name, conclusion = 'failure', runAttempt = 2) {
  return { name, conclusion, run_attempt: runAttempt, status: 'completed' };
}

function githubMock({
  runs = [run()],
  jobs = new Map([[100, [job('CI preflight')]]]),
  currentRuns = new Map(),
  blobByRef = new Map(),
  jobsErrorFor = new Set(),
  cancelError,
} = {}) {
  const calls = { cancelled: [], jobRequests: [] };
  const listWorkflowRuns = async () => {};
  const listJobsForWorkflowRun = async () => {};
  const github = {
    paginate: async (method, params) => {
      if (method === listWorkflowRuns) return runs;
      assert.equal(method, listJobsForWorkflowRun);
      calls.jobRequests.push(params);
      if (jobsErrorFor.has(params.run_id)) throw new Error('jobs unavailable');
      return jobs.get(params.run_id) || [];
    },
    rest: {
      actions: {
        listWorkflowRuns,
        listJobsForWorkflowRun,
        getWorkflowRun: async ({ run_id }) => ({ data: currentRuns.get(run_id) || runs.find(r => r.id === run_id) }),
        cancelWorkflowRun: async ({ run_id }) => {
          if (cancelError) throw cancelError;
          calls.cancelled.push(run_id);
        },
      },
      repos: {
        getContent: async ({ path, ref }) => ({ data: { sha: blobByRef.get(`${ref}:${path}`) || `blob:${path}` } }),
      },
    },
  };
  return { github, calls };
}

test('run metadata is strict and records the trigger-time base', () => {
  assert.deepEqual(parseRunMetadata(run().display_title), {
    policyVersion: 'v1', pullNumber: 42, baseRef: 'main', baseSha, headSha,
  });
  for (const title of [
    '',
    `CI_REQUIRED/v2 pr=42 base=main base_sha=${baseSha} head_sha=${headSha}`,
    `CI_REQUIRED/v1 pr=0 base=main base_sha=${baseSha} head_sha=${headSha}`,
    `CI_REQUIRED/v1 pr=42 base=main base_sha=short head_sha=${headSha}`,
    `prefix CI_REQUIRED/v1 pr=42 base=main base_sha=${baseSha} head_sha=${headSha}`,
  ]) assert.equal(parseRunMetadata(title), null, title);
});

test('only exact required jobs with terminal doomed conclusions qualify', () => {
  for (const conclusion of DOOMED_CONCLUSIONS) {
    assert.equal(failedRequiredJobs([job('CI preflight', conclusion)], 2).length, 1, conclusion);
  }
  for (const candidate of [
    job('Matrixone Utils CI / pr-size-label'),
    job('Matrixone Shared Build / Build MatrixOne (shared)'),
    job('Matrixone Upgrade CI / Compatibility Test With Target on Linux/x64(LAUNCH)'),
    job('CodeQL'),
    job('MO Checkin Regression On TKE / BUILD MO DOCKER IMAGE'),
    job('Matrixone CI execution / unknown future child'),
    job('CI preflight', 'skipped'),
    job('CI preflight', 'neutral'),
    job('CI preflight', 'failure', 1),
    { ...job('CI preflight'), status: 'in_progress' },
  ]) assert.equal(failedRequiredJobs([candidate], 2).length, 0, candidate.name);
});

test('external failures require the reviewed immutable workflow revision', () => {
  const failure = {
    job: job('Matrixone CI execution / SCA Test on Linux/arm64'),
    policy: JOB_POLICIES.get('Matrixone CI execution / SCA Test on Linux/arm64'),
  };
  assert.equal(authorizeFailure(run(), failure).authorized, true);
  const unknown = run({ referenced_workflows: [{ path: CI_WORKFLOWS.core, sha: 'c'.repeat(40) }] });
  assert.match(authorizeFailure(unknown, failure).error, /unsupported/);
  assert.match(authorizeFailure(run({ referenced_workflows: [] }), failure).error, /missing unique/);
});

test('scanner cancels an in-progress attempt with an authorized local failure', async () => {
  const { github, calls } = githubMock();
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, [100]);
  assert.equal(result.cancelled.length, 1);
  assert.equal(result.errors.length, 0);
  assert.equal(result.cancelled[0].failedJob, 'CI preflight');
});

test('scanner authorizes an exact external failure and ignores unrelated failures', async () => {
  const jobs = new Map([[100, [
    job('Matrixone Utils CI / pr-size-label'),
    job('Matrixone CI execution / SCA Test on Linux/arm64'),
  ]]]);
  const { github, calls } = githubMock({ jobs });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, [100]);
  assert.equal(result.cancelled[0].failedJob, 'Matrixone CI execution / SCA Test on Linux/arm64');
});

test('old metadata, head mismatch, and trigger-time 3.0 base are skipped', async () => {
  const runs = [
    run({ id: 1, display_title: 'legacy title' }),
    run({ id: 2, head_sha: 'c'.repeat(40) }),
    run({ id: 3, display_title: `CI_REQUIRED/v1 pr=42 base=3.0-dev base_sha=${baseSha} head_sha=${headSha}` }),
  ];
  const { github, calls } = githubMock({ runs, jobs: new Map() });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, []);
  assert.equal(result.skippedMetadata, 2);
  assert.equal(result.skippedBase, 1);
});

test('local policy drift rejects every cancellation source', async () => {
  const blobs = new Map([[`${baseSha}:.github/ci/change-scope.cjs`, 'old-policy']]);
  const { github, calls } = githubMock({ blobByRef: blobs });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, []);
  assert.match(result.errors[0].detail, /local cancellation policy revision differs/);
});

test('unsupported external policy is visible and fails closed', async () => {
  const badRun = run({
    referenced_workflows: [{ path: CI_WORKFLOWS.core, sha: 'd'.repeat(40) }],
  });
  const jobs = new Map([[100, [job('Matrixone CI execution / UT Test on Ubuntu/x86')]]]);
  const { github, calls } = githubMock({ runs: [badRun], jobs });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, []);
  assert.match(result.errors[0].detail, /unsupported/);
});

test('latest job pages cannot carry an old attempt failure into a new attempt', async () => {
  const latestRun = run({ run_attempt: 3 });
  const jobs = new Map([[100, [job('CI preflight', 'failure', 2), job('CI preflight', null, 3)]]]);
  const { github, calls } = githubMock({ runs: [latestRun], jobs });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, []);
  assert.equal(result.healthy, 1);
  assert.equal(calls.jobRequests[0].filter, 'latest');
});

test('final status and attempt check rejects a stale decision', async () => {
  for (const current of [
    run({ status: 'completed', conclusion: 'failure' }),
    run({ run_attempt: 3 }),
    run({ head_sha: 'c'.repeat(40) }),
  ]) {
    const { github, calls } = githubMock({ currentRuns: new Map([[100, current]]) });
    const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
    assert.deepEqual(calls.cancelled, []);
    assert.equal(result.raced.length, 1);
  }
});

test('dry run reports without mutation and cancellation conflict is a race', async () => {
  const dry = githubMock();
  const dryResult = await cancelDoomedRuns({
    github: dry.github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy', dryRun: true,
  });
  assert.deepEqual(dry.calls.cancelled, []);
  assert.equal(dryResult.wouldCancel.length, 1);

  const conflict = githubMock({ cancelError: Object.assign(new Error('Conflict'), { status: 409 }) });
  const conflictResult = await cancelDoomedRuns({
    github: conflict.github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy',
  });
  assert.equal(conflictResult.raced.length, 1);
  assert.equal(conflictResult.errors.length, 0);
});

test('one run API failure is reported without preventing another safe cancellation', async () => {
  const runs = [run({ id: 100 }), run({ id: 200, html_url: 'https://github.example/runs/200' })];
  const jobs = new Map([[200, [job('CI preflight')]]]);
  const { github, calls } = githubMock({ runs, jobs, jobsErrorFor: new Set([100]) });
  const result = await cancelDoomedRuns({ github, owner: 'matrixorigin', repo: 'matrixone', policyRef: 'policy' });
  assert.deepEqual(calls.cancelled, [200]);
  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0].detail, /jobs unavailable/);
});

test('workflow has a five-minute trusted, bounded, least-privilege controller', () => {
  const watchdog = readFileSync(`${__dirname}/../workflows/cancel-doomed-pr-ci.yaml`, 'utf8');
  assert.match(watchdog, /cron: '\*\/5 \* \* \* \*'/);
  assert.match(watchdog, /cancel-in-progress: false/);
  assert.match(watchdog,
    /github\.ref == format\('refs\/heads\/\{0\}', github\.event\.repository\.default_branch\)/);
  assert.match(watchdog, /^permissions: \{\}$/m);
  assert.match(watchdog, /^      actions: write$/m);
  assert.match(watchdog, /^      contents: read$/m);
  assert.doesNotMatch(watchdog, /pull-requests: write|issues: write|contents: write/);
  assert.match(watchdog, /persist-credentials: false/);
  assert.match(watchdog, /sparse-checkout: \.github\/ci/);
  assert.match(watchdog, /timeout-minutes: 5/);

  const entrypoint = readFileSync(`${__dirname}/../workflows/entrypoint.yaml`, 'utf8');
  assert.match(entrypoint, /^run-name: CI_REQUIRED\/v1 pr=\$\{\{ github\.event\.pull_request\.number \}\} base=/m);
});

test('summary identifies the cancellation owner and remains bounded', () => {
  const result = {
    dryRun: false, scanned: 1, eligible: 1, healthy: 0,
    cancelled: [{ runId: 100, runUrl: 'https://github.example/runs/100', pullNumber: 42,
      attempt: 2, failedJob: 'CI preflight', conclusion: 'failure' }],
    wouldCancel: [], raced: [], errors: [],
  };
  const summary = renderSummary(result);
  assert.match(summary, /\| #42 \|/);
  assert.match(summary, /CI preflight/);
  assert.match(summary, /100.*\/2/);
});

test('required-job policy matches the current entrypoint callers', () => {
  assert.deepEqual(ALWAYS_REQUIRED_JOBS, ['preflight']);
  assert.deepEqual(REQUIRED_JOBS_BY_SCOPE.bvt, ['matrixone-compose-ci', 'matrixone-standalone-ci']);
  assert.ok(requiredJobUnion().includes('preflight'));
  assert.ok(!requiredJobUnion().includes('bvt-group-plan'));
  assert.ok(JOB_POLICIES.has('CI preflight'));
  assert.ok(!JOB_POLICIES.has('CHECK PR VALID'));
});
