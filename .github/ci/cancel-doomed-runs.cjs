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

const { requiredJobUnion } = require('./change-scope.cjs');

const POLICY_VERSION = 'v1';
const ENTRYPOINT_WORKFLOW = 'entrypoint.yaml';
const LOCAL_POLICY_PATHS = Object.freeze([
  '.github/workflows/entrypoint.yaml',
  '.github/ci/change-scope.cjs',
]);
const DOOMED_CONCLUSIONS = new Set([
  'failure',
  'cancelled',
  'timed_out',
  'action_required',
  'startup_failure',
  'stale',
]);

// A reusable workflow is cancellation authority only after its exact revision
// and the named job's failure propagation have been reviewed. Unknown revisions
// fail closed until this policy is updated.
const SUPPORTED_CI_REVISIONS = Object.freeze([
  '2ab1af3a85ca3c4e4eab7483201aca3e1913dd99',
]);

const CI_WORKFLOWS = Object.freeze({
  core: 'matrixorigin/CI/.github/workflows/ci.yaml@main',
  coverageUT: 'matrixorigin/CI/.github/workflows/coverage-ut.yaml@main',
  compose: 'matrixorigin/CI/.github/workflows/e2e-compose-parallel.yaml@main',
  standalone: 'matrixorigin/CI/.github/workflows/e2e-standalone-parallel.yaml@main',
  coverageMerge: 'matrixorigin/CI/.github/workflows/coverage-merge.yaml@main',
});

const JOB_POLICIES = new Map([
  ['CI preflight', { logicalJob: 'preflight' }],
  ['Documentation whitespace check', { logicalJob: 'docs-check' }],
  ['Matrixone CI execution / UT Test on Ubuntu/x86',
    { logicalJob: 'matrixone-ci', workflow: CI_WORKFLOWS.core }],
  ['Matrixone CI execution / SCA Test on Linux/arm64',
    { logicalJob: 'matrixone-ci', workflow: CI_WORKFLOWS.core }],
  ['Matrixone UT Coverage execution / Check coverage eligibility',
    { logicalJob: 'matrixone-ut-coverage', workflow: CI_WORKFLOWS.coverageUT }],
  ['Matrixone UT Coverage execution / UT Coverage on Ubuntu/x86',
    { logicalJob: 'matrixone-ut-coverage', workflow: CI_WORKFLOWS.coverageUT }],
  ['Matrixone Compose CI execution / multi cn e2e bvt test docker compose(PROXY)',
    { logicalJob: 'matrixone-compose-ci', workflow: CI_WORKFLOWS.compose }],
  ['Matrixone Standlone CI execution / multi CN e2e BVT Test on Linux/x64(COMPOSE, PESSIMISTIC)',
    { logicalJob: 'matrixone-standalone-ci', workflow: CI_WORKFLOWS.standalone }],
  ['Matrixone Coverage execution / Coverage',
    { logicalJob: 'matrixone-coverage-merge', workflow: CI_WORKFLOWS.coverageMerge }],
]);

function validatePolicyCoverage() {
  const required = new Set(requiredJobUnion());
  const covered = new Set();
  for (const policy of JOB_POLICIES.values()) {
    if (!required.has(policy.logicalJob)) {
      throw new Error(`Cancellation policy names non-required job ${policy.logicalJob}`);
    }
    covered.add(policy.logicalJob);
  }
  for (const logicalJob of required) {
    if (!covered.has(logicalJob)) {
      throw new Error(`Cancellation policy lacks required job ${logicalJob}`);
    }
  }
}
validatePolicyCoverage();

function parseRunMetadata(displayTitle) {
  if (typeof displayTitle !== 'string') return null;
  const match = displayTitle.match(
    /^CI_REQUIRED\/v1 pr=([1-9][0-9]*) base=([^\s]+) base_sha=([0-9a-f]{40}) head_sha=([0-9a-f]{40})$/,
  );
  if (!match) return null;
  return {
    policyVersion: POLICY_VERSION,
    pullNumber: Number(match[1]),
    baseRef: match[2],
    baseSha: match[3],
    headSha: match[4],
  };
}

function failedRequiredJobs(jobs, runAttempt) {
  return jobs.flatMap(job => {
    const policy = JOB_POLICIES.get(job.name);
    if (!policy || job.run_attempt !== runAttempt || job.status !== 'completed' ||
        !DOOMED_CONCLUSIONS.has(job.conclusion)) return [];
    return [{ job, policy }];
  });
}

function referencedWorkflow(run, path) {
  const matches = (run.referenced_workflows || []).filter(workflow => workflow.path === path);
  return matches.length === 1 ? matches[0] : null;
}

function authorizeFailure(run, failure) {
  if (!failure.policy.workflow) return { authorized: true };
  const workflow = referencedWorkflow(run, failure.policy.workflow);
  if (!workflow) {
    return { authorized: false, error: `missing unique referenced workflow ${failure.policy.workflow}` };
  }
  if (!SUPPORTED_CI_REVISIONS.includes(workflow.sha)) {
    return {
      authorized: false,
      error: `unsupported ${failure.policy.workflow} revision ${workflow.sha || 'missing'}`,
    };
  }
  return { authorized: true };
}

async function policyBlobs(github, owner, repo, ref) {
  const entries = await Promise.all(LOCAL_POLICY_PATHS.map(async path => {
    const response = await github.rest.repos.getContent({ owner, repo, path, ref });
    if (Array.isArray(response.data) || typeof response.data?.sha !== 'string') {
      throw new Error(`missing policy blob ${path} at ${ref}`);
    }
    return [path, response.data.sha];
  }));
  return Object.fromEntries(entries);
}

function samePolicyBlobs(left, right) {
  return LOCAL_POLICY_PATHS.every(path => left[path] === right[path]);
}

function newResult(dryRun) {
  return {
    dryRun,
    scanned: 0,
    eligible: 0,
    healthy: 0,
    skippedMetadata: 0,
    skippedBase: 0,
    cancelled: [],
    wouldCancel: [],
    raced: [],
    errors: [],
  };
}

function runRecord(run, metadata, failure, detail) {
  return {
    runId: run.id,
    runUrl: run.html_url,
    pullNumber: metadata?.pullNumber,
    attempt: run.run_attempt,
    failedJob: failure?.job.name,
    conclusion: failure?.job.conclusion,
    detail,
  };
}

async function cancelDoomedRuns({ github, owner, repo, policyRef, dryRun = false }) {
  if (!policyRef) throw new Error('policyRef is required');
  const result = newResult(dryRun);
  const currentPolicy = await policyBlobs(github, owner, repo, policyRef);
  const basePolicies = new Map([[policyRef, currentPolicy]]);
  const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
    owner,
    repo,
    workflow_id: ENTRYPOINT_WORKFLOW,
    event: 'pull_request_target',
    status: 'in_progress',
    per_page: 100,
  });
  result.scanned = runs.length;

  for (const run of runs) {
    if (run.event !== 'pull_request_target' || run.status !== 'in_progress' ||
        (run.path && run.path !== '.github/workflows/entrypoint.yaml')) {
      result.skippedMetadata++;
      continue;
    }
    const metadata = parseRunMetadata(run.display_title);
    if (!metadata || metadata.headSha !== run.head_sha) {
      result.skippedMetadata++;
      continue;
    }
    if (metadata.baseRef === '3.0-dev') {
      result.skippedBase++;
      continue;
    }
    result.eligible++;

    try {
      let basePolicy = basePolicies.get(metadata.baseSha);
      if (!basePolicy) {
        basePolicy = await policyBlobs(github, owner, repo, metadata.baseSha);
        basePolicies.set(metadata.baseSha, basePolicy);
      }
      if (!samePolicyBlobs(currentPolicy, basePolicy)) {
        result.errors.push(runRecord(run, metadata, null, 'local cancellation policy revision differs'));
        continue;
      }

      const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
        owner,
        repo,
        run_id: run.id,
        filter: 'latest',
        per_page: 100,
      });
      const failures = failedRequiredJobs(jobs, run.run_attempt);
      if (failures.length === 0) {
        result.healthy++;
        continue;
      }

      let chosen;
      const authorizationErrors = [];
      for (const failure of failures) {
        const authorization = authorizeFailure(run, failure);
        if (authorization.authorized) {
          chosen = failure;
          break;
        }
        authorizationErrors.push(`${failure.job.name}: ${authorization.error}`);
      }
      if (!chosen) {
        result.errors.push(runRecord(run, metadata, failures[0], authorizationErrors.join('; ')));
        continue;
      }

      const current = (await github.rest.actions.getWorkflowRun({
        owner,
        repo,
        run_id: run.id,
      })).data;
      if (current.status !== 'in_progress' || current.run_attempt !== run.run_attempt ||
          current.head_sha !== metadata.headSha) {
        result.raced.push(runRecord(run, metadata, chosen, 'run completed or attempt changed'));
        continue;
      }

      const record = runRecord(run, metadata, chosen);
      if (dryRun) {
        result.wouldCancel.push(record);
        continue;
      }
      try {
        await github.rest.actions.cancelWorkflowRun({ owner, repo, run_id: run.id });
        result.cancelled.push(record);
      } catch (error) {
        if (error?.status === 409) {
          result.raced.push({ ...record, detail: 'workflow changed before cancellation' });
          continue;
        }
        throw error;
      }
    } catch (error) {
      result.errors.push(runRecord(run, metadata, null, error instanceof Error ? error.message : String(error)));
    }
  }
  return result;
}

function escapeTable(value) {
  return String(value ?? '').replaceAll('|', '\\|').replaceAll('\n', ' ');
}

function renderSummary(result) {
  const lines = [
    '### Doomed PR CI watchdog',
    '',
    `- Mode: ${result.dryRun ? 'dry run' : 'enforce'}`,
    `- Runs scanned: ${result.scanned}`,
    `- Eligible policy-v1 runs: ${result.eligible}`,
    `- Skipped legacy or invalid metadata: ${result.skippedMetadata}`,
    `- Skipped 3.0-dev runs: ${result.skippedBase}`,
    `- Cancelled: ${result.cancelled.length}`,
    `- Would cancel: ${result.wouldCancel.length}`,
    `- Races: ${result.raced.length}`,
    `- Errors: ${result.errors.length}`,
  ];
  const records = [
    ...result.cancelled.map(record => ['cancelled', record]),
    ...result.wouldCancel.map(record => ['would cancel', record]),
    ...result.raced.map(record => ['race', record]),
    ...result.errors.map(record => ['error', record]),
  ];
  if (records.length > 0) {
    lines.push('', '| Result | PR | Run/attempt | Failed required job | Conclusion | Detail |',
      '| --- | --- | --- | --- | --- | --- |');
    for (const [kind, record] of records.slice(0, 100)) {
      const run = record.runUrl ? `[${record.runId}](${record.runUrl})` : record.runId;
      lines.push(`| ${kind} | #${record.pullNumber || '?'} | ${run}/${record.attempt} | ` +
        `${escapeTable(record.failedJob || '')} | ${escapeTable(record.conclusion || '')} | ` +
        `${escapeTable(record.detail || '')} |`);
    }
    if (records.length > 100) lines.push('', `${records.length - 100} additional records omitted.`);
  }
  return `${lines.join('\n')}\n`;
}

module.exports = {
  CI_WORKFLOWS,
  DOOMED_CONCLUSIONS,
  ENTRYPOINT_WORKFLOW,
  JOB_POLICIES,
  LOCAL_POLICY_PATHS,
  POLICY_VERSION,
  SUPPORTED_CI_REVISIONS,
  authorizeFailure,
  cancelDoomedRuns,
  failedRequiredJobs,
  parseRunMetadata,
  renderSummary,
  samePolicyBlobs,
};
