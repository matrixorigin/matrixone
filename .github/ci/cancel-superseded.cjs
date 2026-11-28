// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
'use strict';

const active = new Set(['queued', 'in_progress', 'pending', 'waiting', 'requested']);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Match entrypoint's head-repository/branch concurrency identity, never branch
// name alone (two forks can have identically named branches). Run IDs order new
// runs; run_started_at also protects a newer rerun of an older run ID.
function isSuperseded(candidate, successor) {
  return active.has(candidate.status) && candidate.event === 'pull_request_target' &&
    candidate.workflow_id === successor.workflow_id &&
    Number.isSafeInteger(candidate.id) && candidate.id < successor.id &&
    !!successor.head_repository?.id &&
    candidate.head_repository?.id === successor.head_repository.id &&
    !!successor.head_branch && candidate.head_branch === successor.head_branch &&
    Date.parse(candidate.run_started_at) <= Date.parse(successor.created_at);
}

async function releaseSupersededRuns({ github, context, core, runId, wait = sleep }) {
  if (!/^[1-9][0-9]*$/.test(String(runId)) || !Number.isSafeInteger(Number(runId))) {
    throw new Error('Invalid successor run ID');
  }
  const repo = context.repo;
  const get = async id => (await github.rest.actions.getWorkflowRun({ ...repo, run_id: id })).data;
  const successor = await get(Number(runId));
  const workflow = (await github.rest.actions.getWorkflow({ ...repo, workflow_id: 'entrypoint.yaml' })).data;
  if (successor.workflow_id !== workflow.id || successor.event !== 'pull_request_target' ||
      !successor.head_repository?.id || !successor.head_branch) {
    throw new Error('Successor must be a MatrixOne ALL CI pull_request_target run');
  }
  if (!active.has(successor.status)) return;

  const matches = run => isSuperseded(run, successor);
  const candidates = await findActiveRuns(github, repo, workflow.id, successor.head_branch, matches);
  const stillCurrent = async () => {
    const current = await get(successor.id);
    return active.has(current.status) && current.run_attempt === successor.run_attempt;
  };
  await cancelMatchingRuns({ github, repo, core, get, candidates, matches, stillCurrent, wait });
}

// Both policies use the same bounded discovery, teardown grace and escalation.
async function findActiveRuns(github, repo, workflowId, branch, matches) {
  const candidates = new Map();
  // Bound discovery to active runs of this workflow and branch. Reject an
  // unexpectedly incomplete response rather than silently claiming recovery.
  for (const status of active) {
    const { data } = await github.rest.actions.listWorkflowRuns({
      ...repo, workflow_id: workflowId, branch,
      event: 'pull_request_target', status, per_page: 100,
    });
    if (data.total_count > 100) throw new Error('Too many active runs; inspect cancellation manually');
    for (const run of data.workflow_runs) {
      if (matches(run)) candidates.set(run.id, run);
    }
  }
  return candidates;
}

async function releaseClosedPullRequestRuns({ github, context, core, wait = sleep }) {
  const closed = context.payload.pull_request;
  if (context.payload.action !== 'closed' || !closed?.number || !closed.closed_at) {
    throw new Error('Expected a closed pull request event');
  }
  const repo = context.repo;
  const stillCurrent = async () => {
    const { data: pr } = await github.rest.pulls.get({ ...repo, pull_number: closed.number });
    if (pr.state !== 'closed' || pr.closed_at !== closed.closed_at ||
        !closed.head.repo?.id || pr.head.repo?.id !== closed.head.repo.id ||
        pr.head.ref !== closed.head.ref) return false;
    // A head branch can serve another open PR against a different base. The
    // Actions branch identity alone must not authorize cancelling its work.
    const { data: open } = await github.rest.pulls.list({
      ...repo, state: 'open', head: `${closed.head.repo.owner.login}:${closed.head.ref}`, per_page: 1,
    });
    return open.length === 0;
  };
  if (!await stillCurrent()) return;
  const workflow = (await github.rest.actions.getWorkflow({ ...repo, workflow_id: 'entrypoint.yaml' })).data;
  const matches = run => active.has(run.status) && run.event === 'pull_request_target' &&
    run.workflow_id === workflow.id && run.head_repository?.id === closed.head.repo.id &&
    run.head_branch === closed.head.ref &&
    Date.parse(run.created_at) <= Date.parse(closed.closed_at) &&
    Date.parse(run.run_started_at) <= Date.parse(closed.closed_at);
  const candidates = await findActiveRuns(github, repo, workflow.id, closed.head.ref, matches);
  const get = async id => (await github.rest.actions.getWorkflowRun({ ...repo, run_id: id })).data;
  await cancelMatchingRuns({ github, repo, core, get, candidates, matches, stillCurrent, wait });
}

class CoverageIdentityPending extends Error {}

// workflow_dispatch children do not inherit entrypoint's concurrency group.
// A separate default-branch controller is needed because the parent collector
// is itself cancelled when its parent terminates. Bind cleanup to the exact
// completed parent attempt and the child workflow identity; never cancel by
// PR branch name or a title substring alone.
async function releaseCompletedCoverageWorkers({ github, context, core, wait = sleep }) {
  const event = context.payload.workflow_run;
  if (context.eventName !== 'workflow_run' || context.payload.action !== 'completed' ||
      !Number.isSafeInteger(event?.id) || !Number.isSafeInteger(event?.run_attempt) ||
      event.run_attempt < 1) throw new Error('Expected a completed workflow run attempt');
  const repo = context.repo;
  const entry = (await github.rest.actions.getWorkflow({ ...repo, workflow_id: 'entrypoint.yaml' })).data;
  const parent = (await github.rest.actions.getWorkflowRunAttempt({
    ...repo, run_id: event.id, attempt_number: event.run_attempt,
  })).data;
  if (parent.id !== event.id || parent.run_attempt !== event.run_attempt ||
      parent.workflow_id !== entry.id || parent.event !== 'pull_request_target' ||
      parent.path !== '.github/workflows/entrypoint.yaml' ||
      parent.repository?.full_name !== 'matrixorigin/matrixone' ||
      !/^[a-f0-9]{40}$/.test(parent.head_sha || '') ||
      parent.status !== 'completed' || !parent.conclusion ||
      event.head_sha !== parent.head_sha) {
    throw new Error('Completed parent attempt identity mismatch');
  }
  const worker = (await github.rest.actions.getWorkflow({
    ...repo, workflow_id: 'coverage-standby-worker.yaml',
  })).data;
  const title = new RegExp(`^Coverage standby / ${parent.id}/${parent.run_attempt}/[01]/` +
    `${parent.head_sha}/(?:cs|idc)/[a-f0-9]{64}$`);
  const isShortTitle = run => /^GROUP[01]$/.test(run.display_title || '');
  const hasWorkerIdentity = run => active.has(run.status) && run.event === 'workflow_dispatch' &&
    run.workflow_id === worker.id && run.path === '.github/workflows/coverage-standby-worker.yaml' &&
    run.repository?.full_name === 'matrixorigin/matrixone' &&
    run.head_branch === 'main' && run.run_attempt === 1;
  const matches = run => hasWorkerIdentity(run) && (
    title.test(run.display_title || '') || (isShortTitle(run) && title.test(run.canonicalTitle || '')));
  const candidates = new Map();
  // A 204 dispatch response can become visible shortly after the parent ends.
  // Rescan briefly so a late-indexed, still-queued child is not left behind.
  const first = Math.floor(Date.parse(parent.run_started_at || parent.created_at) / 1000);
  const last = Math.floor(Date.parse(parent.updated_at) / 1000) + 60;
  if (!Number.isSafeInteger(first) || !Number.isSafeInteger(last) || last < first) {
    throw new Error('Invalid completed parent time window');
  }
  const stamp = sec => new Date(sec * 1000).toISOString().replace('.000Z', 'Z');
  async function scan(status, lo, hi) {
    for (let page = 1; ; page++) {
      const { data } = await github.rest.actions.listWorkflowRuns({
        ...repo, workflow_id: worker.id, branch: 'main', event: 'workflow_dispatch',
        status, created: `${stamp(lo)}..${stamp(hi)}`, per_page: 100, page,
      });
      // GitHub caps filtered searches at 1000 results, even with pagination.
      if (data.total_count > 1000) {
        if (lo === hi) throw new Error('Coverage worker search saturated within one second');
        const mid = Math.floor((lo + hi) / 2);
        await scan(status, lo, mid);
        await scan(status, mid + 1, hi);
        return;
      }
      // Short titles are just discovery hints. Check the same full identity
      // from the live worker's skipped job before any cancellation.
      for (const run of data.workflow_runs) {
        if (hasWorkerIdentity(run) && (title.test(run.display_title || '') || isShortTitle(run))) {
          candidates.set(run.id, run);
        }
      }
      if (data.workflow_runs.length < 100 || page * 100 >= data.total_count) return;
    }
  }
  async function scanReservations() {
    for (let round = 0; round < 3; round++) {
      for (const status of active) await scan(status, first, last);
      if (round < 2) await wait(5_000);
    }
  }
  async function scanGrants() {
    // The parent publishes coverage-admit-<child ID> before that child runs UT.
    // A receipt is a lookup hint, not cancellation authority. Do not download
    // grant.json. Artifacts can span attempts; matches() checks the live run.
    for (let page = 1; ; page++) {
      const { data } = await github.rest.actions.listWorkflowRunArtifacts({
        ...repo, run_id: parent.id, per_page: 100, page,
      });
      for (const { name } of data.artifacts) {
        const match = /^coverage-admit-([1-9][0-9]*)$/.exec(name);
        if (!match) continue;
        const id = Number(match[1]);
        if (Number.isSafeInteger(id) && name === `coverage-admit-${id}` && !candidates.has(id)) {
          candidates.set(id, { id, run_attempt: 1 });
        }
      }
      if (data.artifacts.length < 100) return;
    }
  }
  // Query receipts last, after the late-index grace period. If one discovery
  // API fails, clean up verified candidates from the other, then surface it.
  const failures = new Map();
  for (const discover of [scanReservations, scanGrants]) {
    try { await discover(); } catch (err) { failures.set(discover.name, err); }
  }
  const get = async id => {
    let run;
    try {
      const { data } = await github.rest.actions.getWorkflowRun({ ...repo, run_id: id });
      if (data.id !== id) throw new Error('Coverage worker lookup ID mismatch');
      run = { ...data, canonicalTitle: null };
    } catch (err) {
      if (err.status === 404) return { id, status: 'missing' };
      throw err;
    }
    if (!hasWorkerIdentity(run) || !isShortTitle(run)) return run;
    const { data } = await github.rest.actions.listJobsForWorkflowRunAttempt({
      ...repo, run_id: id, attempt_number: 1, per_page: 100,
    }).catch(err => {
      // The run is visible, but GitHub may not have indexed its attempt jobs yet.
      if (err.status === 404) throw new CoverageIdentityPending('Coverage worker attempt jobs returned HTTP 404');
      throw err;
    });
    if (data.jobs.length !== data.total_count) throw new Error('Incomplete coverage worker identity');
    const suffix = ' / Run identity';
    const markers = data.jobs.filter(job => job.name.endsWith(suffix));
    if (!markers.length) throw new CoverageIdentityPending('Coverage worker identity is not visible');
    if (markers.length !== 1) throw new Error('Ambiguous coverage worker identity');
    const [marker] = markers;
    if (marker.run_id !== id || marker.run_attempt !== 1) throw new Error('Coverage worker identity run/attempt mismatch');
    if (marker.status !== 'completed') throw new CoverageIdentityPending('Coverage worker identity has not completed');
    if (marker.conclusion !== 'skipped') throw new Error('Coverage worker identity job was not skipped');
    const canonicalTitle = marker.name.slice(0, -suffix.length);
    const identity = /^Coverage standby \/ [1-9]\d*\/[1-9]\d*\/([01])\/[a-f0-9]{40}\/(?:cs|idc)\/[a-f0-9]{64}$/.exec(canonicalTitle);
    if (!identity || run.display_title !== `GROUP${identity[1]}`) throw new Error('Invalid coverage worker identity title');
    return { ...run, canonicalTitle };
  };
  const stillCurrent = async () => {
    const { data: current } = await github.rest.actions.getWorkflowRunAttempt({
      ...repo, run_id: parent.id, attempt_number: parent.run_attempt,
    });
    return current.id === parent.id && current.run_attempt === parent.run_attempt &&
      current.status === 'completed' && current.head_sha === parent.head_sha;
  };
  await cancelMatchingRuns({ github, repo, core, get, candidates, matches, stillCurrent, wait,
    onRunError(id, err) {
      if (!failures.has(id)) failures.set(id, err);
    } });
  if (failures.size) {
    throw new Error('Incomplete coverage cleanup: ' +
      [...failures].map(([source, err]) => `${source}: ${err.message}`).join('; '));
  }
}

async function cancelMatchingRuns({ github, repo, core, get, candidates, matches, stillCurrent, wait, onRunError }) {
  const pending = new Map();
  // Coverage receipts can span attempts. Isolate only per-run operations;
  // errors checking the shared parent must still stop the entire cleanup.
  const forRun = async (id, operation) => {
    try { await operation(); } catch (err) {
      if (!onRunError) throw err;
      onRunError(id, err);
    }
  };
  const requestCancel = async (run, force) => {
    try {
      if (force) {
        await github.request('POST /repos/{owner}/{repo}/actions/runs/{run_id}/force-cancel', { ...repo, run_id: run.id });
      } else {
        await github.rest.actions.cancelWorkflowRun({ ...repo, run_id: run.id });
      }
    } catch (error) {
      // Completion can race either cancellation request. Do not hide real API
      // failures or retry against an attempt that has since been rerun.
      if (error.status !== 409) throw error;
      const current = await get(run.id);
      if (current.status === 'completed') return;
      if (force) throw error;
      // Normal cancellation may already have been requested by concurrency.
      // Revalidate before allowing the ordinary grace/escalation path; a 409
      // alone is never evidence that the run has finished or is safe to kill.
      if (!matches(current) || current.run_attempt !== run.run_attempt) return;
      core.info(`Run ${run.id} is still active after cancellation conflict; waiting before escalation`);
    }
    core.info(`${force ? 'Force cancellation' : 'Cancellation'} requested for run ${run.id}, attempt ${run.run_attempt}`);
  };
  // A dispatched run can be visible before GitHub indexes its skipped job.
  // Retry only unresolved lookups, with one shared wait per round. Never
  // retry a cancellation request or infer identity from a short title.
  let lookups = candidates.values();
  for (let round = 0; ; round++) {
    const deferred = [];
    for (const candidate of lookups) {
      if (!await stillCurrent()) return;
      await forRun(candidate.id, async () => {
        let run;
        try { run = await get(candidate.id); } catch (err) {
          if (!(err instanceof CoverageIdentityPending) || round >= 2) throw err;
          deferred.push(candidate);
          return;
        }
        if (!matches(run) || run.run_attempt !== candidate.run_attempt) return;
        await requestCancel(run, false);
        pending.set(run.id, run);
      });
    }
    if (!deferred.length) break;
    await wait(5_000);
    lookups = deferred;
  }
  // Give ordinary cancellation two minutes for process teardown and bounded
  // diagnostics. Poll all candidates together, so cost is not N grace periods.
  for (let round = 0; pending.size && round < 12; round++) {
    await wait(10_000);
    if (!await stillCurrent()) return;
    for (const [id, initial] of pending) {
      await forRun(id, async () => {
        let run;
        try { run = await get(id); } catch (err) {
          // The grace poll takes no action. Defer an unindexed marker to the
          // next poll; escalation below still requires fresh positive evidence.
          if (err instanceof CoverageIdentityPending) return;
          throw err;
        }
        if (!matches(run) || run.run_attempt !== initial.run_attempt) pending.delete(id);
      });
    }
  }
  for (const [id, initial] of pending) {
    if (!await stillCurrent()) return;
    await forRun(id, async () => {
      const run = await get(id);
      if (!matches(run) || run.run_attempt !== initial.run_attempt) return;
      await requestCancel(run, true);
    });
  }
  await core.summary.addHeading('CI cancellation')
    .addRaw(`Matched obsolete runs: ${candidates.size}; escalation candidates after grace: ${pending.size}. Cancellation acceptance is not proof that runner cleanup has finished.`)
    .write();
}

module.exports = { isSuperseded, releaseSupersededRuns, releaseClosedPullRequestRuns,
  releaseCompletedCoverageWorkers };
