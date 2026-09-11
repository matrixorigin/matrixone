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

  const candidates = new Map();
  // Bound discovery to active runs of this workflow and branch. Reject an
  // unexpectedly incomplete response rather than silently claiming recovery.
  for (const status of active) {
    const { data } = await github.rest.actions.listWorkflowRuns({
      ...repo, workflow_id: workflow.id, branch: successor.head_branch,
      event: 'pull_request_target', status, per_page: 100,
    });
    if (data.total_count > 100) throw new Error('Too many active runs; inspect cancellation manually');
    for (const run of data.workflow_runs) {
      if (isSuperseded(run, successor)) candidates.set(run.id, run);
    }
  }
  const pending = new Map();
  const stillCurrent = async () => {
    const current = await get(successor.id);
    return active.has(current.status) && current.run_attempt === successor.run_attempt;
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
      if (!isSuperseded(current, successor) || current.run_attempt !== run.run_attempt) return;
      core.info(`Run ${run.id} is still active after cancellation conflict; waiting before escalation`);
    }
    core.info(`${force ? 'Force cancellation' : 'Cancellation'} requested for run ${run.id}, attempt ${run.run_attempt}`);
  };
  for (const candidate of candidates.values()) {
    if (!await stillCurrent()) return;
    const run = await get(candidate.id);
    if (!isSuperseded(run, successor) || run.run_attempt !== candidate.run_attempt) continue;
    await requestCancel(run, false);
    pending.set(run.id, run);
  }
  // Give ordinary cancellation two minutes for process teardown and bounded
  // diagnostics. Poll all candidates together, so cost is not N grace periods.
  for (let round = 0; pending.size && round < 12; round++) {
    await wait(10_000);
    if (!await stillCurrent()) return;
    for (const [id, initial] of pending) {
      const run = await get(id);
      if (!isSuperseded(run, successor) || run.run_attempt !== initial.run_attempt) pending.delete(id);
    }
  }
  for (const [id, initial] of pending) {
    if (!await stillCurrent()) return;
    const run = await get(id);
    if (!isSuperseded(run, successor) || run.run_attempt !== initial.run_attempt) continue;
    await requestCancel(run, true);
  }
  await core.summary.addHeading('Superseded CI cancellation')
    .addRaw(`Successor: ${successor.id}; matched older runs: ${candidates.size}; escalation candidates after grace: ${pending.size}. Cancellation acceptance is not proof that runner cleanup has finished.`)
    .write();
}

module.exports = { isSuperseded, releaseSupersededRuns };
