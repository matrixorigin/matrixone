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

const path = require('node:path');
const { execFileSync } = require('node:child_process');
function validPath(path) {
  return typeof path === 'string' && !path.split('/').some(p => !p || p === '.' || p === '..');
}

function category(path) {
  if (!validPath(path)) return 'full';
  if (path.startsWith('clients/python/') || path === '.github/workflows/python-sdk.yml') return 'sdk';
  if (/(^|\/)(testdata|fixtures)\//i.test(path)) return 'full';
  const extension = path.slice(path.lastIndexOf('.')).toLowerCase();
  if (extension === '.md' || extension === '.rst' ||
      /^docs\/.*\.(png|jpg|jpeg|gif|svg)$/.test(path)) return 'docs';
  if (/^pkg\/.*_test\.go$/.test(path)) return 'ut';
  if (/^test\/distributed\/cases\/.*\.(sql|test|result)$/.test(path)) return 'bvt';
  return 'full';
}

function immutableChanges({ cwd = process.cwd(), env = process.env, git, report = () => {} } = {}) {
  const full = reason => { report(reason); return null; };
  const fs = require('node:fs');
  const { execFileSync } = require('node:child_process');
  git ||= args => execFileSync('git', args, {
    cwd, encoding: 'utf8', timeout: 30000, maxBuffer: 8 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if (env.GITHUB_ACTIONS !== 'true' || !env.GITHUB_EVENT_PATH ||
      !['pull_request', 'pull_request_target'].includes(env.GITHUB_EVENT_NAME)) return full('immutable PR event unavailable');
  try {
    const pr = JSON.parse(fs.readFileSync(env.GITHUB_EVENT_PATH, 'utf8')).pull_request;
    const base = pr?.base?.sha;
    const head = pr?.head?.sha;
    if (![base, head].every(sha => /^[0-9a-f]{40}$/.test(sha || '')) ||
        git(['rev-parse', 'HEAD']).trim() !== head) return full('invalid PR revision or checkout head mismatch');
    // Local edits cannot use a committed-input exemption. Include untracked
    // inputs too; ignored native outputs and runner reports are not source.
    if (git(['status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none']).trim()) return full('checkout contains uncommitted inputs');
    try {
      git(['cat-file', '-e', `${base}^{commit}`]);
    } catch {
      // Fetch the immutable object only. Do not move a checkout or local ref.
      if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY || '')) return full('base unavailable and repository identity invalid');
      git(['fetch', '--no-tags', '--depth=1',
        `https://github.com/${env.GITHUB_REPOSITORY}.git`, base]);
    }
    // Tree comparison also works when a shallow checkout lacks ancestry. Both
    // sides of a rename are roots; do not hide removals behind rename matching.
    const raw = git(['diff', '--no-ext-diff', '--no-renames', '--ignore-submodules=none', '--name-status', '-z', base, head]);
    const fields = raw.split('\0');
    if (fields.pop() !== '' || fields.length === 0 || fields.length % 2) return full('empty or malformed immutable diff');
    const files = [];
    for (let i = 0; i < fields.length; i += 2) {
      if (!['A', 'M', 'D', 'T'].includes(fields[i]) || !validPath(fields[i + 1])) return full('unsupported immutable diff entry');
      files.push({ status: fields[i], filename: fields[i + 1] });
    }
    return files;
  } catch {
    return full('immutable event, Git status, base fetch or diff unavailable');
  }
}

// Emit one bounded record per main-module package/variant. Go owns import and
// build-tag resolution, including test-only importers; no source import parser.
const inventoryFields = 'Name,ImportPath,Dir,Module,Imports,TestImports,XTestImports,' +
  'GoFiles,CgoFiles,TestGoFiles,XTestGoFiles';

// go list emits adjacent JSON objects rather than an array. Frame the objects;
// JSON.parse remains the decoder (including escaped strings and nested values).
function inventory(text) {
  const records = [];
  let depth = 0, start = 0, quoted = false, escaped = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (depth === 0 && !quoted) {
      if (/\s/.test(c)) continue;
      if (c !== '{') throw new Error('Invalid Go inventory');
    }
    if (quoted) {
      if (escaped) escaped = false;
      else if (c === '\\') escaped = true;
      else if (c === '"') quoted = false;
    } else if (c === '"') quoted = true;
    else if (c === '{') { if (depth++ === 0) start = i; }
    else if (c === '}') {
      if (--depth < 0) throw new Error('Invalid Go inventory');
      if (depth === 0) {
        const p = JSON.parse(text.slice(start, i + 1));
        if (p.Module?.Main) {
          // The only non-source main-module GoFiles are Go's generated test
          // entrypoints in its build cache. Regular package files are relative.
          const generatedMain = p.Name === 'main' && p.ImportPath.endsWith('.test') &&
            p.GoFiles?.length && p.GoFiles.every(file => path.isAbsolute(file));
          records.push({ id: p.ImportPath, dir: p.Dir, imports: p.Imports || [],
            testImports: p.TestImports || [], xTestImports: p.XTestImports || [],
            sources: [...(generatedMain ? [] : p.GoFiles || []), ...(p.CgoFiles || []),
              ...(p.TestGoFiles || []), ...(p.XTestGoFiles || [])] });
        }
      }
    } else if (depth === 0 && !/\s/.test(c)) throw new Error('Invalid Go inventory');
  }
  if (depth || quoted || !records.length) throw new Error('Incomplete Go inventory');
  return records;
}

function goChanges(files, report = () => {}) {
  if (!files?.length) { report('no verified changed inputs'); return null; }
  const changed = [];
  for (const file of files) {
    if (file.status === 'T') { report(`file type changed: ${JSON.stringify(file.filename.slice(0, 200))}`); return null; }
    if (/^pkg\/.*\.go$/.test(file.filename) &&
        !/(^|\/)(testdata|fixtures)\//i.test(file.filename)) {
      changed.push(file.filename);
    } else if (!['docs', 'bvt'].includes(category(file.filename))) {
      report(`input requires full analysis: ${JSON.stringify(file.filename.slice(0, 200))}`);
      return null;
    }
  }
  // No Go change means the cheap policy check still runs, but these analyzers
  // have no changed inputs. Keep a nonempty full scan rather than inventing a
  // separate successful-empty mode in this first increment.
  if (!changed.length) report('no changed Go inputs');
  return changed.length ? changed : null;
}

function selectPackages(files, records, root, report = () => {}) {
  const full = reason => { report(reason); return ['./...']; };
  const changed = goChanges(files, report);
  if (!changed || !records.length) return ['./...'];
  const owners = new Map();
  const variants = new Map();
  const reverse = new Map();
  for (const record of records) {
    const relative = path.relative(root, record.dir);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative) ||
        !/^[\w./-]+$/.test(relative)) return full('package owner is outside the supported source layout');
    const owner = `./${relative}`;
    owners.set(relative, owner);
    variants.set(record.id, owner);
  }
  for (const record of records) {
    const owner = variants.get(record.id);
    for (const imported of [...(record.imports || []), ...(record.testImports || []),
      ...(record.xTestImports || [])]) {
      const dependency = variants.get(imported);
      if (!dependency) {
        // A main-module test variant missing from discovery is not a known
        // external dependency. Do not guess how its package ID maps.
        if (imported.includes(' [')) return full('test variant is missing from the verified graph');
        continue;
      }
      if (!reverse.has(dependency)) reverse.set(dependency, new Set());
      reverse.get(dependency).add(owner);
    }
  }
  const selected = new Set();
  const production = [];
  for (const file of changed) {
    const owner = owners.get(path.posix.dirname(file));
    if (!owner) return full('changed package is missing from the verified graph');
    selected.add(owner);
    if (!file.endsWith('_test.go')) production.push(owner);
  }
  const seen = new Set(production);
  for (let i = 0; i < production.length; i++) {
    for (const consumer of reverse.get(production[i]) || []) {
      selected.add(consumer);
      if (!seen.has(consumer)) {
        seen.add(consumer);
        production.push(consumer);
      }
    }
  }
  return selected.size && selected.size < owners.size / 2 ? [...selected].sort() :
    full(`affected closure reaches full-scan threshold (${selected.size}/${owners.size} packages)`);
}

function plan({ cwd = process.cwd(), env = process.env, run = execFileSync } = {}) {
  // Always verify the whole production/test graph once, including on fallback.
  // Discovery failure is a real SCA failure, not a successful empty selection.
  const text = run('go', ['list', '-mod=readonly', '-deps', '-test', `-json=${inventoryFields}`, './...'], {
    cwd, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
    env: { ...env, GOWORK: 'off' }, stdio: ['ignore', 'pipe', 'inherit'],
  });
  const records = inventory(text);
  const report = reason => console.error(`SCA full-scan reason: ${reason}`);
  let files = env.SCA_FULL === '1' ? null : immutableChanges({ cwd, env, report });
  if (env.SCA_FULL === '1') report('SCA_FULL=1');
  if (files) {
    // Git status omits ignored files, but Go still compiles an ignored .go
    // file inside a package. Require every real source input to be committed
    // and regular; generated/untracked inputs and source symlinks keep full SCA.
    try {
      const entries = run('git', ['ls-files', '--stage', '-z'], {
        cwd, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const tracked = new Set(entries.split('\0').flatMap(entry => {
        const match = /^100(?:644|755) [0-9a-f]+ 0\t([\s\S]+)$/.exec(entry);
        return match ? [match[1]] : [];
      }));
      if (records.some(p => p.sources.some(source =>
        !tracked.has(path.relative(cwd, path.resolve(p.dir, source)).split(path.sep).join('/'))))) {
        report('Go graph contains untracked, generated or nonregular source inputs');
        files = null;
      }
    } catch {
      report('tracked source inventory unavailable');
      files = null;
    }
  }
  const selected = selectPackages(files, records, cwd, report);
  const total = new Set(records.map(p => p.dir)).size;
  console.error(`SCA selection: ${selected[0] === './...' ? 'full graph' : `${selected.length}/${total} packages`}`);
  return selected;
}

if (require.main === module) {
  try {
    process.stdout.write(`${plan().join(' ')}\n`);
  } catch (error) {
    console.error(`SCA package discovery failed: ${error.message}`);
    process.exitCode = 1;
  }
}
module.exports = { goChanges, immutableChanges, inventoryFields, inventory, selectPackages, plan };
