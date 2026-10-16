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

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync, spawnSync } = require('node:child_process');
const { immutableChanges } = require('./change-scope.cjs');
const { inventory, selectPackages } = require('./static-analysis.cjs');

const root = '/repo';
const records = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'].map(name => ({
  id: `example.org/project/pkg/${name}`, dir: `${root}/pkg/${name}`, imports: [],
}));
records[1].imports = [records[0].id];
records.push({ id: `${records[2].id}_test [${records[2].id}.test]`, dir: records[2].dir,
  imports: [records[0].id] });
const file = (filename, status = 'M') => ({ filename, status });

test('complete package selection follows production and test imports, not test reverse imports', () => {
  assert.deepEqual(selectPackages([file('pkg/a/a.go')], records, root), ['./pkg/a', './pkg/b', './pkg/c']);
  assert.deepEqual(selectPackages([file('pkg/a/a_test.go')], records, root), ['./pkg/a']);
  assert.deepEqual(selectPackages([file('pkg/a/a.go', 'D'), file('pkg/d/a.go', 'A')], records, root), ['./...']);
  assert.deepEqual(selectPackages([file('pkg/a/a.go', 'D')], records, root), ['./pkg/a', './pkg/b', './pkg/c']);
});

test('uncertain inputs and broad changes retain full analysis', () => {
  for (const files of [null, [], [file('README.md')], [file('go.mod')], [file('cgo/x.c')],
    [file('.golangci.yml')], [file('pkg/a/testdata/input.go')], [file('pkg/gone/x.go', 'D')],
    [file('pkg/a/a.go', 'T')], [file('pkg/a/a.go'), file('Makefile')],
    [file('pkg/a/a.go'), file('pkg/d/d.go')]]) {
    assert.deepEqual(selectPackages(files, records, root), ['./...']);
  }
  const unknown = [...records, { id: 'unknown', dir: `${root}/pkg/e`, imports: ['missing [test]'] }];
  assert.deepEqual(selectPackages([file('pkg/a/a.go')], unknown, root), ['./...']);
});

test('Go JSON inventory is framed without interpreting braces inside strings', () => {
  const p = { ImportPath: 'example.org/pkg/a', Dir: '/repo/pkg/a', Module: { Main: true },
    Imports: ['external/{quoted}"'], TestImports: ['testing'] };
  const parsed = inventory(`${JSON.stringify({ ImportPath: 'fmt' })}\n${JSON.stringify(p)}\n`);
  assert.equal(parsed.length, 1);
  assert.deepEqual(parsed[0].imports, p.Imports);
  for (const malformed of ['', '{', JSON.stringify(p) + 'junk', JSON.stringify(p) + '}', '"prefix"' + JSON.stringify(p)]) {
    assert.throws(() => inventory(malformed));
  }
});

function temporary(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mo-sca-contract-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}

test('immutable tree comparison handles shallow bases, moves, dirt and unavailable history', t => {
  const dir = temporary(t);
  const event = path.join(dir, 'event.json');
  const base = 'a'.repeat(40), head = 'b'.repeat(40);
  fs.writeFileSync(event, JSON.stringify({ pull_request: { base: { sha: base }, head: { sha: head } } }));
  const env = { GITHUB_ACTIONS: 'true', GITHUB_EVENT_NAME: 'pull_request_target',
    GITHUB_EVENT_PATH: event, GITHUB_REPOSITORY: 'matrixorigin/matrixone' };
  const calls = [];
  function git(args) {
    calls.push(args);
    if (args[0] === 'rev-parse') return head;
    if (args[0] === 'status') return '';
    if (args[0] === 'cat-file') throw new Error('shallow');
    if (args[0] === 'fetch') return '';
    return 'D\0pkg/a/old.go\0A\0pkg/b/new.go\0';
  }
  assert.deepEqual(immutableChanges({ env, git }), [file('pkg/a/old.go', 'D'), file('pkg/b/new.go', 'A')]);
  assert.ok(calls.some(args => args[0] === 'fetch' && args.at(-1) === base));
  for (const failed of ['status', 'fetch', 'diff']) {
    assert.equal(immutableChanges({ env, git: args => {
      if (args[0] === failed) throw new Error('unavailable');
      return git(args);
    } }), null);
  }
  assert.equal(immutableChanges({ env, git: args => args[0] === 'status' ? ' M pkg/a/a.go' : git(args) }), null);
  assert.equal(immutableChanges({ env, git: args => args[0] === 'rev-parse' ? base : git(args) }), null);
  assert.equal(immutableChanges({ env: { ...env, GITHUB_EVENT_NAME: 'push' }, git }), null);
});

test('real Make consumer shares selection, keeps full override, and propagates analyzer failures', { timeout: 90000 }, t => {
  const dir = temporary(t), repo = path.join(dir, 'repo'), bin = path.join(dir, 'bin');
  fs.mkdirSync(repo); fs.mkdirSync(bin);
  const git = args => execFileSync('git', args, { cwd: repo, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  fs.writeFileSync(path.join(repo, '.gitignore'), 'pkg/h/ignored.go\n');
  fs.writeFileSync(path.join(repo, 'go.mod'), 'module example.org/project\n\ngo 1.23.0\n');
  for (const name of ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']) {
    const p = path.join(repo, 'pkg', name); fs.mkdirSync(p, { recursive: true });
    fs.writeFileSync(path.join(p, `${name}.go`), `package ${name}\n${name === 'b' ? 'import _ "example.org/project/pkg/a"\n' : ''}func Value() int { return 1 }\n`);
    if (name === 'c') fs.writeFileSync(path.join(p, 'c_test.go'), 'package c_test\nimport _ "example.org/project/pkg/a"\n');
  }
  const source = path.resolve(__dirname, '../..');
  fs.mkdirSync(path.join(repo, '.github/ci'), { recursive: true });
  for (const name of ['static-analysis.cjs', 'change-scope.cjs']) {
    fs.copyFileSync(path.join(source, '.github/ci', name), path.join(repo, '.github/ci', name));
  }
  const makefile = fs.readFileSync(path.join(source, 'Makefile'), 'utf8');
  const section = makefile.slice(makefile.indexOf('.PHONY: install-static-check-tools'));
  fs.writeFileSync(path.join(repo, 'Makefile'), 'GO_MODULE_MODE = -mod=readonly\nconfig:\n\tgo list -mod=readonly -test ./... >/dev/null\n' + section);
  git(['init', '-q']);
  function commit() {
    git(['add', '.']);
    git(['-c', 'user.name=CI test', '-c', 'user.email=ci@example.invalid', 'commit', '-qm', 'fixture']);
    return git(['rev-parse', 'HEAD']);
  }
  const base = commit();
  fs.appendFileSync(path.join(repo, 'pkg/a/a.go'), '// changed production input\n');
  const head = commit();
  const event = path.join(dir, 'event.json'), log = path.join(dir, 'commands.log');
  fs.writeFileSync(event, JSON.stringify({ pull_request: { base: { sha: base }, head: { sha: head } } }));
  const realGo = execFileSync('which', ['go'], { encoding: 'utf8' }).trim();
  const tools = {
    go: '#!/bin/sh\nif [ "$1" = vet ]; then printf "vet %s\\n" "$*" >> "$COMMAND_LOG"; exit "${FAIL_VET:-0}"; fi\nexec "$REAL_GO" "$@"\n',
    'golangci-lint': '#!/bin/sh\nprintf "lint %s\\n" "$*" >> "$COMMAND_LOG"\nexit "${FAIL_LINT:-0}"\n',
    molint: '#!/bin/sh\nexit 0\n',
  };
  for (const [name, contents] of Object.entries(tools)) fs.writeFileSync(path.join(bin, name), contents, { mode: 0o755 });
  const env = { ...process.env, PATH: `${bin}:${process.env.PATH}`, REAL_GO: realGo, COMMAND_LOG: log,
    GITHUB_ACTIONS: 'true', GITHUB_EVENT_NAME: 'pull_request_target', GITHUB_EVENT_PATH: event,
    GITHUB_REPOSITORY: 'matrixorigin/matrixone', GOWORK: 'off', GOFLAGS: '', GOTOOLCHAIN: 'local' };
  function run(extra = {}) {
    fs.writeFileSync(log, '');
    const result = spawnSync('make', ['--no-print-directory', 'static-check-analysis', 'GOLANGCI_LINT_CONCURRENCY=1'],
      { cwd: repo, env: { ...env, ...extra }, encoding: 'utf8', timeout: 60000 });
    return { result, commands: fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean) };
  }
  const first = run();
  assert.equal(first.result.status, 0, first.result.stderr + first.result.stdout);
  assert.equal(first.commands.length, 2);
  for (const command of first.commands) assert.ok(command.endsWith('./pkg/a ./pkg/b ./pkg/c'), command);
  assert.ok(first.commands[1].includes('--concurrency 1'));
  const ignored = path.join(repo, 'pkg/h/ignored.go');
  fs.writeFileSync(ignored, 'package h\nvar GeneratedInput = 1\n');
  const generated = run();
  fs.unlinkSync(ignored);
  assert.equal(generated.result.status, 0, generated.result.stderr);
  assert.ok(generated.commands.every(command => command.endsWith('./...')),
    'Go consumes ignored inputs too; Git status alone cannot exempt their packages');
  const full = run({ SCA_FULL: '1' });
  assert.equal(full.result.status, 0, full.result.stderr);
  assert.ok(full.commands.every(command => command.endsWith('./...')));
  const vetFailure = run({ FAIL_VET: '7' });
  assert.notEqual(vetFailure.result.status, 0);
  assert.equal(vetFailure.commands.length, 1);
  assert.notEqual(run({ FAIL_LINT: '9' }).result.status, 0);
  fs.writeFileSync(path.join(repo, 'pkg/a/a.go'), 'not a Go package\n');
  const invalid = run();
  assert.notEqual(invalid.result.status, 0);
  assert.equal(invalid.commands.length, 0);
});
