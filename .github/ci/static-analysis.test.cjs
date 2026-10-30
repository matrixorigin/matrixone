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
  imports: [records[1].id] });
const file = (filename, status = 'M') => ({ filename, status });

test('complete package selection follows production and test imports, not test reverse imports', () => {
  assert.deepEqual(selectPackages([file('pkg/a/a.go')], records, root), ['./pkg/a', './pkg/b', './pkg/c']);
  assert.deepEqual(selectPackages([file('pkg/a/a_test.go')], records, root), ['./pkg/a']);
  const reasons = [];
  assert.deepEqual(selectPackages([file('pkg/a/a.go', 'D'), file('pkg/d/a.go', 'A')], records, root, reason => reasons.push(reason)), ['./...']);
  assert.match(reasons.join('\n'), /threshold \(4\/8 packages\)/);
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
  const reasons = [];
  assert.equal(immutableChanges({ env, git: args => args[0] === 'status' ? ' M pkg/a/a.go' : git(args), report: reason => reasons.push(reason) }), null);
  assert.deepEqual(reasons, ['checkout contains uncommitted inputs']);
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
    go: '#!/bin/sh\nexec "$REAL_GO" "$@"\n',
    'golangci-lint': '#!/bin/sh\nprintf "lint %s\\n" "$*" >> "$COMMAND_LOG"\nexit "${FAIL_LINT:-0}"\n',
    molint: '#!/bin/sh\nprintf "molint %s\\n" "$*" >> "$COMMAND_LOG"\nexit "${FAIL_MOLINT:-0}"\n',
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
  assert.ok(first.commands[0].startsWith('molint check -mod=readonly '));
  assert.match(first.result.stderr, /SCA selection: 3\/8 packages/);
  for (const command of first.commands) assert.ok(command.endsWith('./pkg/a ./pkg/b ./pkg/c'), command);
  assert.ok(first.commands[1].includes('--concurrency 1'));
  const ignored = path.join(repo, 'pkg/h/ignored.go');
  fs.writeFileSync(ignored, 'package h\nvar GeneratedInput = 1\n');
  const generated = run();
  fs.unlinkSync(ignored);
  assert.equal(generated.result.status, 0, generated.result.stderr);
  assert.match(generated.result.stderr, /untracked, generated or nonregular source inputs/);
  assert.ok(generated.commands.every(command => command.endsWith('./...')),
    'Go consumes ignored inputs too; Git status alone cannot exempt their packages');
  const full = run({ SCA_FULL: '1' });
  assert.equal(full.result.status, 0, full.result.stderr);
  assert.match(full.result.stderr, /SCA_FULL=1/);
  assert.ok(full.commands.every(command => command.endsWith('./...')));
  const molintFailure = run({ FAIL_MOLINT: '7' });
  assert.notEqual(molintFailure.result.status, 0);
  assert.equal(molintFailure.commands.length, 1);
  assert.notEqual(run({ FAIL_LINT: '9' }).result.status, 0);
  const noNode = path.join(dir, 'without-node');
  fs.mkdirSync(noNode);
  for (const command of ['make', 'grep', 'git', 'which']) {
    const executable = execFileSync('which', [command], { encoding: 'utf8' }).trim();
    fs.symlinkSync(executable, path.join(noNode, command));
  }
  for (const command of Object.keys(tools)) fs.symlinkSync(path.join(bin, command), path.join(noNode, command));
  const missingNode = run({ PATH: noNode });
  assert.equal(missingNode.result.status, 0, missingNode.result.stderr);
  assert.match(missingNode.result.stderr, /full graph \(Node unavailable on PATH in CI\)/);
  assert.equal(missingNode.commands.length, 2);
  assert.ok(missingNode.commands.every(command => command.endsWith('./...')));
  const missingNodeMolintFailure = run({ PATH: noNode, FAIL_MOLINT: '7' });
  assert.notEqual(missingNodeMolintFailure.result.status, 0);
  assert.equal(missingNodeMolintFailure.commands.length, 1);
  const missingNodeLintFailure = run({ PATH: noNode, FAIL_LINT: '9' });
  assert.notEqual(missingNodeLintFailure.result.status, 0);
  assert.equal(missingNodeLintFailure.commands.length, 2);
  const local = run({ PATH: noNode, GITHUB_ACTIONS: 'false' });
  assert.equal(local.result.status, 0, local.result.stderr);
  assert.match(local.result.stderr, /full graph \(local invocation\)/);
  assert.ok(local.commands.every(command => command.endsWith('./...')));
  fs.writeFileSync(path.join(repo, 'pkg/a/a.go'), 'not a Go package\n');
  const invalid = run();
  assert.notEqual(invalid.result.status, 0);
  assert.equal(invalid.commands.length, 0);
  const missingNodeInvalid = run({ PATH: noNode });
  assert.notEqual(missingNodeInvalid.result.status, 0);
  assert.equal(missingNodeInvalid.commands.length, 0, 'full graph validation must precede either analyzer');
});

// Exercise the public Make target with the existing downloader boundary stubbed;
// thirdparties/download_test owns network, checksum and byte-limit validation.
test('tool installation avoids linter compilation and preserves failure/compatibility paths', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sca-install-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const source = path.resolve(__dirname, '../..');
  const target = fs.readFileSync(path.join(source, 'Makefile'), 'utf8')
    .match(/^install-static-check-tools:\n[\s\S]*?(?=\n\n)/m)[0];
  fs.writeFileSync(path.join(dir, 'Makefile'), target + '\n');
  const bin = path.join(dir, 'bin'), fixtures = path.join(dir, 'fixtures');
  fs.mkdirSync(bin); fs.mkdirSync(fixtures); fs.mkdirSync(path.join(dir, 'thirdparties'));
  const log = path.join(dir, 'commands.log'), gopath = path.join(dir, 'gopath');
  fs.writeFileSync(path.join(bin, 'go'), `#!/bin/sh
set -eu
if [ "$1" = env ]; then
  case "$2" in
    GOHOSTOS) echo "$INSTALL_OS" ;;
    GOHOSTARCH) echo "$INSTALL_ARCH" ;;
    GOVERSION) echo "$INSTALL_GO" ;;
    *) exit 2 ;;
  esac
else
  echo "$*" >> "$INSTALL_LOG"
  case "$*" in *"\${FAIL_TOOL:-not-a-tool}"*) exit 9 ;; esac
  tool=\${2%@*}; tool=\${tool##*/}
  mkdir -p "$GOBIN"
  printf '#!/bin/sh\\nexit 0\\n' > "$GOBIN/$tool"
  chmod +x "$GOBIN/$tool"
fi
`, { mode: 0o755 });
  fs.writeFileSync(path.join(dir, 'thirdparties/download'), `#!/bin/sh
set -eu
printf 'download %s %s\\n' "$1" "$2" >> "$INSTALL_LOG"
test "\${#2}" -eq 64
test "$MO_DOWNLOAD_ATTEMPTS/$MO_DOWNLOAD_RETRY_ROUNDS/$MO_DOWNLOAD_MAX_TIME_SECONDS" = 1/1/60
if [ "\${FAIL_DOWNLOAD:-0}" = 1 ]; then exit 8; fi
if [ "\${BAD_ARCHIVE:-0}" = 1 ]; then echo corrupt > "$3"; else cp "$INSTALL_FIXTURES/\${1##*/}" "$3"; fi
`, { mode: 0o755 });
  for (const platform of ['linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64']) {
    const name = `golangci-lint-2.14.0-${platform}`;
    fs.mkdirSync(path.join(fixtures, name));
    fs.writeFileSync(path.join(fixtures, name, 'golangci-lint'), '#!/bin/sh\necho prebuilt\n');
    execFileSync('tar', ['-czf', path.join(fixtures, `${name}.tar.gz`), '-C', fixtures, `${name}/golangci-lint`]);
  }
  function run(extra = {}) {
    fs.writeFileSync(log, '');
    const result = spawnSync('make', ['--no-print-directory', 'install-static-check-tools', `GOPATH=${gopath}`], {
      cwd: dir, encoding: 'utf8', timeout: 10000,
      env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, INSTALL_LOG: log, INSTALL_FIXTURES: fixtures,
        INSTALL_OS: 'linux', INSTALL_ARCH: 'arm64', INSTALL_GO: 'go1.27.1', ...extra },
    });
    return { result, commands: fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean) };
  }
  for (const [INSTALL_OS, INSTALL_ARCH] of [['linux', 'amd64'], ['linux', 'arm64'], ['darwin', 'amd64'], ['darwin', 'arm64']]) {
    const { result, commands } = run({ INSTALL_OS, INSTALL_ARCH });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(commands.length, 3);
    assert.match(commands[0], new RegExp(`^download https://github.com/golangci/golangci-lint/releases/download/v2.14.0/golangci-lint-2.14.0-${INSTALL_OS}-${INSTALL_ARCH}.tar.gz [a-f0-9]{64}$`));
    assert.ok(commands[1].startsWith('install github.com/matrixorigin/linter/cmd/molint@'));
    assert.ok(commands[2].startsWith('install github.com/apache/skywalking-eyes/cmd/license-eye@'));
    assert.equal(execFileSync(path.join(gopath, 'bin/golangci-lint'), { encoding: 'utf8' }).trim(), 'prebuilt');
  }
  for (const extra of [{ INSTALL_GO: 'go1.28.0' }, { INSTALL_OS: 'freebsd' }]) {
    const { result, commands } = run(extra);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(commands.length, 3);
    assert.ok(commands.every(command => command.startsWith('install ')));
    assert.match(commands[0], /golangci-lint\/v2\/cmd\/golangci-lint@v2.14.0$/);
  }
  for (const extra of [{ FAIL_DOWNLOAD: '1' }, { BAD_ARCHIVE: '1' }]) {
    const fallback = run(extra);
    assert.equal(fallback.result.status, 0, fallback.result.stderr);
    assert.equal(fallback.commands.length, 4);
    assert.ok(fallback.commands.slice(1).every(command => command.startsWith('install ')));
    assert.match(fallback.result.stderr, /installing the pinned version from source/);
    fs.writeFileSync(path.join(gopath, 'bin/golangci-lint'), 'prior generation');
    const failed = run({ ...extra, FAIL_TOOL: 'golangci-lint' });
    assert.notEqual(failed.result.status, 0);
    assert.equal(failed.commands.length, 2, 'a failed fallback must not install other tools');
    assert.equal(fs.readFileSync(path.join(gopath, 'bin/golangci-lint'), 'utf8'), 'prior generation');
    assert.ok(!fs.readdirSync(path.join(gopath, 'bin')).some(name => name.startsWith('.golangci-lint.')));
  }
  const failure = run({ FAIL_TOOL: 'molint' });
  assert.notEqual(failure.result.status, 0);
  assert.equal(failure.commands.length, 2, 'a failed source tool must stop the installer');
});
