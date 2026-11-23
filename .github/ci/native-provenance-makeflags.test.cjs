const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

// Run the real target with only the external provenance program replaced.
function withFixture(t, run) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mo-native-makeflags-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.mkdirSync(path.join(dir, 'cgo'));
  fs.mkdirSync(path.join(dir, 'pkg'));
  fs.copyFileSync(path.join(__dirname, '..', '..', 'Makefile'), path.join(dir, 'Makefile'));
  const log = path.join(dir, 'events');
  const stub = path.join(dir, 'cgo', 'mo-native-provenance');
  fs.writeFileSync(stub, '#!/bin/sh\nprintf "%s\\n" "$1" >> "$MO_NATIVE_TEST_LOG"\n' +
    'case "$1" in prepare) echo "$MO_NATIVE_TEST_ACTION" ;; begin) ;; *) exit 1 ;; esac\n', { mode: 0o755 });
  fs.mkdirSync(path.join(dir, 'thirdparties'));
  for (const name of ['cgo', 'thirdparties']) {
    fs.writeFileSync(path.join(dir, name, 'Makefile'),
      `.PHONY: clean\nclean:\n\t@echo clean-${name} >> "$$MO_NATIVE_TEST_LOG"\n` +
      `\t@test "$$MO_NATIVE_TEST_FAIL" != ${name}\n`);
  }
  fs.writeFileSync(path.join(dir, 'parent.mk'),
    '.PHONY: parent\nparent:\n\t@$(MAKE) -f Makefile cgo-native-prepare-internal\n');
  const env = { ...process.env, MO_NATIVE_TEST_LOG: log };
  for (const name of ['MAKEFLAGS', 'MFLAGS', 'GNUMAKEFLAGS', 'MAKELEVEL',
    'GOOS', 'GOARCH', 'TARGET_OS', 'TARGET_ARCH', 'MO_CL_CUDA', 'MO_SIRIUS']) delete env[name];

  run((args = [], { simulatedFlags = null, recursive = false, action = 'local', fail = '', status = 0 } = {}) => {
    fs.rmSync(log, { force: true });
    const makefiles = ['-f', recursive ? 'parent.mk' : 'Makefile'];
    if (simulatedFlags !== null) {
      fs.writeFileSync(path.join(dir, 'flags.mk'), `override MAKEFLAGS := ${simulatedFlags}\n`);
      makefiles.push('-f', 'flags.mk');
    }
    const target = recursive ? 'parent' : 'cgo-native-prepare-internal';
    const result = spawnSync('make', [...makefiles, ...args, target], {
      cwd: dir, env: { ...env, MO_NATIVE_TEST_ACTION: action, MO_NATIVE_TEST_FAIL: fail },
      encoding: 'utf8', timeout: 15000,
    });
    assert.equal(result.status, status, `${args} ${simulatedFlags}: ${result.stderr}`);
    return fs.existsSync(log) ? fs.readFileSync(log, 'utf8').trim().split('\n') : [];
  }, dir);
}

test('native prepare runs for actual and assignment-only MAKEFLAGS', t => {
  withFixture(t, make => {
    for (const flags of [null, 'ONNX_PREBUILT_DIR=/tmp/onnx', 'QUESTION=none',
      'NATIVE_PROVENANCE_SIMSIMD=system', '-- ONNX_PREBUILT_DIR=/tmp/onnx']) {
      assert.deepEqual(make([], { simulatedFlags: flags }), ['prepare', 'begin'], String(flags));
    }
    for (const args of [['ONNX_PREBUILT_DIR=/tmp/onnx'], ['SOME=foo --touch'],
      ['SOME=foo --dry-run'], ['SOME=foo -n'], ['SOME=foo "bar'],
      ['-k'], ['-j2'], ['-I', '/tmp/name'], ['-I', '--touch'],
      ['--no-print-directory', 'MFLAGS=-n']]) {
      assert.deepEqual(make(args), ['prepare', 'begin'], String(args));
    }
    assert.deepEqual(make(['ONNX_PREBUILT_DIR=/tmp/onnx'], { recursive: true }), ['prepare', 'begin']);
  });
});

test('native prepare does not run in dry-run, touch or question mode', t => {
  withFixture(t, make => {
    for (const args of [['-n'], ['--just-print'], ['-t'], ['--touch'],
      ['-kn', 'ONNX_PREBUILT_DIR=/tmp/onnx'], ['MAKEFLAGS=-n'],
      ['MAKEFLAGS=-t'], ['MAKEFLAGS=--dry-run'],
      ['MAKEFLAGS=--just-print'], ['MAKEFLAGS=--recon'], ['MAKEFLAGS=--touch'],
      ['MAKEFLAGS=-k --touch'], ['MAKEFLAGS=-kn'], ['-n', 'MAKEFLAGS='],
      ['--warn-undefined-variables', '-n'], ['--no-print-directory', '-n'],
      ['-I', '/tmp/name', '-n']]) {
      assert.deepEqual(make(args), [], String(args));
    }
    for (const args of [['-q'], ['--question'], ['MAKEFLAGS=-q'], ['MAKEFLAGS=--question']]) {
      assert.deepEqual(make(args, { status: 1 }), [], String(args));
    }
    assert.deepEqual(make(['-n', 'ONNX_PREBUILT_DIR=/tmp/onnx'], { recursive: true }), []);
  });
});

test('native prepare preserves clean order and stops if a clean fails', t => {
  withFixture(t, (make, dir) => {
    const fatbin = path.join(dir, 'mocl_kernel64.fatbin');
    fs.writeFileSync(fatbin, 'fixture');
    assert.deepEqual(make([], { action: 'reuse' }), ['prepare', 'begin']);
    assert.ok(fs.existsSync(fatbin));
    assert.deepEqual(make([], { action: 'rebuild-cgo' }), ['prepare', 'clean-cgo', 'begin']);
    assert.equal(fs.existsSync(fatbin), false);
    fs.writeFileSync(fatbin, 'fixture');
    assert.deepEqual(make(['-j2'], { action: 'rebuild-all' }),
      ['prepare', 'clean-cgo', 'clean-thirdparties', 'begin']);
    assert.equal(fs.existsSync(fatbin), false);
    fs.writeFileSync(fatbin, 'fixture');
    assert.deepEqual(make([], { action: 'rebuild-all', fail: 'cgo', status: 2 }),
      ['prepare', 'clean-cgo']);
    assert.ok(fs.existsSync(fatbin));
    assert.deepEqual(make([], { action: 'rebuild-all', fail: 'thirdparties', status: 2 }),
      ['prepare', 'clean-cgo', 'clean-thirdparties']);
    assert.equal(fs.existsSync(fatbin), false);
    assert.deepEqual(make([], { action: 'invalid', status: 2 }), ['prepare']);
    fs.writeFileSync(fatbin, 'fixture');
    assert.deepEqual(make(['-n'], { action: 'rebuild-all' }), []);
    assert.ok(fs.existsSync(fatbin));
  });
});
