const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const test = require('node:test');

// Exercise the exact Make recipe guard without running the expensive CGo build.
const makefile = fs.readFileSync(path.join(__dirname, '..', '..', 'Makefile'), 'utf8');
const target = '\ncgo-native-prepare-internal:\n';
const start = makefile.indexOf(target);
assert.notEqual(start, -1, 'native provenance target must exist');
const recipeStart = start + target.length;
const actionStart = makefile.indexOf('\t\taction=$$(GO=', recipeStart);
assert.ok(actionStart > recipeStart, 'native provenance prepare action must exist');
const guard = makefile.slice(recipeStart, actionStart);
assert.match(guard, /firstword \$\(MAKEFLAGS\)/);

function runsPrepare(flags) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mo-native-flags-'));
  const marker = path.join(dir, 'prepared');
  try {
    const source = `override MAKEFLAGS := ${flags}\n.PHONY: probe\nprobe:\n${guard}\t\tprintf 'prepared' > '${marker}'\n`;
    const result = spawnSync('make', ['--no-print-directory', '-s', '-f', '-', 'probe'], {
      input: source,
      encoding: 'utf8',
      timeout: 10000,
    });
    // Question mode exits 1 when the phony probe would have to be remade.
    assert.ok(result.status === 0 || (flags === 'q' && result.status === 1),
      `make failed with flags ${flags}: ${result.stderr}`);
    return fs.existsSync(marker);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('Make command-line assignments never disable native provenance preparation', () => {
  for (const flags of [
    'ONNX_PREBUILT_DIR=/tmp/onnx',
    'TEST_NAME=/tmp/name',
    'NATIVE_PROVENANCE_SIMSIMD=system',
    'QUESTION=foo',
    '-- ONNX_PREBUILT_DIR=/tmp/onnx',
  ]) {
    assert.equal(runsPrepare(flags), true, `failed to prepare under ${flags}`);
  }
});

test('Actual dry-run/touch/question flags still suppress preparation', () => {
  for (const flags of [
    'n', 't', 'q', 'kn', 'nt',
    'n -- ONNX_PREBUILT_DIR=/tmp/onnx',
    '-- --just-print', '-- --dry-run', '-- --touch', '-- --question',
  ]) {
    assert.equal(runsPrepare(flags), false, `unexpected prepare under ${flags}`);
  }
});
