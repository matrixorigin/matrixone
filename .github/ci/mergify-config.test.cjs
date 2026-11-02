const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const config = fs.readFileSync(path.join(__dirname, '..', 'mergify.yml'), 'utf8');
const ciScopeWorkflow = fs.readFileSync(
  path.join(__dirname, '..', 'workflows', 'ci-scope-check.yml'),
  'utf8',
);
const queueSection = config.split('\npull_request_rules:', 1)[0];
const queueRules = queueSection
  .slice(queueSection.indexOf('  - name: '))
  .split(/\n(?=  - name: )/);

test('all merge queues use declarative commit message formatting', () => {
  assert.doesNotMatch(queueSection, /^\s+commit_message_template:/m);

  assert.deepEqual(
    queueRules.map((rule) => rule.match(/^  - name: (.+)$/m)[1]),
    ['main', 'release-4.2', 'release-4.1', 'release-4.0', 'release-3.0', 'release-2.2', 'release-2.1'],
  );

  for (const rule of queueRules) {
    assert.match(
      rule,
      /\n    commit_message_format:\n      title: pr-title\n      body: pr-body\n      trailers:\n        - approved-by(?:\n|$)/,
    );
  }
});

test('Mergify-only changes run the configuration regression', () => {
  assert.match(ciScopeWorkflow, /^\s+- '\.github\/mergify\.yml'$/m);
});
