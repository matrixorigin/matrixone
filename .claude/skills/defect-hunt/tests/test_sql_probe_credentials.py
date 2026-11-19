"""Regression: defect-hunt SQL probe never exposes database passwords in argv."""
import contextlib
import io
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import types
import unittest
from unittest import mock


ADAPTER = Path(__file__).resolve().parents[1] / 'references' / 'matrixone-adapter.md'


class SQLProbeCredentialTests(unittest.TestCase):
    def setUp(self):
        self.secret = 'synthetic-password--NOT-A-REAL-SECRET'
        self.env = mock.patch.dict(os.environ, {'MO_PORT': '6001', 'MO_PASS': self.secret})
        self.env.start()
        self.addCleanup(self.env.stop)
        doc = ADAPTER.read_text(encoding='utf-8')
        match = re.search(r'\x60\x60\x60python\n(.*?)\n\x60\x60\x60', doc, re.DOTALL)
        self.assertIsNotNone(match)
        self.ns = {}
        exec(compile(match.group(1), str(ADAPTER), 'exec'), self.ns)
        self.addCleanup(lambda: shutil.rmtree(self.ns['EVIDENCE'], ignore_errors=True))

    def run_probe(self, *, error=None, database=None, check_parser=False):
        seen = {}
        real_run = subprocess.run

        def fake_run(command, **kwargs):
            self.assertEqual(command[0], 'mysql')
            if self.secret:
                self.assertNotIn(self.secret, '\0'.join(command))
            self.assertNotIn('--password', '\0'.join(command))
            self.assertTrue(command[1].startswith('--defaults-file='))
            self.assertEqual(command[2], '--no-login-paths')
            self.assertIn('--protocol=TCP', command)
            self.assertIn('-N', command)
            self.assertIn('--raw', command)
            if database is not None:
                self.assertIn(database, command)
            auth = Path(command[1].split('=', 1)[1])
            self.assertTrue(auth.is_file())
            self.assertEqual(stat.S_IMODE(auth.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(auth.parent.stat().st_mode), 0o700)
            content = auth.read_bytes()
            self.assertTrue(content.startswith(b'[client]\npassword='))
            if check_parser:
                parsed = real_run(
                    ['my_print_defaults', f'--defaults-file={auth}', '--show', 'client'],
                    capture_output=True, check=True,
                )
                self.assertEqual(b'--password=' + os.fsencode(self.secret) + b'\n', parsed.stdout)
            seen['auth'] = auth
            if error is not None:
                raise error
            return types.SimpleNamespace(returncode=0)

        with mock.patch.object(subprocess, 'run', side_effect=fake_run):
            with contextlib.redirect_stdout(io.StringIO()):
                result = self.ns['show']('probe', 'SELECT VERSION(), 1', db=database)
        self.assertIn('auth', seen)
        self.assertFalse(seen['auth'].exists(), 'private credentials must be deleted after launch')
        if self.secret:
            self.assertNotIn(self.secret, str(result))
            self.assertNotIn(self.secret, (Path(result['evidence']) / 'result.json').read_text())
        return result

    def test_argv_is_secret_free_and_private_file_is_deleted(self):
        result = self.run_probe()
        self.assertEqual(result['status'], 'exited')
        self.assertEqual(result['rc'], 0)

    def test_database_argument_and_result_preserved(self):
        result = self.run_probe(database='isolated_probe')
        self.assertEqual(result['database'], 'isolated_probe')

    @unittest.skipUnless(shutil.which('my_print_defaults'), 'MySQL option parser unavailable')
    def test_mysql_option_parser_preserves_password(self):
        passwords = ['', 'plain', ' a#b ', 'quote"slash\\', '中文']
        passwords += ['a' + chr(c) + 'b' for c in range(1, 32) if c not in (10, 13)]
        if os.name == 'posix':
            passwords.append(os.fsdecode(b'nonutf-\xff-value'))
        for index, password in enumerate(passwords):
            with self.subTest(case=index):
                self.secret = password
                self.ns['os'].environ['MO_PASS'] = password
                self.run_probe(check_parser=True)

    def test_timeout_cleans_credentials(self):
        result = self.run_probe(error=subprocess.TimeoutExpired('mysql', 1))
        self.assertEqual(result['status'], 'timeout')

    def test_launch_error_cleans_credentials(self):
        result = self.run_probe(error=OSError('synthetic subprocess failure'))
        self.assertEqual(result['status'], 'launch-error')

    def test_password_with_line_break_is_rejected_without_launching_mysql(self):
        for password in ('first\nsecond', 'first\rsecond'):
            with self.subTest(password=repr(password)):
                self.ns['os'].environ['MO_PASS'] = password
                with mock.patch.object(subprocess, 'run') as called:
                    with self.assertRaisesRegex(ValueError, 'MO_PASS'):
                        self.ns['show']('bad-password', 'SELECT 1')
                called.assert_not_called()


if __name__ == '__main__':
    unittest.main()
