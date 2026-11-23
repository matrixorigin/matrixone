# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Invalid CDC duration arguments remain recoverable CLI input errors."""

import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from matrixone import cli_tools


@pytest.mark.parametrize('value', ['inf', 'infs', '1e309h', '1e20m', 'nan', '-1s'])
def test_invalid_duration_raises_value_error(value):
    with pytest.raises(ValueError):
        cli_tools._parse_duration(value)


@pytest.mark.parametrize(
    'value, expected',
    [
        ('0s', datetime.timedelta(0)),
        ('1.5h', datetime.timedelta(minutes=90)),
        ('30s', datetime.timedelta(seconds=30)),
        ('2', datetime.timedelta(minutes=2)),
        (' 5M ', datetime.timedelta(minutes=5)),
    ],
)
def test_valid_durations_keep_their_units(value, expected):
    assert cli_tools._parse_duration(value) == expected


@pytest.mark.parametrize('value', ['inf', '1e20'])
@pytest.mark.parametrize(
    'command',
    [
        'cdc_health --threshold={value}',
        'cdc_health {value}',
        'cdc_task example --threshold={value}',
        'cdc_task example --threshold {value}',
    ],
)
def test_cdc_commands_reject_overflow_without_exiting(command, value, monkeypatch, capsys):
    monkeypatch.setattr(cli_tools, 'PROMPT_TOOLKIT_AVAILABLE', False)
    cdc = Mock()
    cli = cli_tools.MatrixOneCLI(SimpleNamespace(cdc=cdc))
    assert not cli.onecmd(command.format(value=value))
    assert capsys.readouterr().out
    assert cdc.mock_calls == []
    # The same interpreter remains usable after rejecting the invalid input.
    cli.onecmd('help cdc_health')
    assert 'Check CDC task health' in capsys.readouterr().out
