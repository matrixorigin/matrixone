"""The supplied exception owns the traceback, independent of ambient handlers."""

import io
import logging

import pytest

from matrixone.logger import MatrixOneLogger


@pytest.fixture
def error_logger():
    output = io.StringIO()
    target = logging.Logger('sdk-traceback-test', logging.ERROR)
    handler = logging.StreamHandler(output)
    target.addHandler(handler)
    try:
        yield MatrixOneLogger(logger=target), output
    finally:
        target.removeHandler(handler)
        handler.close()
        output.close()


def saved_error():
    try:
        raise ValueError('original failure')
    except ValueError as error:
        return error


def test_traceback_survives_leaving_exception_handler(error_logger):
    logger, output = error_logger
    logger.log_error(saved_error(), include_traceback=True)
    rendered = output.getvalue()
    assert 'in saved_error' in rendered
    assert 'ValueError: original failure' in rendered
    assert 'NoneType: None' not in rendered


def test_traceback_does_not_use_unrelated_active_exception(error_logger):
    logger, output = error_logger
    error = saved_error()
    try:
        raise RuntimeError('unrelated failure')
    except RuntimeError:
        logger.log_error(error, include_traceback=True)
    rendered = output.getvalue()
    assert 'in saved_error' in rendered
    assert 'ValueError: original failure' in rendered
    assert 'unrelated failure' not in rendered


def test_unraised_exception_is_logged_without_fake_traceback(error_logger):
    logger, output = error_logger
    logger.log_error(ValueError('never raised'), include_traceback=True)
    rendered = output.getvalue()
    assert 'ValueError: never raised' in rendered
    assert 'Traceback (most recent call last)' not in rendered
    assert 'NoneType: None' not in rendered


def test_traceback_remains_opt_in(error_logger):
    logger, output = error_logger
    logger.log_error(saved_error())
    rendered = output.getvalue()
    assert 'original failure' in rendered
    assert 'Traceback (most recent call last)' not in rendered
    assert 'in saved_error' not in rendered
