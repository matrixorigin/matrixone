"""Offline tests use installed dependencies and scoped executor doubles."""


def pytest_configure(config):
    config.addinivalue_line('markers', 'offline: offline unit test')
    config.addinivalue_line('markers', 'vector: vector operation contract')
