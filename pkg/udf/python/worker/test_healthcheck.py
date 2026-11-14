import unittest

import healthcheck


class _Result:
    def __init__(self, body):
        self.body = body


class _Reader:
    def __init__(self, results):
        self.results = iter(results)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.results)


class _Client:
    def __init__(self, address, results):
        self.address = address
        self.results = results
        self.action = None

    def do_action(self, action):
        self.action = action
        return _Reader(self.results)


class HealthcheckTest(unittest.TestCase):
    def test_accepts_current_capability_protocol(self):
        clients = []

        def factory(address):
            client = _Client(
                address, [_Result(b'{"protocol_version":1}')]
            )
            clients.append(client)
            return client

        capabilities = healthcheck.check_capabilities(
            "grpc://worker:50051", client_factory=factory
        )

        self.assertEqual(capabilities["protocol_version"], 1)
        self.assertEqual(clients[0].address, "grpc://worker:50051")
        self.assertEqual(clients[0].action.type, "GetPythonCapabilities")
        self.assertEqual(clients[0].action.body, b'{"protocol_version":1}')

    def test_rejects_empty_or_unknown_protocol(self):
        for response in (None, b'{"protocol_version":2}'):
            def factory(address, response=response):
                results = [] if response is None else [_Result(response)]
                return _Client(address, results)

            with self.assertRaises(RuntimeError):
                healthcheck.check_capabilities(client_factory=factory)


if __name__ == "__main__":
    unittest.main()
