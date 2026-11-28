"""Capability health check used by the container orchestrator."""

import json

import pyarrow.flight as flight


PROTOCOL_VERSION = 1
CAPABILITY_ACTION = "GetPythonCapabilities"


def check_capabilities(
    address="grpc://127.0.0.1:50051", client_factory=flight.FlightClient
):
    client = client_factory(address)
    reader = client.do_action(
        flight.Action(CAPABILITY_ACTION, b'{"protocol_version":1}')
    )
    result = next(iter(reader), None)
    if result is None:
        raise RuntimeError("empty capability response")
    capabilities = json.loads(bytes(result.body))
    if capabilities.get("protocol_version") != PROTOCOL_VERSION:
        raise RuntimeError("unsupported capability protocol version")
    return capabilities


def main():
    check_capabilities()


if __name__ == "__main__":
    main()
