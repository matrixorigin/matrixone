"""Executable design oracle, not a MatrixOne implementation or SQL comparator."""

from collections import Counter


class Conflict(ValueError):
    pass


def choose(base, feature, upstream, policy):
    if policy not in ("fail", "keep_upstream", "keep_branch"):
        raise ValueError("unknown conflict policy")
    if feature == base:
        return upstream
    if upstream == base or feature == upstream:
        return feature
    if policy == "fail":
        raise Conflict("divergent concurrent changes")
    return upstream if policy == "keep_upstream" else feature


def replay_pk(base, feature, upstream, policy="fail"):
    """Keys map to visible tuples. Absence differs from a tuple containing NULL."""
    missing = object()
    result = {}
    for key in base.keys() | feature.keys() | upstream.keys():
        row = choose(base.get(key, missing), feature.get(key, missing),
                     upstream.get(key, missing), policy)
        if row is not missing:
            result[key] = row
    # Validate even when all inputs are empty.
    choose(0, 0, 0, policy)
    return result


def replay_bag(base, feature, upstream, policy="fail"):
    """Proposed count-level semantics. Not approved as adapter compatibility."""
    inputs = [Counter(rows) for rows in (base, feature, upstream)]
    b, f, u = inputs
    result = Counter()
    choose(0, 0, 0, policy)
    for row in b.keys() | f.keys() | u.keys():
        count = choose(b[row], f[row], u[row], policy)
        if count:
            result[row] = count
    return result
