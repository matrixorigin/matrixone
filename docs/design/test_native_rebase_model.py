"""Small independent contract examples; no database or transaction validation."""

import itertools
import unittest
from collections import Counter

from native_rebase_model import Conflict, choose, replay_bag, replay_pk


class RebaseContractTests(unittest.TestCase):
    def test_decision_table(self):
        for b, f, u in itertools.product((0, 1, 2), repeat=3):
            with self.subTest(base=b, feature=f, upstream=u):
                conflict = f != b and u != b and f != u
                if conflict:
                    with self.assertRaises(Conflict):
                        choose(b, f, u, "fail")
                    self.assertEqual(choose(b, f, u, "keep_upstream"), u)
                    self.assertEqual(choose(b, f, u, "keep_branch"), f)
                else:
                    expected = u if f == b else f
                    self.assertEqual(choose(b, f, u, "fail"), expected)

    def test_disjoint_updates_and_insert(self):
        base = {1: ("a",), 2: ("b",)}
        feature = {1: ("changed",), 2: ("b",)}
        upstream = {1: ("a",), 2: ("other",), 3: ("new",)}
        self.assertEqual(replay_pk(base, feature, upstream),
                         {1: ("changed",), 2: ("other",), 3: ("new",)})
        self.assertEqual(base, {1: ("a",), 2: ("b",)})

    def test_delete_update_conflict(self):
        args = ({1: (10,)}, {}, {1: (20,)})
        with self.assertRaises(Conflict):
            replay_pk(*args)
        self.assertEqual(replay_pk(*args, "keep_branch"), {})
        self.assertEqual(replay_pk(*args, "keep_upstream"), {1: (20,)})

    def test_equal_inserts_coalesce(self):
        self.assertEqual(replay_pk({}, {1: (2,)}, {1: (2,)}), {1: (2,)})

    def test_null_not_absence_and_composite_key(self):
        key = (1, "warehouse")
        self.assertEqual(replay_pk({}, {key: (None,)}, {}), {key: (None,)})
        self.assertEqual(replay_pk({key: (None,)}, {}, {key: (None,)}), {})

    def test_repeated_rebase_new_base(self):
        base = {1: (10,), 2: (20,)}
        feature = {1: (11,), 2: (20,)}
        upstream = {1: (10,), 2: (21,)}
        result = replay_pk(base, feature, upstream)
        newer = {1: (10,), 2: (22,)}
        self.assertEqual(replay_pk(upstream, result, newer),
                         {1: (11,), 2: (22,)})

    def test_bag_ignores_regenerated_hidden_keys(self):
        before = [(101, ("payment", 100)), (102, ("payment", 100))]
        after = [(205, ("payment", 100)), (206, ("payment", 100))]
        visible = lambda rows: [row for _, row in rows]
        self.assertEqual(replay_bag(visible(before), visible(after), visible(before)),
                         Counter({("payment", 100): 2}))

    def test_bag_duplicate_delete_one_and_null(self):
        row = (None, 10)
        self.assertEqual(replay_bag([row, row], [row], [row, row]),
                         Counter({row: 1}))

    def test_bag_divergent_counts(self):
        row = (7,)
        with self.assertRaises(Conflict):
            replay_bag([row, row], [row], [row, row, row])

    def test_invalid_policy_empty_inputs(self):
        for function, args in ((replay_pk, ({}, {}, {})),
                               (replay_bag, ([], [], []))):
            with self.assertRaises(ValueError):
                function(*args, "typo")


if __name__ == "__main__":
    unittest.main()
