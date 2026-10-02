import unittest
from decimal import Decimal

from coldledger.inventory.ledger import InventoryLedger
from coldledger.inventory.reconciliation import Bin, reconcile
from coldledger.inventory.stocktake import StocktakeBatch, StocktakeService
from coldledger.storage.event_store import EventStore

D = Decimal


def bins():
    return {
        "A": Bin("A", D("10"), 1),
        "B": Bin("B", D("20"), 1),
        "C": Bin("C", D("30"), 1),
    }


class ReconcileAtomicityTests(unittest.TestCase):
    def test_conflict_on_second_bin_leaves_first_untouched(self):
        state = bins()
        original = dict(state)
        with self.assertRaises(RuntimeError):
            reconcile(state, {"A": D("11"), "B": D("25")}, {"A": 1, "B": 999})
        self.assertEqual(state, original)

    def test_apply_returns_new_state_without_mutating_input(self):
        state = bins()
        nxt, adjustments = reconcile(state, {"A": D("12"), "B": D("18")}, {"A": 1, "B": 1})
        self.assertEqual(adjustments, {"A": D("2"), "B": D("-2")})
        self.assertEqual(nxt["A"], Bin("A", D("12"), 2))
        self.assertEqual(nxt["B"], Bin("B", D("18"), 2))
        self.assertEqual(state["A"], Bin("A", D("10"), 1))


class StocktakeServiceTests(unittest.TestCase):
    def setUp(self):
        self.state = bins()
        self.ledger = InventoryLedger()
        self.events = EventStore()
        self.service = StocktakeService(self.state, self.ledger, self.events)

    def batch(self, batch_id="B-1", versions=None, counts=None):
        return StocktakeBatch(
            batch_id,
            counts or {"A": D("12"), "B": D("18")},
            versions or {"A": 1, "B": 1},
            "cycle-count",
        )

    def test_conflict_is_all_or_nothing_then_retry_with_fresh_versions(self):
        with self.assertRaises(RuntimeError):
            self.service.apply(self.batch(versions={"A": 1, "B": 999}))
        self.assertEqual(self.state["A"].quantity, D("10"))
        self.assertEqual(self.state["B"].quantity, D("20"))
        self.assertEqual(self.ledger.entries(), ())
        self.assertEqual(self.events.read("stocktake"), [])
        # failed batch left no idempotency record, so a corrected retry commits
        receipt = self.service.apply(self.batch())
        self.assertTrue(receipt.applied)
        self.assertEqual(self.state["A"].quantity, D("12"))
        self.assertEqual(self.state["B"].quantity, D("18"))

    def test_committed_batch_ties_out_state_ledger_and_event(self):
        receipt = self.service.apply(self.batch())
        self.assertEqual(receipt.adjustments, {"A": D("2"), "B": D("-2")})
        self.assertEqual(self.ledger.balance("A"), D("2"))
        self.assertEqual(self.ledger.balance("B"), D("-2"))
        (event,) = self.events.read("stocktake")
        self.assertEqual(event.sequence, receipt.event_sequence)
        posted = {e.account: e.quantity for e in self.ledger.entries()}
        self.assertEqual({k: D(v) for k, v in event.payload["adjustments"].items()}, posted)
        self.assertEqual(event.payload["references"], {"A": "B-1:A", "B": "B-1:B"})

    def test_retry_of_committed_batch_does_not_reapply(self):
        first = self.service.apply(self.batch())
        second = self.service.apply(self.batch())
        self.assertFalse(second.applied)
        self.assertEqual(second.event_sequence, first.event_sequence)
        self.assertEqual(self.state["A"].quantity, D("12"))
        self.assertEqual(len(self.ledger.entries()), 2)
        self.assertEqual(len(self.events.read("stocktake")), 1)

    def test_batch_id_reuse_with_different_payload_is_rejected(self):
        self.service.apply(self.batch())
        with self.assertRaises(ValueError):
            self.service.apply(self.batch(counts={"A": D("99"), "B": D("18")}))

    def test_zero_delta_posts_no_ledger_entry_but_versions_bump(self):
        receipt = self.service.apply(self.batch(counts={"A": D("10"), "B": D("18")}))
        self.assertEqual(receipt.adjustments, {"B": D("-2")})
        self.assertEqual([e.account for e in self.ledger.entries()], ["B"])
        self.assertEqual(self.state["A"].version, 2)


if __name__ == "__main__":
    unittest.main()
