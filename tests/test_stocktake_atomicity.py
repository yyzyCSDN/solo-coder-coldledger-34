import unittest
from decimal import Decimal

from coldledger.inventory.ledger import InventoryLedger
from coldledger.inventory.reconciliation import Bin, Stocktaker, reconcile
from coldledger.storage.event_store import EventStore


def make_bins(**specs):
    return {sku: Bin(sku, Decimal(qty), ver) for sku, (qty, ver) in specs.items()}


class ReconcileAtomicityTests(unittest.TestCase):
    def test_conflict_on_second_location_writes_nothing(self):
        current = make_bins(A=('10', 3), B=('4', 1))
        before = dict(current)
        with self.assertRaises(RuntimeError):
            reconcile(current, {'A': 8, 'B': 9}, {'A': 3, 'B': 2})
        self.assertEqual(current, before)

    def test_negative_count_rejected_before_any_write(self):
        current = make_bins(A=('10', 0), B=('4', 0))
        before = dict(current)
        with self.assertRaises(ValueError):
            reconcile(current, {'A': 8, 'B': -1}, {'A': 0, 'B': 0})
        self.assertEqual(current, before)

    def test_unknown_sku_rejected_before_any_write(self):
        current = make_bins(A=('10', 0))
        before = dict(current)
        with self.assertRaises(KeyError):
            reconcile(current, {'A': 8, 'Z': 1}, {'A': 0, 'Z': 0})
        self.assertEqual(current, before)

    def test_key_mismatch_rejected(self):
        current = make_bins(A=('10', 0))
        with self.assertRaises(ValueError):
            reconcile(current, {'A': 8}, {'A': 0, 'B': 0})

    def test_success_applies_every_location_and_returns_new_mapping(self):
        current = make_bins(A=('10', 0), B=('4', 0))
        nxt, adjustments = reconcile(current, {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        self.assertEqual(nxt['A'], Bin('A', Decimal('8'), 1))
        self.assertEqual(nxt['B'], Bin('B', Decimal('6'), 1))
        self.assertEqual(adjustments, {'A': Decimal('-2'), 'B': Decimal('2')})
        self.assertEqual(current['A'].quantity, Decimal('10'))
        self.assertEqual(current['A'].version, 0)


class StocktakerTests(unittest.TestCase):
    def setUp(self):
        self.ledger = InventoryLedger()
        self.events = EventStore()
        self.stocktaker = Stocktaker(
            make_bins(A=('10', 0), B=('4', 0)),
            ledger=self.ledger,
            events=self.events,
        )

    def test_failed_batch_leaves_no_trace_anywhere(self):
        with self.assertRaises(RuntimeError):
            self.stocktaker.apply('b1', {'A': 8, 'B': 9}, {'A': 0, 'B': 1})
        self.assertEqual(self.stocktaker.bins()['A'].quantity, Decimal('10'))
        self.assertEqual(self.stocktaker.bins()['B'].quantity, Decimal('4'))
        self.assertEqual(self.ledger.entries(), ())
        self.assertEqual(self.events.read('stocktake'), [])
        self.assertFalse(self.stocktaker.applied('b1'))

    def test_retry_after_failure_applies_exactly_once(self):
        with self.assertRaises(RuntimeError):
            self.stocktaker.apply('b1', {'A': 8, 'B': 9}, {'A': 0, 'B': 1})
        receipt = self.stocktaker.apply('b1', {'A': 8, 'B': 9}, {'A': 0, 'B': 0})
        self.assertFalse(receipt.replayed)
        self.assertEqual(self.stocktaker.bins()['A'].version, 1)
        self.assertEqual(self.stocktaker.bins()['B'].version, 1)
        self.assertEqual(len(self.ledger.entries()), 2)
        self.assertEqual(len(self.events.read('stocktake')), 1)

    def test_retry_of_applied_batch_does_not_reapply(self):
        first = self.stocktaker.apply('b1', {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        second = self.stocktaker.apply('b1', {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        self.assertEqual(second.adjustments, first.adjustments)
        self.assertEqual(self.stocktaker.bins()['A'], Bin('A', Decimal('8'), 1))
        self.assertEqual(len(self.ledger.entries()), 2)
        self.assertEqual(len(self.events.read('stocktake')), 1)

    def test_batch_id_reuse_with_different_counts_rejected(self):
        self.stocktaker.apply('b1', {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        with self.assertRaises(ValueError):
            self.stocktaker.apply('b1', {'A': 7, 'B': 6}, {'A': 0, 'B': 0})
        self.assertEqual(self.stocktaker.bins()['A'].quantity, Decimal('8'))

    def test_ledger_and_event_reconcile_with_batch(self):
        self.stocktaker.apply('b1', {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        self.assertEqual(self.stocktaker.verify('b1'), {})
        (event,) = self.events.read('stocktake')
        self.assertEqual(event.kind, 'stocktake_batch')
        self.assertEqual(event.payload['batch_id'], 'b1')
        self.assertEqual(event.payload['adjustments'], {'A': '-2', 'B': '2'})
        refs = {e.reference for e in self.ledger.entries()}
        self.assertEqual(refs, {'b1:A', 'b1:B'})

    def test_zero_delta_skips_ledger_but_stays_in_batch_record(self):
        receipt = self.stocktaker.apply('b1', {'A': 10, 'B': 6}, {'A': 0, 'B': 0})
        self.assertEqual(receipt.adjustments['A'], Decimal('0'))
        self.assertEqual([e.account for e in self.ledger.entries()], ['B'])
        self.assertEqual(self.stocktaker.verify('b1'), {})

    def test_verify_flags_divergence(self):
        self.stocktaker.apply('b1', {'A': 8, 'B': 6}, {'A': 0, 'B': 0})
        self.ledger.post('A', 'unrelated', 1, 'manual')
        self.assertEqual(self.stocktaker.verify('b1'), {})
        self.ledger.post('C', 'b1:C', 1, 'stocktake')
        self.assertEqual(self.stocktaker.verify('b1'), {'C': Decimal('1')})


if __name__ == '__main__':
    unittest.main()
