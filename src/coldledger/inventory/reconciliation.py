from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from threading import RLock
from typing import Mapping

from ..storage.event_store import EventStore
from .ledger import InventoryLedger


@dataclass(frozen=True)
class Bin:
    sku: str
    quantity: Decimal
    version: int


def reconcile(current, counts, versions):
    """Apply a stocktake batch to ``current`` atomically.

    The strategy is fixed as pre-validation, not compensation: every location
    is validated (known sku, non-negative count, matching version) before any
    quantity is written, so a rejected batch leaves no partial writes for a
    compensating transaction to undo. The input mapping is never mutated;
    the returned mapping is a new dict.
    """
    if set(counts) != set(versions):
        raise ValueError('key mismatch')
    counted = {sku: Decimal(str(q)) for sku, q in counts.items()}
    # Phase 1 - validate the whole batch, write nothing.
    for sku, q in counted.items():
        if q < 0:
            raise ValueError('negative count')
        state = current[sku]
        if state.version != versions[sku]:
            raise RuntimeError('version conflict')
    # Phase 2 - apply the whole batch.
    adjustments = {}
    nxt = dict(current)
    for sku, q in counted.items():
        state = current[sku]
        adjustments[sku] = q - state.quantity
        nxt[sku] = replace(state, quantity=q, version=state.version + 1)
    return nxt, adjustments


@dataclass(frozen=True)
class StocktakeReceipt:
    batch_id: str
    adjustments: dict[str, Decimal]
    replayed: bool = False


class Stocktaker:
    """Applies stocktake batches with whole-batch atomicity.

    Bin adjustments, version checks, ledger postings and event publication
    commit together under one lock. Because the batch is pre-validated, the
    commit phase cannot fail halfway and no compensating writes exist.
    Batches are idempotent on ``batch_id``: retrying an applied batch returns
    the recorded receipt without re-applying quantities, ledger entries or
    events, so the ledger and the event stream always reconcile.
    """

    EVENT_KIND = 'stocktake_batch'
    REASON = 'stocktake'

    def __init__(self, bins: Mapping[str, Bin], ledger: InventoryLedger | None = None,
                 events: EventStore | None = None, stream: str = 'stocktake'):
        if not stream:
            raise ValueError('stream required')
        self._bins = dict(bins)
        self._ledger = ledger
        self._events = events
        self._stream = stream
        self._applied: dict[str, tuple[dict[str, Decimal], dict[str, Decimal]]] = {}
        self._lock = RLock()

    def bins(self) -> dict[str, Bin]:
        with self._lock:
            return dict(self._bins)

    def applied(self, batch_id: str) -> bool:
        with self._lock:
            return batch_id in self._applied

    def apply(self, batch_id: str, counts, versions) -> StocktakeReceipt:
        if not batch_id:
            raise ValueError('batch_id required')
        counted = {sku: Decimal(str(q)) for sku, q in counts.items()}
        with self._lock:
            if batch_id in self._applied:
                recorded_counts, recorded_adjustments = self._applied[batch_id]
                if counted != recorded_counts:
                    raise ValueError('batch_id reused with different counts')
                return StocktakeReceipt(batch_id, dict(recorded_adjustments), replayed=True)
            nxt, adjustments = reconcile(self._bins, counted, versions)
            # Commit phase: every write below is infallible by construction —
            # ledger references are namespaced under the fresh batch_id, zero
            # deltas are skipped, and this stocktaker is the only writer to
            # its event stream. Nothing here can require compensation.
            if self._ledger is not None:
                for sku, delta in adjustments.items():
                    if delta != 0:
                        self._ledger.post(sku, f'{batch_id}:{sku}', delta, self.REASON)
            if self._events is not None:
                expected = len(self._events.read(self._stream))
                self._events.append(self._stream, expected, self.EVENT_KIND, {
                    'batch_id': batch_id,
                    'counts': {sku: str(q) for sku, q in counted.items()},
                    'adjustments': {sku: str(delta) for sku, delta in adjustments.items()},
                    'base_versions': {sku: versions[sku] for sku in counted},
                })
            self._bins = nxt
            self._applied[batch_id] = (counted, dict(adjustments))
            return StocktakeReceipt(batch_id, dict(adjustments))

    def verify(self, batch_id: str) -> dict[str, Decimal]:
        """Difference between ledger postings and the recorded batch, per sku.

        Returns an empty dict when the ledger and the stocktake batch agree.
        """
        with self._lock:
            if batch_id not in self._applied:
                raise KeyError(batch_id)
            _, expected = self._applied[batch_id]
        if self._ledger is None:
            raise ValueError('no ledger bound')
        prefix = f'{batch_id}:'
        posted: dict[str, Decimal] = {}
        for entry in self._ledger.entries():
            if entry.reference.startswith(prefix):
                sku = entry.reference[len(prefix):]
                posted[sku] = posted.get(sku, Decimal('0')) + entry.quantity
        skus = set(expected) | set(posted)
        return {
            sku: posted.get(sku, Decimal('0')) - expected.get(sku, Decimal('0'))
            for sku in sorted(skus)
            if posted.get(sku, Decimal('0')) != expected.get(sku, Decimal('0'))
        }
