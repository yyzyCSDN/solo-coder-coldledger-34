from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from threading import RLock

from ..storage.event_store import EventStore
from .ledger import InventoryLedger
from .reconciliation import Bin, reconcile

DEFAULT_STREAM = "stocktake"


@dataclass(frozen=True)
class StocktakeBatch:
    batch_id: str
    counts: dict[str, Decimal]
    versions: dict[str, int]
    reason: str = "stocktake-adjustment"


@dataclass(frozen=True)
class StocktakeReceipt:
    batch_id: str
    adjustments: dict[str, Decimal]
    event_sequence: int
    applied: bool


class StocktakeService:
    """Applies a multi-bin stocktake batch with whole-batch atomicity.

    The strategy is fixed to pre-validation, not compensation: every
    failure point (key mismatch, negative counts, optimistic version
    conflicts, duplicate ledger references, event stream position) is
    checked before the first write, then bin state, ledger entries and
    the stocktake event are committed together under one lock. The event
    store is append-only, so compensating reversals are never used.
    Committed batch ids are remembered, so a retry of the same batch
    returns the original receipt instead of re-applying it.
    """

    def __init__(self, bins: dict[str, Bin], ledger: InventoryLedger, events: EventStore, stream: str = DEFAULT_STREAM):
        self._bins = bins
        self._ledger = ledger
        self._events = events
        self._stream = stream
        self._committed: dict[str, StocktakeReceipt] = {}
        self._payloads: dict[str, tuple] = {}
        self._lock = RLock()

    def apply(self, batch: StocktakeBatch) -> StocktakeReceipt:
        if not batch.batch_id:
            raise ValueError("batch id required")
        if not batch.reason:
            raise ValueError("reason required")
        counts = {sku: Decimal(str(q)) for sku, q in batch.counts.items()}
        if not counts:
            raise ValueError("empty stocktake batch")
        payload = (tuple(sorted(counts.items())), tuple(sorted(batch.versions.items())), batch.reason)
        with self._lock:
            committed = self._committed.get(batch.batch_id)
            if committed is not None:
                if self._payloads[batch.batch_id] != payload:
                    raise ValueError("batch id reused with a different payload")
                return replace(committed, applied=False)
            # phase 1: pre-validate the whole batch, write nothing
            nxt, deltas = reconcile(self._bins, counts, batch.versions)
            adjustments = {sku: d for sku, d in deltas.items() if d != 0}
            references = {sku: f"{batch.batch_id}:{sku}" for sku in adjustments}
            for sku, reference in references.items():
                if self._ledger.has_reference(sku, reference):
                    raise ValueError("duplicate idempotency reference")
            expected_sequence = len(self._events.read(self._stream))
            # phase 2: commit state, ledger and event together
            self._bins.update(nxt)
            for sku, delta in adjustments.items():
                self._ledger.post(sku, references[sku], delta, batch.reason)
            event = self._events.append(self._stream, expected_sequence, "stocktake-applied", {
                "batch_id": batch.batch_id,
                "reason": batch.reason,
                "counts": {sku: str(q) for sku, q in sorted(counts.items())},
                "adjustments": {sku: str(d) for sku, d in sorted(adjustments.items())},
                "references": dict(sorted(references.items())),
            })
            receipt = StocktakeReceipt(batch.batch_id, adjustments, event.sequence, True)
            self._committed[batch.batch_id] = receipt
            self._payloads[batch.batch_id] = payload
            return receipt
