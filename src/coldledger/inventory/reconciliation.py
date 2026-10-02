from dataclasses import dataclass,replace
from decimal import Decimal
@dataclass(frozen=True)
class Bin: sku:str;quantity:Decimal;version:int
def reconcile(current,counts,versions):
    if set(counts)!=set(versions):raise ValueError('key mismatch')
    # phase 1: pre-validate the whole batch, write nothing
    for sku,counted in counts.items():
        if counted<0:raise ValueError('negative count')
        state=current[sku]
        if state.version!=versions[sku]:raise RuntimeError('version conflict')
    # phase 2: apply to a fresh dict; the input is never mutated
    nxt=dict(current)
    adjustments={}
    for sku,counted in counts.items():
        state=current[sku]
        adjustments[sku]=counted-state.quantity
        nxt[sku]=replace(state,quantity=counted,version=state.version+1)
    return nxt,adjustments
