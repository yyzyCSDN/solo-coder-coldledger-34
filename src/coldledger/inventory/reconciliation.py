from dataclasses import dataclass,replace
from decimal import Decimal
@dataclass(frozen=True)
class Bin: sku:str;quantity:Decimal;version:int
def reconcile(current,counts,versions):
    if set(counts)!=set(versions):raise ValueError('key mismatch')
    adjustments={}
    for sku,counted in counts.items():
        if counted<0:raise ValueError('negative count')
        state=current[sku]
        if state.version!=versions[sku]:raise RuntimeError('version conflict')
        delta=counted-state.quantity
        adjustments[sku]=delta
        current[sku]=replace(state,quantity=counted,version=state.version+1)
    nxt=dict(current)
    return nxt,adjustments
