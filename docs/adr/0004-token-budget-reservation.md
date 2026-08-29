# ADR 0004: Token budgets via estimate → reserve → settle

Status: accepted

## Context

Classic rate limiters (token bucket, sliding window) assume a request's
cost is known at admission: one request, one token. LLM traffic breaks
the assumption twice — budgets are denominated in tokens/dollars rather
than requests, and a streaming response's true cost is unknown until the
stream finishes, possibly tens of seconds after admission. Naively
charging at completion lets N concurrent streams each pass the check
against the same remaining budget and collectively overrun it; naively
charging a worst-case price at admission rejects traffic the budget could
actually afford.

## Decision

Reservation, the same shape banks use for card holds:

1. **Estimate** the request's cost at admission (prompt size + requested
   max tokens).
2. **Reserve** that estimate against the tenant's budget atomically —
   concurrent requests race for the *reservation*, so the budget can
   never be double-spent. If the estimate doesn't fit: `429` with an
   honest `Retry-After`.
3. **Meter** actual usage as the stream runs.
4. **Settle** when the request ends — refund the overestimate (or charge
   the shortfall), including on client disconnect and provider error:
   every admission path ends in exactly one settle.

## Consequences

- Concurrent greedy clients cannot overrun a budget (demonstrated by
  `examples/budget_demo.py`: four racing streams admit only what fits,
  and settles refill what estimates over-held).
- The cost of safety is temporary over-holding: between admit and settle,
  the estimate — not the truth — occupies the budget, so a burst of
  overestimated requests can 429 traffic the budget would have fit.
  Tighter estimates shrink the window; the refund makes it temporary.
- Admission outcomes are observable (per-tenant admitted/rejected/charged
  in `/healthz`, budgets on the dashboard), because a rate limiter whose
  refusals are invisible reads as an outage.
- The pattern generalizes: this is the second reservation system in this
  portfolio — Exchange holds *money* pending settlement, Scope holds
  *capacity* pending metering. Same invariant (never let concurrent
  optimists spend the same balance), different domain — which is the
  tell that it is a pattern, not a trick.
