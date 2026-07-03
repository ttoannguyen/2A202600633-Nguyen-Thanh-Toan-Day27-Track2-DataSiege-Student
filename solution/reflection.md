# Reflection (≤1 page)

**Which fault types were hardest to catch, and why?**

Two structural lineage faults, `missing_upstream` and `orphan_output`, have no
baseline field to threshold against. The declared payload `inputs`/`outputs`
aren't reliable ground truth — a clean run's *actual* upstream set already
differs from what it declares (clean `dbt:stg_orders` runs consistently touch
two upstreams, `raw.orders` + `raw.customers`, and produce one downstream). So
instead of diffing against the declaration, I learn each job's **majority**
observed upstream-set / downstream-count in `ctx.state` and flag deviation once
≥3 samples have accumulated. Near-zero FPR, and it caught both.

The genuinely hard ones are the **subtle-magnitude** faults the private phase
leans on: feature-skew, distribution/volume, drift, staleness instances sitting
just outside — sometimes *inside* — normal ±3σ variance. By construction a
single static threshold can't separate those from clean traffic without
flooding false alarms, which is exactly what capped my private TPR at ~0.67.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

The key realisation was that a *uniform* threshold shrink is wrong. I scanned
the labelled practice stream field-by-field and found clean traffic sits very
differently relative to each field's cap: `mean_amount` clean tops out at ~1.5σ
(faults at 10σ+ → tighten hard, almost free TPR), but `feature mean-shift` and
`lineage runtime` clean values already ride up near ~0.9× the cap (tightening
them at all is almost pure FPR). So each field gets its own alert line, placed a
small margin above where *clean* actually tops out — which generalises, because
clean's spread is the shared baseline across phases, rather than being fit to
any one run's faults. Cost was never the binding constraint: single-pass
coverage (one metered call per event) runs ~180–300 credits, comfortably inside
budget, so I spent nothing on redundant calls and put all the effort into
per-field placement.

With more time I'd add **multi-signal corroboration** for the subtle faults a
single field can't catch: treat two fields *each* sitting at 1.5σ (individually
sub-threshold) as joint evidence of a `data_batch` fault. That targets the
subtle tail without the FPR cost of globally lowering any one field's line —
the one lever that could lift TPR on private without the false-alarm blowup a
flat aggressive threshold produces. I deliberately did **not** hand-fit
thresholds to the private run's score: that overfits one seed and doesn't
transfer, so all calibration here is from practice + the published baseline.
