"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.

Strategy
--------
One metered call per event (single-pass coverage fits private's 320 budget
with headroom). Baselines are the published mean +/- 3 sigma on clean traffic.

Detection is a per-FIELD threshold, each placed just above where CLEAN traffic
actually tops out. Flat uniform shrinking is wrong here: some fields keep clean
values far below their cap (a fault stands out at 2 sigma with zero collateral),
while others (feature-shift, lineage runtime) carry clean values that already
ride right up near the cap, so tightening them at all just manufactures false
alarms. So each field gets its own line:

  field                 clean tops out at   -> alert line
  mean_amount           ~1.5 sigma          -> 2.0 sigma       (aggressive, safe)
  row_count             ~2.5 sigma          -> 2.7 sigma
  null_rate             ~0.64 x cap         -> 0.70 x cap
  staleness             ~0.80 x cap         -> 0.85 x cap
  contract freshness    ~0.81 x cap         -> 0.88 x cap  (+ declared-SLA check)
  lineage runtime       ~0.90 x cap         -> 0.96 x cap  (clean crowds the cap)
  feature mean-shift    ~0.90 x cap         -> 0.95 x cap  (clean crowds the cap)
  embedding centroid    ~0.89 x cap         -> 0.94 x cap
  corpus doc-age        ~0.82 x cap         -> 0.88 x cap

Each line sits a small margin above the clean maximum observed on the labelled
practice stream, so it generalizes (clean's spread is the same shared baseline
across phases) rather than being fit to any one run's faults. The margin gives
private's clean tail a little room before it trips a false alarm, while every
line still sits well below where that field's faults land.

This is justified by the score formula `0.5*TPR - 0.3*FPR`: on fields where
clean is well-separated, tightening is almost pure TPR gain; on fields where
clean crowds the cap, tightening is almost pure FPR loss. Per-field lets us
take the first everywhere it's cheap and refuse it everywhere it's not --
which a single global knob (tried: too loose misses subtle faults, too tight
floods false alarms) structurally cannot do.

Deterministic faults (contract schema/type violations, lineage
missing-upstream / orphan-output) need no threshold at all: the toolkit reports
the violation directly, and structural lineage faults are caught by comparing
each run to the per-job MAJORITY upstream-set / downstream-count learned in
ctx.state -- both essentially zero-FPR.
"""
from collections import Counter
from api import Verdict

# data_batch
ROW_COUNT_Z = 2.7
MEAN_AMOUNT_Z = 2.0
NULL_RATE_FRAC = 0.70
STALENESS_FRAC = 0.85
# contract_checkpoint
CONTRACT_FRESH_FRAC = 0.88
# lineage_run
RUNTIME_FRAC = 0.96
MIN_LINEAGE_SAMPLES = 3
# feature / embedding
FEATURE_SHIFT_FRAC = 0.95
EMBED_CENTROID_FRAC = 0.94
CORPUS_AGE_FRAC = 0.88

COST = {
    "batch_profile": 1.0,
    "contract_diff": 1.5,
    "lineage_graph_slice": 1.0,
    "feature_drift": 2.0,
    "embedding_drift": 2.0,
}


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _affordable(ctx, method):
    return ctx.tools.budget_remaining() >= COST[method]


def _sigma_from(value, lo, hi):
    """How many sigma `value` is from the clean mean, given a mean +/- 3sigma
    [lo, hi] baseline band. Returns 0 if the band is degenerate."""
    sigma = (hi - lo) / 6.0
    if sigma <= 0:
        return 0.0
    return abs(value - (lo + hi) / 2.0) / sigma


def check_data_batch(payload, ctx):
    pillar = "checks"
    if not _affordable(ctx, "batch_profile"):
        return Verdict(alert=False, pillar=pillar, reason="budget guard")
    prof = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in prof:
        return Verdict(alert=False, pillar=pillar, reason="tool error")

    b = ctx.baseline
    reasons = []
    if _sigma_from(prof["row_count"], b["row_count_min"], b["row_count_max"]) > ROW_COUNT_Z:
        reasons.append("volume")
    if prof["null_rate"]["customer_id"] > b["null_rate_max"] * NULL_RATE_FRAC:
        reasons.append("null_rate")
    if _sigma_from(prof["mean_amount"], b["mean_amount_min"], b["mean_amount_max"]) > MEAN_AMOUNT_Z:
        reasons.append("distribution")
    if prof["staleness_min"] > b["staleness_min_max"] * STALENESS_FRAC:
        reasons.append("freshness")
    return Verdict(alert=bool(reasons), pillar=pillar, reason=",".join(reasons))


def check_contract_checkpoint(payload, ctx):
    pillar = "contracts"
    if not _affordable(ctx, "contract_diff"):
        return Verdict(alert=False, pillar=pillar, reason="budget guard")
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in diff:
        return Verdict(alert=False, pillar=pillar, reason="tool error")

    # Schema/type violations are computed by the toolkit and are always real.
    reasons = list(diff.get("violations", []))
    delay = diff["freshness_delay_min"]
    # SLA freshness violation: flag if the delay breaches the baseline cap OR
    # the contract's own declared freshness SLA (whichever the payload gives us).
    cap = ctx.baseline["freshness_delay_max_min"] * CONTRACT_FRESH_FRAC
    declared = payload.get("declared_sla", {}).get("freshness_min")
    if declared is not None:
        cap = min(cap, declared)
    if delay > cap:
        reasons.append("freshness_sla")
    return Verdict(alert=bool(reasons), pillar=pillar, reason=",".join(reasons))


def check_lineage_run(payload, ctx):
    pillar = "lineage"
    if not _affordable(ctx, "lineage_graph_slice"):
        return Verdict(alert=False, pillar=pillar, reason="budget guard")
    slc = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in slc:
        return Verdict(alert=False, pillar=pillar, reason="tool error")

    job = payload.get("job", "")
    stats = ctx.state.setdefault("lineage_stats", {}).setdefault(
        job, {"upstream": Counter(), "downstream": Counter(), "n": 0}
    )
    upstream_key = tuple(sorted(slc["actual_upstream"]))
    downstream_key = slc["actual_downstream_count"]

    reasons = []
    if stats["n"] >= MIN_LINEAGE_SAMPLES:
        if upstream_key != stats["upstream"].most_common(1)[0][0]:
            reasons.append("upstream_shape")
        if downstream_key != stats["downstream"].most_common(1)[0][0]:
            reasons.append("downstream_shape")
    if slc["duration_ms"] > ctx.baseline["lineage_duration_ms_max"] * RUNTIME_FRAC:
        reasons.append("runtime")

    stats["upstream"][upstream_key] += 1
    stats["downstream"][downstream_key] += 1
    stats["n"] += 1
    return Verdict(alert=bool(reasons), pillar=pillar, reason=",".join(reasons))


def check_feature_materialization(payload, ctx):
    pillar = "ai_infra"
    if not _affordable(ctx, "feature_drift"):
        return Verdict(alert=False, pillar=pillar, reason="budget guard")
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar=pillar, reason="tool error")

    # mean_shift_sigma is already in sigma units; compare against the cap.
    alert = drift["mean_shift_sigma"] > ctx.baseline["feature_mean_shift_sigma_max"] * FEATURE_SHIFT_FRAC
    return Verdict(alert=alert, pillar=pillar, reason="feature_skew" if alert else "")


def check_embedding_batch(payload, ctx):
    pillar = "ai_infra"
    if not _affordable(ctx, "embedding_drift"):
        return Verdict(alert=False, pillar=pillar, reason="budget guard")
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar=pillar, reason="tool error")

    reasons = []
    if drift["centroid_shift"] > ctx.baseline["embedding_centroid_shift_max"] * EMBED_CENTROID_FRAC:
        reasons.append("embedding_drift")
    if drift["avg_doc_age_days"] > ctx.baseline["corpus_avg_doc_age_days_max"] * CORPUS_AGE_FRAC:
        reasons.append("corpus_staleness")
    return Verdict(alert=bool(reasons), pillar=pillar, reason=",".join(reasons))
