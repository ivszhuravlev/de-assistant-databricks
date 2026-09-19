from pathlib import Path

from evals import EVAL_IDS

ROOT = Path(__file__).resolve().parents[1]
COOKBOOK = (
    "expected_count",
    "1369765",
    "tpep_",
    "lpep_",
    "sha2",
    "unionByName",
    "left join dim_zones",
    "Deliverable tables",
)


def test_eval_briefs_are_ideas_not_finished_jobs():
    for name in EVAL_IDS:
        text = (ROOT / "evals" / name / "brief.md").read_text(encoding="utf-8")
        hits = [token for token in COOKBOOK if token in text]
        assert hits == [], f"{name} brief still looks like a finished pipeline: {hits}"


def test_generator_briefs_match_eval_briefs():
    mapping = {
        "taxi": "pipeline-brief.md",
        "transaction_cat": "transaction-cat-brief.md",
        "fresh_retail": "fresh-retail-brief.md",
    }
    for eval_id, spec_name in mapping.items():
        eval_text = (ROOT / "evals" / eval_id / "brief.md").read_text(encoding="utf-8")
        wired = (ROOT / "spec" / spec_name).read_text(encoding="utf-8")
        assert wired == eval_text, spec_name
