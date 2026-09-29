"""Plan membership parsing and validation for check requests."""

import re

from digital_land.phase.normalise import NormalisePhase


PLAN_DATASETS = ("local-plan", "supplementary-plan", "minerals-plan", "waste-plan")
PLAN_CHECK_DATASETS = ("plan", *PLAN_DATASETS)


def split_dataset_values(value):
    return [part.strip() for part in re.split("[;:,]", value or "") if part.strip()]


def set_plan_issue_severity(issue_log):
    """Promote plan membership failures after standard severity enrichment."""
    for issue in issue_log.rows:
        if (
            issue.get("field") in ("dataset", "datasets")
            and issue.get("issue-type") == "invalid category value"
        ):
            issue["severity"] = "critical"


def validate_plan_datasets(facts, rows, issue_log):
    """Add invalid plan memberships to the issue log before severity enrichment."""
    invalid_facts = []
    for fact in facts:
        if fact.get("field") in ("dataset", "datasets") and fact.get("value"):
            values = split_dataset_values(fact["value"])
            if not values or any(value not in PLAN_DATASETS for value in values):
                invalid_facts.append(fact)
    if invalid_facts:
        normalise = NormalisePhase()
        source_lines = [
            str(index + 2)
            for index, row in enumerate(rows)
            if any(
                normalise.strip_nulls(
                    normalise.normalise_whitespace(
                        [value or "" for value in row.values()]
                    )
                )
            )
        ]
        for fact in invalid_facts:
            issue_log.log_issue(
                fieldname=fact["field"],
                issue_type="invalid category value",
                value=fact["value"],
                entry_number=fact["entry-number"],
                line_number=fact.get("line-number")
                or source_lines[int(fact["entry-number"]) - 1],
                message="Use "
                + ", ".join(PLAN_DATASETS)
                + "; separate multiple values with semicolons.",
            )
