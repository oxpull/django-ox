#!/usr/bin/env python3
"""Lint external-facing prose against the project's copy standard.

Every file in TARGETS is read by strangers deciding whether to install or buy.
This checks the rules that a reviewer kept having to catch by hand:

  * nothing published may describe the product as weak, slow or broken
  * nothing published may narrate pre-release defects ("the benchmark caught
    two real bugs" class); scoping facts and measurement caveats are fine,
    development autobiography is not
  * no build-assistant attribution or AI-generation notes anywhere public
  * no internal review markers or internal artefact paths
  * one spelling of the package and the company, one number for the test count

Two severities. ERROR blocks: high-confidence patterns that are wrong in every
context we ship. WARN reports without blocking: words that are usually fine and
occasionally fatal, where a human should glance rather than a regex decide.
`--strict` promotes warnings to errors.

Suppress a single line with a trailing comment naming the rule and a reason:

    Retries handle a slow downstream API.
    <!-- copy-lint: allow SELF_DOWNGRADE the API is theirs, not ours -->

Bare suppressions with no reason are rejected: the reason is the point.

Usage:
    python tools/copy_lint.py              # lint the default target set
    python tools/copy_lint.py --strict     # warnings block too
    python tools/copy_lint.py FILE...      # lint specific files
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Everything a stranger can read. Deliberately not the whole repo: test names,
# source comments and internal notes are held to a different standard.
TARGETS = [
    "README.md",
    "CHANGELOG.md",
    "SECURITY.md",
    "CONTRIBUTING.md",
    "docs/*.md",
    "benchmarks/*.md",
]

SUPPRESS = re.compile(r"copy-lint:\s*allow\s+([A-Z_]+)\b[ \t]*(?P<reason>[^\n>]*)")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    pattern: re.Pattern[str]
    why: str
    error: bool = True


def _words(*alternatives: str) -> str:
    return r"\b(?:" + "|".join(alternatives) + r")\b"


RULES: list[Rule] = [
    # --- Development autobiography -------------------------------------------
    # The 2026-08-17 miss was "django-ox's worker was measurably slow at launch
    # shape, the benchmark caught it" in docs/benchmarks.md. These patterns are
    # written to catch that sentence and its relatives, not to guess at intent.
    Rule(
        "BUG_AUTOBIOGRAPHY",
        re.compile(
            r"(?:benchmark|test|suite|review|soak|profiler|audit)\w*\s+"
            r"(?:caught|found|revealed|exposed|surfaced|uncovered)\s+"
            # "...caught it" / "...caught that" count: the object is a defect
            # named earlier in the sentence, which is the shape the 2026-08-17
            # miss actually used.
            r"(?:(?:\w+\s+){0,3}?(?:bug|defect|regression|problem|issue|race)"
            r"|(?:it|that|this|them|both)\b)",
            re.I,
        ),
        "Narrates a pre-release defect. State the current behaviour instead.",
    ),
    Rule(
        "BUG_AUTOBIOGRAPHY",
        re.compile(
            r"\bpre-release\s+(?:\w+\s+){0,2}?(?:review|audit|finding|fix)"
            r"|\badversarial\s+review\b"
            r"|\bfindings from a\b",
            re.I,
        ),
        "Cites an internal pre-release review. Publish the property, not the process.",
    ),
    Rule(
        "BUG_AUTOBIOGRAPHY",
        re.compile(
            r"(?:before|until|after)\s+(?:we\s+)?"
            r"(?:fix|fixed|fixing|patch|patched|discover|discovered)\w*\b",
            re.I,
        ),
        "Before/after-the-fix framing is development history, not documentation.",
    ),
    Rule(
        "BUG_AUTOBIOGRAPHY",
        re.compile(r"\b(?:runs|the ones|cells?)\s+(?:that\s+)?we\s+lost\b", re.I),
        "Describes our own failed runs. Report the released behaviour.",
    ),
    Rule(
        "BUG_AUTOBIOGRAPHY",
        re.compile(r"\b(?:we|it)\s+(?:used to|previously)\s+\w+", re.I),
        "Prior-version confession. The changelog is the place for history.",
    ),
    # --- Self-downgrading claims --------------------------------------------
    Rule(
        "SELF_DOWNGRADE",
        re.compile(
            # The possessive matters: the sentence this rule exists for was
            # "django-ox's worker was measurably slow at launch shape".
            r"(?:django-ox|django_ox|ox|our|the)(?:'s)?\s+(?:\w+(?:'s)?\s+){0,3}?"
            r"(?:is|was|were|are|can be|remains?)\s+(?:\w+\s+){0,2}?"
            + _words(
                "slow",
                "slower",
                "sluggish",
                "weak",
                "weaker",
                "buggy",
                "broken",
                "unreliable",
                "fragile",
                "immature",
                "unfinished",
                "half-baked",
                "naive",
                "inefficient",
            ),
            re.I,
        ),
        "Describes our own product as deficient.",
    ),
    Rule(
        "SELF_DOWNGRADE",
        re.compile(
            _words("lost", "loses", "lose")
            + r"\s+(?:every|all|most|each)\s+(?:cell|row|run|round|case)",
            re.I,
        ),
        "Reports us losing a comparison. Lead with what we win or tie.",
    ),
    Rule(
        "SELF_DOWNGRADE",
        re.compile(
            r"\bnot (?:yet )?(?:good|ready|production[- ]ready|suitable)\b", re.I
        ),
        "Tells the reader not to adopt it.",
    ),
    Rule(
        "SELF_DOWNGRADE",
        re.compile(
            _words(
                "rough edges", "early days", "toy", "hobby project", "weekend project"
            ),
            re.I,
        ),
        "Undersells a product with a paid tier attached.",
    ),
    # --- Build-assistant attribution ----------------------------------------
    # This corpus was swept clean by hand once. A gate keeps it clean.
    Rule(
        "AI_DISCLOSURE",
        re.compile(
            r"(?:"
            + _words(
                "claude", "chatgpt", "gpt-4", "gpt-5", "copilot", "anthropic", "openai"
            )
            + r"|\bAI[- ](?:generated|written|assisted|authored)\b"
            + r"|\bgenerated (?:by|with) (?:an? )?(?:AI|LLM|model|assistant)\b"
            + r"|\b(?:LLM|language model)[- ](?:generated|written)\b"
            + r"|\bco-authored-by:\s*claude"
            + r")",
            re.I,
        ),
        "Build-assistant attribution does not belong on a public surface.",
    ),
    # --- Internal markers and artefacts -------------------------------------
    Rule(
        "INTERNAL_LEAK",
        re.compile(
            r"\[(?:PHILIP|AGENT|AGENT-OK|TODO|FIXME|DRAFT|INTERNAL|REVIEW)\]"
            r"|\.contract-ref\b"
            r"|\bBUILD-NOTES\b"
            r"|\bRELEASE-CHECKLIST\b"
            r"|\binternal-notes\b"
            r"|\bnight-shift\b",
            re.I,
        ),
        "Internal review marker or internal artefact path.",
    ),
    Rule(
        "INTERNAL_LEAK",
        re.compile(r"\b(?:TODO|FIXME|XXX|HACK|WIP)\b(?!\s*\w*\s*command)"),
        "Unfinished-work marker in published prose.",
    ),
    Rule(
        "INTERNAL_LEAK",
        re.compile(
            r"(?:^|\s)(?:TBD|TBC|\?\?\?|<placeholder>|LOREM IPSUM)(?:\s|$)", re.I
        ),
        "Placeholder left in published prose.",
    ),
    # --- Assistant-prose tells ----------------------------------------------
    # The standard is that external text reads like a person wrote it. These are
    # the phrases that most reliably say otherwise.
    Rule(
        "AI_SLOP",
        re.compile(
            _words(
                "delve",
                "delves",
                "seamless",
                "seamlessly",
                "effortless",
                "effortlessly",
                "robust and",
                "cutting-edge",
                "state-of-the-art",
                "game-changer",
                "game-changing",
                "revolutionary",
                "supercharge",
                "unlock the power",
                "harness the power",
                "take it to the next level",
                "best-in-class",
                "world-class",
                "battle-tested",
                "blazing[- ]fast",
                "blazingly fast",
                "lightning[- ]fast",
                "rock[- ]solid",
                "bulletproof",
            ),
            re.I,
        ),
        "Marketing filler. Say the specific thing instead.",
    ),
    Rule(
        "AI_SLOP",
        re.compile(
            r"\bin today's [\w\s-]*(?:world|landscape|environment)\b"
            r"|\bwe(?:'re| are) (?:thrilled|excited|pleased) to (?:announce|share)\b"
            r"|\bit(?:'s| is) (?:important|worth) (?:to note|noting) that\b"
            r"|\bat the end of the day\b"
            r"|\bwhen it comes to\b"
            r"|\bthe world of\b",
            re.I,
        ),
        "Filler opener with no information in it.",
    ),
    Rule(
        "AI_SLOP",
        re.compile(r"\bnot (?:only|just) \w+[^.]{0,60}\bbut also\b", re.I),
        "Not-only-but-also construction reads as generated copy.",
    ),
    Rule(
        "AI_SLOP",
        # The standing house style is no em-dashes in external text. Written as
        # escapes (U+2014 em, U+2013 en) rather than literal characters so this
        # file does not trip the ambiguous-character lint that its own rule set
        # exists to enforce elsewhere.
        re.compile("\u2014|\u2013(?=\\s)|(?<=\\s)\u2013"),
        "Em or en dash. House style uses commas, colons or full stops.",
    ),
    Rule(
        "AI_SLOP",
        re.compile(r"[\U0001F300-\U0001FAFF✅❌✨⚠️]"),
        "Emoji. Not the register this product ships in.",
    ),
    # --- Brand consistency ---------------------------------------------------
    Rule(
        "BRAND",
        re.compile(r"\bDjango[-_ ]OX\b|\bDjangoOx\b|\bDjango Ox\b|\bDJANGO-OX\b(?!`)"),
        "The package is 'django-ox', lowercase, hyphenated.",
    ),
    Rule(
        "BRAND",
        re.compile(r"(?<![\w./`_-])oxpull(?![\w./`-])"),
        "The company is 'Oxpull', capitalised, outside code and URLs.",
    ),
    Rule(
        "BRAND",
        re.compile(r"\bOx[- ]?Pull\b|\bOXPULL\b(?!\.)"),
        "The company is 'Oxpull', one word, one capital.",
    ),
    Rule(
        "BRAND",
        re.compile(r"\bDjango Software Foundation\b(?![^.]{0,80}not affiliated)", re.I),
        "Naming the DSF needs the non-affiliation disclaimer nearby.",
        error=False,
    ),
    # --- Context-dependent, reported not blocked ----------------------------
    Rule(
        "WEAK_WORD",
        re.compile(
            _words(
                "limitation", "limitations", "shortcoming", "drawback", "caveat emptor"
            ),
            re.I,
        ),
        "Prefer a scope statement over a deficiency list.",
        error=False,
    ),
    Rule(
        "WEAK_WORD",
        re.compile(r"\bunfortunately\b|\bsadly\b|\bregrettably\b", re.I),
        "Apologetic register.",
        error=False,
    ),
    Rule(
        "HEDGE",
        re.compile(
            r"\bshould (?:probably|mostly)\b|\bmore or less\b|\bkind of\b", re.I
        ),
        "Hedge. Either it does or it does not; say which.",
        error=False,
    ),
]


def _display(path: Path) -> str:
    """Repo-relative where possible, absolute otherwise.

    Files can be passed explicitly from anywhere, including a scratch directory
    outside the repo, so this must not assume containment.
    """
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


@dataclass
class Finding:
    path: Path
    line_no: int
    rule: Rule
    excerpt: str

    def render(self) -> str:
        level = "error" if self.rule.error else "warning"
        rel = _display(self.path)
        return (
            f"{rel}:{self.line_no}: {level}: [{self.rule.rule_id}] "
            f"{self.rule.why}\n    {self.excerpt.strip()}"
        )


def _strip_code(text: str) -> list[str]:
    """Blank out fenced code blocks and inline code, keeping line numbering.

    Prose rules must not fire on identifiers, shell output or config snippets:
    `django_ox` is correct in code and wrong in a sentence, and a CLI table full
    of flags is not marketing copy.
    """
    lines = text.split("\n")
    in_fence = False
    out: list[str] = []
    for line in lines:
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            out.append("")
            continue
        if in_fence:
            out.append("")
            continue
        # Indented code blocks, and inline spans.
        stripped = line.lstrip()
        if (
            line.startswith("    ")
            and line.strip()
            and not stripped.startswith(("-", "*", "|", ">", "#"))
        ):
            out.append("")
            continue
        out.append(re.sub(r"`[^`]*`", "``", line))
    return out


def _suppressed(raw_line: str, rule_id: str) -> bool:
    for match in SUPPRESS.finditer(raw_line):
        if match.group(1) != rule_id:
            continue
        # The trailing "--" of an HTML comment close is not a reason. Require
        # actual words, or a bare suppression would silently pass.
        if len(re.findall(r"\w+", match.group("reason"))) >= 2:
            return True
        print(
            f"    note: bare '{rule_id}' suppression ignored, it needs a reason",
            file=sys.stderr,
        )
    return False


def lint_file(path: Path) -> list[Finding]:
    text = path.read_text(encoding="utf-8")
    raw = text.split("\n")
    prose = _strip_code(text)
    findings: list[Finding] = []
    for index, line in enumerate(prose):
        if not line.strip():
            continue
        for rule in RULES:
            if not rule.pattern.search(line):
                continue
            if _suppressed(raw[index], rule.rule_id):
                continue
            findings.append(Finding(path, index + 1, rule, raw[index]))
    return findings


def check_test_count(paths: list[Path]) -> list[str]:
    """One number for the suite size, everywhere.

    Stale counts (141, then 151, then 191) have drifted across surfaces before.
    No source of truth is needed to catch it: two different numbers in the same
    corpus means at least one is wrong.
    """
    counts: dict[str, list[str]] = {}
    for path in paths:
        for line_no, line in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
            for match in re.finditer(r"\b(\d{2,4})\s+tests\b", line):
                where = f"{_display(path)}:{line_no}"
                counts.setdefault(match.group(1), []).append(where)
    if len(counts) <= 1:
        return []
    detail = "; ".join(f"{n} at {', '.join(v)}" for n, v in sorted(counts.items()))
    return [f"error: [CONSISTENCY] the test count disagrees across surfaces: {detail}"]


def collect(explicit: list[str]) -> list[Path]:
    if explicit:
        return [Path(p).resolve() for p in explicit]
    paths: list[Path] = []
    for pattern in TARGETS:
        paths.extend(sorted(REPO.glob(pattern)))
    return [p for p in paths if p.is_file()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files", nargs="*", help="files to lint (default: the target set)"
    )
    parser.add_argument(
        "--strict", action="store_true", help="treat warnings as errors"
    )
    args = parser.parse_args()

    paths = collect(args.files)
    if not paths:
        print("copy-lint: no files matched", file=sys.stderr)
        return 1

    findings = [f for path in paths for f in lint_file(path)]
    consistency = check_test_count(paths)

    errors = [f for f in findings if f.rule.error or args.strict]
    warnings = [f for f in findings if not f.rule.error and not args.strict]

    for finding in sorted(findings, key=lambda f: (f.path, f.line_no)):
        print(finding.render())
    for problem in consistency:
        print(problem)

    failed = len(errors) + len(consistency)
    print(
        f"\ncopy-lint: {len(paths)} files, "
        f"{failed} error(s), {len(warnings)} warning(s)"
    )
    if failed:
        print(
            "Fix the errors, or suppress a line with "
            "'<!-- copy-lint: allow RULE_ID reason -->' when the rule is genuinely "
            "wrong about that line."
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
