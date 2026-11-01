#!/usr/bin/env python3
"""Write a prod .env from GitHub Actions secrets and variables. Run by the Deploy workflow, never on the VM.

The keys are the ones in the example file (e.g. infrastructure/prod/.env.example). Each value comes from the
repository secret of the same name, else the repository variable of the same name, else empty (the compose
default then applies). The values in the example file are only examples and are never copied.

    SECRETS_JSON='${{ toJSON(secrets) }}' VARS_JSON='${{ toJSON(vars) }}' \\
        python3 render_env.py <example file> <output file> [--all-required]

Fails, listing every problem and never printing a value, when:
- a key marked __GENERATE__ in the example (or any key, with --all-required) has no value;
- a sensitive key (password, secret, token, key, accounts, webhook) is a variable instead of a secret;
- a value contains a single quote or a line break (values are written single-quoted, taken literally);
- any secret or variable of the repository is still the placeholder TODO (also PROD_REPO_DIR, PROD_SSH_PORT…, which
  the workflow reads directly): use TODO for "set this later", and the deploy reminds you.
"""

import json
import os
import re
import sys

KEY_RE = re.compile(r"^\s*([A-Z][A-Z0-9_]*)=(.*)$")
SENSITIVE_RE = re.compile(r"PASSWORD|SECRET|TOKEN|KEY|ACCOUNTS|WEBHOOK")
PLACEHOLDER = "TODO"


def example_keys(path: str) -> list[tuple[str, bool]]:
    """(key, required) for every KEY=value line; required when the example value is __GENERATE__."""
    keys = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if m := KEY_RE.match(line):
                keys.append((m.group(1), m.group(2).strip() == "__GENERATE__"))
    return keys


def render(
    keys: list[tuple[str, bool]], secrets: dict, variables: dict, all_required: bool
) -> tuple[list[str], list[str]]:
    lines, problems = [], []
    for key, required in keys:
        if key in variables and SENSITIVE_RE.search(key):
            problems.append(f"{key} is a repository variable: store it as a secret instead (variables show in logs)")
            continue
        value = secrets.get(key) or variables.get(key) or ""
        if not value and (required or all_required):
            problems.append(f"{key} is missing: add it as a repository secret")
            continue
        if "'" in value or "\n" in value or "\r" in value:
            problems.append(f"{key} contains a single quote or a line break, which the .env file can't hold")
            continue
        lines.append(f"{key}='{value}'")
    return lines, problems


def placeholders(secrets: dict, variables: dict) -> list[str]:
    """Every repository secret or variable still set to TODO, whether or not this file uses it."""
    return [
        f"{key} is still {PLACEHOLDER}: set its real value in GitHub, or delete it to use the default"
        for values in (secrets, variables)
        for key, value in sorted(values.items())
        if key.isupper() and str(value).strip().upper() == PLACEHOLDER
    ]


def main(argv: list[str]) -> int:
    args = [a for a in argv if not a.startswith("--")]
    if len(args) != 2:
        print(__doc__)
        return 2
    example, output = args
    secrets = json.loads(os.environ.get("SECRETS_JSON") or "{}")
    variables = json.loads(os.environ.get("VARS_JSON") or "{}")
    keys = example_keys(example)
    lines, problems = render(keys, secrets, variables, "--all-required" in argv)
    problems += placeholders(secrets, variables)
    if problems:
        for p in problems:
            print(f"::error::{p}")
        return 1
    with open(os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as f:
        f.write(
            f"# Written by the Deploy workflow from GitHub secrets and variables ({example}). Don't edit on the VM.\n"
        )
        f.write("\n".join(lines) + "\n")
    filled = sum(1 for line in lines if not line.endswith("=''"))
    print(f"{output}: {len(lines)} keys, {filled} set from GitHub, {len(lines) - filled} empty (compose defaults)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
