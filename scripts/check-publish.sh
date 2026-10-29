#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# check-publish.sh: fails when a file of the repository (tracked, or new and
# not ignored) names something that must not be published: an email address
# (but noreply@anthropic.com, git@github.com and the reserved example
# domains), an IPv4 address outside loopback, 0.0.0.0, the private ranges
# and the documentation ranges, a customer or employer name, an internal
# host name (*.internal, *.corp), a cloud project or secret name, or a home
# directory path. Prints file:line: what for each finding (make
# check-publish). A justified line goes in scripts/publish-allow.txt as
# "<file>: <finding> -- <reason>"; a stale entry fails too.
set -euo pipefail
cd "$(dirname "$0")/.."
git ls-files -z --cached --others --exclude-standard | python3 -c '
import hashlib, ipaddress, re, sys

SELF = {"scripts/check-publish.sh", "scripts/publish-allow.txt"}
SKIP = re.compile(r"(^|/)go\.sum$")
ALLOWED_NETS = [ipaddress.ip_network(n) for n in (
    "127.0.0.0/8", "0.0.0.0/32", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
    "192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")]
ALLOWED_MAIL = re.compile(r"^(noreply@anthropic\.com|git@github\.com|[^@]+@([\w.-]+\.)?(example\.(com|org|net)|example|test|invalid|localhost))$", re.I)
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b")
IPV4 = re.compile(r"(?<![\w.])(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?![\w.]*\d)")
WORDS = re.compile(r"/Users/|/home/|\bprojects/[\w-]+|secretmanager|\bgcloud\b", re.I)
# Names that must not be published, as the SHA-256 of each lowercase word,
# so this file names nobody: every [a-z]+ token of a line is compared.
DENY = {"5feeabbf59b2cc17a5ef019ec8738584c1ca770fc783e2a838f685be62d18842", "a1ce7a1b010b368f1d833a408dc8dcffc2d04c5fb34812c2eaa1965afbd2af45"}
TOKEN = re.compile(r"[a-z]+")
GOTEXT = re.compile(r"\"(?:[^\"\\]|\\.)*\"|`[^`]*`|//.*")
HOST = re.compile(r"(?<![\w.$])(?:[a-z0-9-]+\.)+(?:internal|corp)\b(?![(\w])")

allow = {}
try:
    for line in open("scripts/publish-allow.txt"):
        line = line.strip()
        if line and not line.startswith("#"):
            key = line.split(" -- ", 1)[0]
            allow[key] = False
except FileNotFoundError:
    pass

def findings(text, go):
    for m in EMAIL.finditer(text):
        if not ALLOWED_MAIL.match(m.group(0)):
            yield "email " + m.group(0)
    for m in IPV4.finditer(text):
        try:
            ip = ipaddress.ip_address(m.group(1))
        except ValueError:
            continue  # an octet over 255: a version or an OID, not an address
        if not any(ip in n for n in ALLOWED_NETS):
            yield "IPv4 " + m.group(1)
    for m in WORDS.finditer(text):
        yield "name " + m.group(0)
    for w in TOKEN.findall(text.lower()):
        if hashlib.sha256(w.encode()).hexdigest() in DENY:
            yield "name (denylisted word)"
    # In Go, p.internal is a field: host names only in strings and comments.
    parts = GOTEXT.findall(text) if go else [text]
    for part in parts:
        for m in HOST.finditer(part):
            yield "host " + m.group(0)

bad = 0
for path in sys.stdin.read().split("\0"):
    if not path or path in SELF or SKIP.search(path):
        continue
    try:
        data = open(path, "rb").read()
    except (IsADirectoryError, FileNotFoundError):
        continue
    if b"\0" in data:
        continue  # binary
    for n, line in enumerate(data.decode("utf-8", "replace").splitlines(), 1):
        for f in findings(line, path.endswith(".go")):
            key = path + ": " + f
            if key in allow:
                allow[key] = True
                continue
            print(f"{path}:{n}: {f}")
            bad = 1
for key, used in allow.items():
    if not used:
        print(f"scripts/publish-allow.txt: stale entry: {key}")
        bad = 1
if bad:
    print("check-publish: fix the lines above, or justify one in scripts/publish-allow.txt", file=sys.stderr)
sys.exit(bad)
'
echo "check-publish: nothing unpublishable found"
