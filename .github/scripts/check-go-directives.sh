#!/usr/bin/env bash
#
# Every module's `go` directive must be exactly what its dependencies force,
# and no more.
#
# # Why this is worth a check
#
# `go mod tidy` raises a module's directive to the highest any dependency
# declares, and never lowers it again. So a directive that was once necessary
# stays after the reason goes away, and nothing complains — the module simply
# asks every consumer for a newer toolchain than it needs, for ever.
#
# That is not hypothetical here. Upstream go-cloud declares a *patch-level*
# `go 1.25.8` on the commit these drivers pin, so the four modules that depend
# on it inherit 1.25.8 and cannot do otherwise. The root module has no
# dependencies at all and was declaring 1.25.8 anyway, purely because it had
# been raised once. A consumer on 1.25.0 through 1.25.7 downloads a whole
# toolchain to build code that needs nothing from any of those patches.
#
# The pin matters downstream: astrogo (TuSKan/astrogo#109) inherits it and
# cannot lower its own directive while any dependency declares more. This check
# makes sure gocloud-ext contributes only what upstream genuinely forces, so
# that when go-cloud tags a release carrying driver.DeleteOptions — the API
# that keeps these drivers on a pseudo-version — the whole chain drops on its
# own rather than staying pinned because nobody noticed it could move.
#
# # What it compares
#
# For each module, the maximum `go` directive across its dependency graph. That
# is exactly the floor the toolchain enforces, so declaring anything above it is
# a choice, and declaring it accidentally is the bug this catches.
#
# A module with no dependencies has no forced floor, so it is held to BASELINE:
# the directive go-cloud's own tagged release declares. "The same as upstream"
# is the rule, and for a module that depends on nothing there is nothing else to
# be the same as.

set -euo pipefail

# BASELINE is what a module with no dependencies must declare: go-cloud's own
# tagged v0.46.0. Raise it only when this repository genuinely needs a newer
# language or standard library, never to make a check pass.
BASELINE="1.25.0"

# verMax prints the greater of two dotted versions.
verMax() {
	printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1
}

# The loop runs in a pipeline, so it cannot set a variable the rest of the
# script would see. Its output is captured instead and inspected afterwards,
# which is also what makes the whole report visible rather than only the first
# problem.
report="$(mktemp)"
trap 'rm -f "$report"' EXIT

# The same allmodules iteration every other job here uses.
sed -e '/^#/d' -e '/^$/d' allmodules | awk '{print $1}' | while read -r dir; do
	declared="$(cd "$dir" && awk '/^go /{print $2; exit}' go.mod)"

	# GOWORK=off so the answer is the one a consumer resolving through the
	# proxy gets. Inside the workspace, MVS across sibling members can select
	# versions no consumer would see — which is why go.work's own comment says
	# consumability must be verified from outside the repository.
	required=""
	while read -r ver; do
		[ -n "$ver" ] || continue
		required="$(verMax "${required:-0}" "$ver")"
	done < <(cd "$dir" && GOWORK=off go list -m -f '{{if not .Main}}{{.GoVersion}}{{end}}' all 2>/dev/null)

	want="${required:-$BASELINE}"

	if [ "$declared" = "$want" ]; then
		printf '  ok   %-26s go %s\n' "$dir" "$declared"
		continue
	fi

	if [ -z "$required" ]; then
		printf '  FAIL %-26s declares go %s, but has no dependencies; the baseline is %s\n' \
			"$dir" "$declared" "$BASELINE"
	else
		printf '  FAIL %-26s declares go %s, but its dependencies require only %s\n' \
			"$dir" "$declared" "$want"
	fi

	printf '       fix: (cd %s && go mod edit -go=%s && GOWORK=off go mod tidy)\n' "$dir" "$want"

	# A directive BELOW the requirement is a different bug and go itself will
	# refuse to build, so it is not this check's job to explain it.
done | tee "$report"

if grep -q '^  FAIL' "$report"; then
	echo
	echo "A go directive higher than its dependencies force asks every consumer for a"
	echo "toolchain they do not need. See this script's comment for why that travels."
	exit 1
fi
