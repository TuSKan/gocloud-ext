#!/usr/bin/env bash
#
# Every module in this repository declares the same `go` directive, and that
# directive is the highest any of their dependencies force.
#
# # Why one number rather than the minimum each module could get away with
#
# The modules here are released and consumed together. A caller reaching any of
# them reaches the root module too — it holds internal/escape and
# internal/useragent, which every driver imports — so a per-module minimum buys
# a consumer nothing: whichever driver they took already carries the highest
# directive in the set, and the graph is resolved as a whole.
#
# What it costs instead is a repository where seven modules disagree about
# their own toolchain floor, each for a reason nobody wrote down, and where
# raising one is a judgement call rather than a fact. One number is checkable.
#
# # Why the number is what it is
#
# Upstream go-cloud declares a patch-level `go 1.25.8` on the commit these
# drivers pin, and they pin that commit because driver.DeleteOptions — part of
# the driver.Bucket interface both httpblob and sftpblob implement — was added
# after v0.46.0. So 1.25.8 is not a choice made here; it is the floor upstream
# imposes, and this repository's job is to pass it on unchanged rather than to
# add to it.
#
# That is the failure this catches. `go mod tidy` raises a directive to the
# highest any dependency declares and never lowers it again, so a module that
# once needed more keeps asking for more after the reason has gone, silently
# and for ever. The check makes the number a fact about the dependency graph
# rather than a residue of whatever order things were tidied in.
#
# The pin travels: astrogo (TuSKan/astrogo#109) inherits it and cannot lower
# its own directive while any dependency declares more. When go-cloud tags a
# release carrying driver.DeleteOptions, this check is what says the whole set
# can move — and fails until it actually does.

set -euo pipefail

# verMax prints the greater of two dotted versions.
verMax() {
	printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1
}

modules() {
	sed -e '/^#/d' -e '/^$/d' allmodules | awk '{print $1}'
}

# required is the highest directive any EXTERNAL dependency forces, which is
# the floor the whole set has to clear.
#
# Our own modules are excluded, and that exclusion is the whole check. The
# modules here depend on one another — every driver imports the root module's
# internal packages — so counting them would make the floor rise to whatever
# they happen to declare, and a set that had all drifted upward together would
# report itself consistent and correct. Found by mutation: without this filter,
# setting all seven to 1.26.0 passes.
#
# GOWORK=off throughout, deliberately: inside the workspace, MVS across sibling
# members selects versions no consumer would ever see. go.work's own comment
# says consumability has to be verified from outside the repository, and a
# check run inside it would answer a question nobody asked.
readonly SELF="github.com/TuSKan/gocloud-ext"

required=""

while read -r dir; do
	while read -r path ver; do
		[ -n "$ver" ] || continue
		case "$path" in
		"$SELF" | "$SELF"/*) continue ;;
		esac
		required="$(verMax "${required:-0}" "$ver")"
	done < <(cd "$dir" && GOWORK=off go list -m -f '{{if not .Main}}{{.Path}} {{.GoVersion}}{{end}}' all 2>/dev/null)
done < <(modules)

if [ -z "$required" ]; then
	echo "FAIL: no dependency reported a go directive; the module graph did not resolve."
	exit 1
fi

echo "dependencies force go $required"

fail=0

while read -r dir; do
	declared="$(cd "$dir" && awk '/^go /{print $2; exit}' go.mod)"

	if [ "$declared" = "$required" ]; then
		printf '  ok   %-26s go %s\n' "$dir" "$declared"
		continue
	fi

	fail=1

	printf '  FAIL %-26s declares go %s, want %s\n' "$dir" "$declared" "$required"
	printf '       fix: (cd %s && go mod edit -go=%s && GOWORK=off go mod tidy)\n' "$dir" "$required"
done < <(modules)

if [ "$fail" -ne 0 ]; then
	echo
	echo "Every module here declares the same go directive, and it is the one our"
	echo "dependencies force — no higher, so we add nothing to what a consumer needs,"
	echo "and no lower, because the toolchain would refuse to build it."
	echo "See this script's comment for where the number comes from."
	exit 1
fi
