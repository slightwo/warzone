#!/usr/bin/env bash
set -euo pipefail

# Regenerate protobuf Go sources from pb/battle.proto.
# Required versions match the headers of the committed generated files.
readonly PROTOC_VERSION="libprotoc 36.1"
readonly PROTOC_GEN_GO_VERSION="protoc-gen-go v1.36.11"
readonly PROTOC_GEN_GO_GRPC_VERSION="protoc-gen-go-grpc 1.5.1"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
src_dir="$(cd "${script_dir}/.." && pwd)"
repo_dir="$(cd "${src_dir}/.." && pwd)"

export PATH="$(go env GOPATH)/bin:${PATH}"

require_version() {
  local command="$1"
  local expected="$2"
  local actual

  if ! command -v "${command}" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "${command}" >&2
    exit 1
  fi
  actual="$("${command}" --version)"
  if [[ "${actual}" != "${expected}" ]]; then
    printf '%s version mismatch: got %q, want %q\n' "${command}" "${actual}" "${expected}" >&2
    exit 1
  fi
}

require_version protoc "${PROTOC_VERSION}"
require_version protoc-gen-go "${PROTOC_GEN_GO_VERSION}"
require_version protoc-gen-go-grpc "${PROTOC_GEN_GO_GRPC_VERSION}"

cd "${src_dir}"
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  pb/battle.proto

if [[ "${1:-}" == "--check" ]]; then
  cd "${repo_dir}"
  git diff --exit-code -- src/pb/battle.pb.go src/pb/battle_grpc.pb.go
fi
