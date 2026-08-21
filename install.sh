#!/usr/bin/env bash
# Installs a prebuilt forkyard binary from GitHub Releases.
#
#   curl -fsSL https://raw.githubusercontent.com/gabrielfior/forkyard/main/install.sh | bash
#
# Env overrides:
#   FORKYARD_VERSION      release tag to install, e.g. v0.1.0 (default: latest)
#   FORKYARD_INSTALL_DIR  install directory (default: $HOME/.forkyard/bin)
set -euo pipefail

REPO="gabrielfior/forkyard"
INSTALL_DIR="${FORKYARD_INSTALL_DIR:-$HOME/.forkyard/bin}"
VERSION="${FORKYARD_VERSION:-latest}"

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux) platform="unknown-linux-gnu" ;;
  Darwin) platform="apple-darwin" ;;
  *)
    echo "forkyard: unsupported OS '$os' — no prebuilt binary for this platform yet." >&2
    echo "forkyard: build from source instead: https://github.com/${REPO}#build-from-source" >&2
    exit 1
    ;;
esac

case "$arch" in
  x86_64 | amd64) cpu="x86_64" ;;
  arm64 | aarch64) cpu="aarch64" ;;
  *)
    echo "forkyard: unsupported architecture '$arch' — no prebuilt binary for this platform yet." >&2
    exit 1
    ;;
esac

target="${cpu}-${platform}"
asset="forkyard-${target}.tar.gz"

if [ "$VERSION" = "latest" ]; then
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${asset}"
fi

echo "forkyard: downloading ${target} build (${VERSION})"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if ! curl -fsSL "$url" -o "$tmp_dir/${asset}"; then
  echo "forkyard: no release build found at ${url}" >&2
  echo "forkyard: supported targets are x86_64/aarch64 on Linux and macOS" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
tar -xzf "$tmp_dir/${asset}" -C "$INSTALL_DIR"
chmod +x "$INSTALL_DIR/forkyard"

echo "forkyard: installed to ${INSTALL_DIR}/forkyard"

case ":${PATH}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    echo ""
    echo "${INSTALL_DIR} is not on your PATH yet. Add it, e.g.:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    ;;
esac
