#!/usr/bin/env bash
# Installs a prebuilt forkyard binary from GitHub Releases.
#
#   curl -fsSL https://raw.githubusercontent.com/gabrielfior/forkyard/main/install.sh | bash
#
# Env overrides:
#   FORKYARD_VERSION      release tag to install, e.g. v0.1.0 (default: latest)
#   FORKYARD_INSTALL_DIR  install directory (default: /usr/local/bin if writable, else ~/.local/bin)
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

REPO="gabrielfior/forkyard"
VERSION="${FORKYARD_VERSION:-latest}"

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux) platform="unknown-linux-gnu" ;;
  Darwin) platform="apple-darwin" ;;
  *)
    echo -e "${RED}forkyard: unsupported OS '$os' — no prebuilt binary for this platform yet.${NC}" >&2
    echo "Build from source instead: https://github.com/${REPO}#build-from-source" >&2
    exit 1
    ;;
esac

case "$arch" in
  x86_64 | amd64) cpu="x86_64" ;;
  arm64 | aarch64) cpu="aarch64" ;;
  *)
    echo -e "${RED}forkyard: unsupported architecture '$arch' — no prebuilt binary for this platform yet.${NC}" >&2
    exit 1
    ;;
esac

target="${cpu}-${platform}"
asset="forkyard-${target}.tar.gz"

# Same default as most curl-installed CLIs: /usr/local/bin is on PATH out of
# the box on essentially every macOS/Linux shell, so prefer it when writable
# and only fall back to a PATH-editing step when it genuinely isn't.
if [ -z "${FORKYARD_INSTALL_DIR:-}" ]; then
  if [ -w "/usr/local/bin" ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
  fi
else
  INSTALL_DIR="$FORKYARD_INSTALL_DIR"
fi

if [ "$VERSION" = "latest" ]; then
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${asset}"
fi

echo ""
echo -e "${BOLD}Installing forkyard${NC}"
echo ""
echo -e "  ${BOLD}Platform${NC}:  $(echo "$os" | tr '[:upper:]' '[:lower:]')/${arch}"
echo -e "  ${BOLD}Target${NC}:    ${target}"
echo -e "  ${BOLD}Location${NC}:  ${INSTALL_DIR}/forkyard"
echo ""

if command -v forkyard &>/dev/null; then
  echo -e "${YELLOW}Note: replacing an existing forkyard on your PATH ($(command -v forkyard))${NC}"
  echo ""
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo -e "${BLUE}Downloading...${NC}"
if ! curl -fsSL "$url" -o "$tmp_dir/${asset}"; then
  echo -e "${RED}forkyard: no release build found at ${url}${NC}" >&2
  echo "Supported targets are x86_64 Linux and arm64 macOS (see .github/workflows/release.yml)" >&2
  exit 1
fi

if curl -fsSL "${url}.sha256" -o "$tmp_dir/${asset}.sha256" 2>/dev/null; then
  echo -e "${BLUE}Verifying checksum...${NC}"
  expected="$(awk '{print $1}' "$tmp_dir/${asset}.sha256")"
  if command -v sha256sum &>/dev/null; then
    actual="$(sha256sum "$tmp_dir/${asset}" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$tmp_dir/${asset}" | awk '{print $1}')"
  fi
  if [ "$expected" != "$actual" ]; then
    echo -e "${RED}Checksum verification failed!${NC}" >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    exit 1
  fi
  echo -e "${GREEN}✓${NC} Checksum verified"
fi

tar -xzf "$tmp_dir/${asset}" -C "$tmp_dir"
chmod +x "$tmp_dir/forkyard"

echo -e "${BLUE}Installing...${NC}"
if [ ! -d "$INSTALL_DIR" ]; then
  mkdir -p "$INSTALL_DIR" 2>/dev/null || sudo mkdir -p "$INSTALL_DIR"
fi
if [ -w "$INSTALL_DIR" ]; then
  mv "$tmp_dir/forkyard" "$INSTALL_DIR/forkyard"
else
  echo -e "${YELLOW}${INSTALL_DIR} needs sudo to write to${NC}"
  sudo mv "$tmp_dir/forkyard" "$INSTALL_DIR/forkyard"
fi

echo ""
echo -e "${GREEN}✓ forkyard installed to ${INSTALL_DIR}/forkyard${NC}"
echo ""

if command -v forkyard &>/dev/null; then
  echo "Get started:"
  echo "  RPC_URL=https://your-mainnet-rpc forkyard"
else
  echo -e "${YELLOW}${INSTALL_DIR} isn't on your PATH yet. Add it:${NC}"
  echo ""
  shell_name="$(basename "${SHELL:-sh}")"
  case "$shell_name" in
    bash)
      echo "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.bashrc && source ~/.bashrc"
      ;;
    zsh)
      echo "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.zshrc && source ~/.zshrc"
      ;;
    fish)
      echo "  fish_add_path ${INSTALL_DIR}"
      ;;
    *)
      echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
      ;;
  esac
  echo ""
  echo "Then:"
  echo "  RPC_URL=https://your-mainnet-rpc forkyard"
fi
echo ""
