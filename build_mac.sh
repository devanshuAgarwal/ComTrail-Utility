#!/usr/bin/env bash
# ComTrail macOS build script
# Run this on a Mac: bash build_mac.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Checking Python..."
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install it from https://www.python.org/downloads/"
    exit 1
fi
python3 --version

echo "==> Installing dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install paramiko pyinstaller

echo "==> Converting icon to .icns..."
if [ -f "logo1.png" ]; then
    ICONSET_DIR="logo1.iconset"
    mkdir -p "$ICONSET_DIR"
    # Generate all required icon sizes
    for size in 16 32 64 128 256 512; do
        sips -z $size $size logo1.png --out "$ICONSET_DIR/icon_${size}x${size}.png"         >/dev/null 2>&1
        double=$((size * 2))
        sips -z $double $double logo1.png --out "$ICONSET_DIR/icon_${size}x${size}@2x.png" >/dev/null 2>&1
    done
    iconutil -c icns "$ICONSET_DIR" -o logo1.icns
    rm -rf "$ICONSET_DIR"
    echo "    logo1.icns created."
    ICON_ARG="logo1.icns"
else
    echo "    logo1.png not found, skipping icon."
    ICON_ARG=""
fi

echo "==> Building ComTrail.app with PyInstaller..."
python3 -m PyInstaller ComTrail_mac.spec --noconfirm

echo ""
echo "==> Build complete!"
echo "    App bundle: $(pwd)/dist/ComTrail.app"
echo ""
echo "    To run: open dist/ComTrail.app"
echo "    To distribute: zip -r ComTrail_mac.zip dist/ComTrail.app"
