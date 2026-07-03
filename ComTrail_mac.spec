# -*- mode: python ; coding: utf-8 -*-
# macOS build spec — produces a .app bundle
# Usage: pyinstaller ComTrail_mac.spec  (or just run build_mac.sh)

import os as _os
_icon = 'logo1.icns' if _os.path.isfile('logo1.icns') else 'logo1.png'

a = Analysis(
    ['comtrail_app.py'],
    pathex=[],
    binaries=[],
    datas=[('logo1.png', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ComTrail',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=True,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=_icon,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='ComTrail',
)

app = BUNDLE(
    coll,
    name='ComTrail.app',
    icon=_icon,
    bundle_identifier='com.cleartrail.comtrail',
    info_plist={
        'NSHighResolutionCapable': True,
        'CFBundleShortVersionString': '3.0',
    },
)
