"""Connect to Balthazar. In any script or notebook in this folder:

    from connect import blt, ba

`blt` is the Balthazar API (same as inside a flow), `ba` gives pandas DataFrames.
The first time, a browser window opens to sign in to Balthazar.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [_HERE, os.path.join(_HERE, "bridge")]

import balthazar_remote  # noqa: E402

with open(os.path.join(_HERE, "bridge_url.txt")) as _f:
    _url = _f.read().strip()
if not _url.startswith("https://"):
    raise SystemExit("Put the bridge address (from 'Open App' in Balthazar) in bridge_url.txt")

balthazar_remote.save_profile(_url, "browser")

import blt_analytics as ba  # noqa: E402
from blt_analytics._blt import get_blt  # noqa: E402

blt = get_blt()
