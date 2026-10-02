#!/usr/bin/env bash
# Build the client handover (dist/balthazar-client.zip) from client/, bridge/ and
# blt_analytics/. dist/ is generated and gitignored: edit the sources, then rerun.
set -euo pipefail
cd "$(dirname "$0")/.."
out=dist/balthazar-client
rm -rf dist && mkdir -p "$out/bridge" "$out/blt_analytics"
cp client/README.md client/AGENTS.md client/CLAUDE.md client/connect.py client/bridge_url.txt "$out/"
cp bridge/balthazar_remote.py bridge/balthazar.py "$out/bridge/"
cp blt_analytics/{__init__,_blt,_compat,cache,paging,frames}.py "$out/blt_analytics/"
# The same instructions as a skill, where Claude Code, Cursor and Copilot find it on their own.
for d in .claude/skills/balthazar .agents/skills/balthazar .github/skills/balthazar; do
  mkdir -p "$out/$d"
  cp -R skills/balthazar-tunnel skills/balthazar-analytics "$out/$d/.."
  { printf -- '---\nname: balthazar\ndescription: Explore, analyse and plot the live Balthazar space (devices, flows, runs) from this folder. Use for any question about Balthazar data.\n---\n\n'; cat client/AGENTS.md; } > "$out/$d/SKILL.md"
done
find "$out" -name __pycache__ -prune -exec rm -rf {} +
(cd dist && zip -qr balthazar-client.zip balthazar-client -x "*.DS_Store")
echo "built dist/balthazar-client.zip — fill in dist/balthazar-client/bridge_url.txt before sending"
