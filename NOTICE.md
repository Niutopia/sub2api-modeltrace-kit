# 许可证说明 / License notice

This repository combines two works under different licenses. Each directory keeps the license of the work it derives from.

## `patches/` — LGPL-3.0

`patches/sub2api-v0.2.8-modeltrace.patch` modifies [sub2api](https://github.com/Wei-Shaw/sub2api) (Copyright © Wei-Shaw and contributors), which is licensed under the GNU Lesser General Public License v3.0. The patch is therefore distributed under the same license; the full text is in `patches/LICENSE`.

## `modeltrace/` — MIT

The detection service is based on [ModelTrace](https://github.com/xqy2006/ModelTrace) (Copyright © 2026 xqy2006, MIT License). `modeltrace/modeltrace/fingerprint.py`, `modeltrace/modeltrace/data/unified_bank.json` and `modeltrace/LICENSE` are copied unmodified from upstream commit `55a2e4a` (hashes in `modeltrace/PROVENANCE.json`). The rest of `modeltrace/` (scheduler, transport, HTTP API, tests) was written for the sub2api integration and is released under the same MIT License.

## `deploy/`

Example configuration files, MIT License.
