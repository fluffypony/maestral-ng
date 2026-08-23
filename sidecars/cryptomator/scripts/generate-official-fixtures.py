#!/usr/bin/env python3
"""Generate permanent format-8 fixtures with the pinned official CryptoFS build."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any


PASSWORD = "maestral-official-fixture"
FIXED_ZIP_TIME = (2026, 8, 23, 0, 0, 0)
LONG_NAME = "long-" + "x" * 185 + ".txt"


class Sidecar:
    def __init__(self, java: Path, jar: Path, exchange: Path) -> None:
        self._request_id = 0
        self._process = subprocess.Popen(
            [str(java), "-jar", str(jar), "--exchange-root", str(exchange)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )

    def call(self, method: str, **params: Any) -> Any:
        assert self._process.stdin is not None
        assert self._process.stdout is not None
        self._request_id += 1
        request = {"id": self._request_id, "method": method, "params": params}
        self._process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
        self._process.stdin.flush()
        response_line = self._process.stdout.readline()
        if not response_line:
            assert self._process.stderr is not None
            raise RuntimeError(self._process.stderr.read())
        response = json.loads(response_line)
        if "error" in response:
            raise RuntimeError(response)
        return response["result"]

    def close(self) -> None:
        self.call("shutdown")
        return_code = self._process.wait(timeout=15)
        if return_code != 0:
            assert self._process.stderr is not None
            raise RuntimeError(self._process.stderr.read())


def encode_secret(value: str) -> str:
    return base64.b64encode(value.encode()).decode("ascii")


def deterministic_bytes(length: int, seed: int) -> bytes:
    return bytes((index * 31 + seed) % 256 for index in range(length))


def write_inline(sidecar: Sidecar, path: str, content: bytes) -> None:
    sidecar.call(
        "write_inline",
        path=path,
        content=base64.b64encode(content).decode("ascii"),
    )


def zip_tree(source: Path, destination: Path) -> None:
    with zipfile.ZipFile(
        destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
            relative = path.relative_to(source).as_posix()
            if path.is_dir():
                info = zipfile.ZipInfo(relative + "/", FIXED_ZIP_TIME)
                info.external_attr = (0o40700 << 16) | 0x10
                archive.writestr(info, b"")
            else:
                info = zipfile.ZipInfo(relative, FIXED_ZIP_TIME)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100600 << 16
                archive.writestr(info, path.read_bytes())


def clean_snapshot(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in entry.items() if key != "modified_ms"}
        for entry in entries
    ]


def generate(java: Path, jar: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    targets = [
        output / "official-cryptofs-2.10.0-v8-valid.zip",
        output / "official-cryptofs-2.10.0-v8-corrupt.zip",
        output / "official-cryptofs-2.10.0-v8-manifest.json",
    ]
    existing = [path for path in targets if path.exists()]
    if existing:
        names = ", ".join(path.name for path in existing)
        raise FileExistsError(f"Fixture output already exists: {names}")

    with tempfile.TemporaryDirectory(prefix="maestral-cryptomator-fixture.") as raw_root:
        root = Path(raw_root)
        exchange = root / "exchange"
        exchange.mkdir(mode=0o700)
        vault = root / "valid-vault"
        sidecar = Sidecar(java, jar, exchange)
        sidecar.call(
            "initialize",
            vault_path=str(vault),
            secret=encode_secret(PASSWORD),
        )

        for directory in (
            "/names",
            "/content",
            "/directories",
            "/directories/nested",
            "/moves",
            "/links",
            "/long-names",
            "/corruption",
        ):
            sidecar.call("mkdir", path=directory)

        write_inline(sidecar, "/names/Grüße 東京.txt", "Unicode ✓\n".encode())
        write_inline(sidecar, "/names/e\u0301.txt", "NFD name\n".encode())
        write_inline(sidecar, "/content/binary.bin", deterministic_bytes(16_385, 7))
        write_inline(sidecar, "/directories/nested/child.txt", b"nested directory\n")
        write_inline(sidecar, "/moves/source.txt", b"moved without re-encryption\n")
        sidecar.call("move", source="/moves/source.txt", target="/moves/final.txt")
        sidecar.call("symlink", path="/links/relative", target="../moves/final.txt")
        write_inline(sidecar, f"/long-names/{LONG_NAME}", b"shortened ciphertext name\n")
        write_inline(
            sidecar,
            "/corruption/authenticated.bin",
            deterministic_bytes(128 * 1024, 19),
        )

        snapshot = clean_snapshot(sidecar.call("snapshot", include_hash=True))
        info = sidecar.call("vault_info")
        sidecar.call("close")
        sidecar.close()

        ciphertext_files = [
            path
            for path in vault.joinpath("d").rglob("*")
            if path.is_file() and path.stat().st_size > 128 * 1024
        ]
        if len(ciphertext_files) != 1:
            raise RuntimeError(
                f"Expected one large ciphertext file, found {len(ciphertext_files)}"
            )
        ciphertext = ciphertext_files[0]
        ciphertext_relative = ciphertext.relative_to(vault).as_posix()

        zip_tree(vault, targets[0])
        corrupt_vault = root / "corrupt-vault"
        shutil.copytree(vault, corrupt_vault)
        corrupt_file = corrupt_vault / ciphertext_relative
        with corrupt_file.open("r+b") as stream:
            stream.seek(-1, os.SEEK_END)
            original = stream.read(1)
            stream.seek(-1, os.SEEK_END)
            stream.write(bytes([original[0] ^ 1]))
        zip_tree(corrupt_vault, targets[1])

        manifest = {
            "fixture_version": 1,
            "generated_by": "Official org.cryptomator:cryptofs:2.10.0",
            "cryptolib_version": "2.2.2",
            "official_desktop_verification": {
                "version": "1.19.3",
                "cryptofs_jar_sha256": (
                    "bca4f7bf7dd8e16f4c21ea96f15d4d2205ce35240c5f6ff09d28418470d4c82f"
                ),
                "cryptolib_jar_sha256": (
                    "8deac22a954c2058d5ad325168b73c1b4d245eeba178e70b2b78a3947f37188f"
                ),
                "result": "Both bundled jars are byte-identical to the Maven build inputs.",
            },
            "official_cli_verification": {
                "version": "0.6.2",
                "cryptofs_version": "2.8.0",
                "result": (
                    "Opened the vault and read names, content, directories, the moved "
                    "file, and the shortened long name."
                ),
                "webdav_link_note": (
                    "The old WebDAV adapter omits symbolic links; CryptoFS 2.10.0 "
                    "readLink tests cover the link fixture."
                ),
            },
            "vault_format": info["vault_format"],
            "shortening_threshold": info["shortening_threshold"],
            "key_id": info["key_id"],
            "password": PASSWORD,
            "logical_snapshot": snapshot,
            "long_name": LONG_NAME,
            "moved_from": "/moves/source.txt",
            "moved_to": "/moves/final.txt",
            "link_path": "/links/relative",
            "link_target": "../moves/final.txt",
            "corrupt_logical_path": "/corruption/authenticated.bin",
            "corrupt_ciphertext_path": ciphertext_relative,
            "valid_zip_sha256": hashlib.sha256(targets[0].read_bytes()).hexdigest(),
        }
        targets[2].write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.java.resolve(), args.jar.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
