#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively convert .txt files from windows-1250 to UTF-8, "
            "create backups, and overwrite only changed files."
        )
    )
    parser.add_argument(
        "target_dir",
        nargs="?",
        default=".",
        help="Directory to process recursively (default: current directory).",
    )
    parser.add_argument(
        "--tolerant",
        action="store_true",
        help=(
            "Decode as windows-1250 with replacement for invalid bytes "
            "instead of skipping such files."
        ),
    )
    return parser.parse_args()


def should_skip(path: Path, backup_root: Path) -> bool:
    return backup_root == path or backup_root in path.parents


def convert_file(
    path: Path, backup_root: Path, target_root: Path, tolerant: bool
) -> tuple[bool, int]:
    original_bytes = path.read_bytes()

    # If file is already valid UTF-8, keep it untouched (idempotent behavior).
    try:
        original_bytes.decode("utf-8")
        return False, 0
    except UnicodeDecodeError:
        pass

    try:
        decoded_text = original_bytes.decode("windows-1250")
        replacement_count = 0
    except UnicodeDecodeError as exc:
        if not tolerant:
            print(f"SKIP (decode error): {path} ({exc})")
            return False, 0
        decoded_text = original_bytes.decode("windows-1250", errors="replace")
        replacement_count = decoded_text.count("\uFFFD")

    converted_bytes = decoded_text.encode("utf-8")

    if converted_bytes == original_bytes:
        return False, replacement_count

    relative_path = path.relative_to(target_root)
    backup_path = backup_root / relative_path
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_bytes(original_bytes)

    path.write_bytes(converted_bytes)
    return True, replacement_count


def main() -> int:
    args = parse_args()
    target_root = Path(args.target_dir).resolve()

    if not target_root.exists() or not target_root.is_dir():
        print(f"ERROR: Directory does not exist: {target_root}")
        return 1

    backup_root = target_root / "backup"
    backup_root.mkdir(parents=True, exist_ok=True)

    changed_files: list[Path] = []
    tolerant_replacements: list[tuple[Path, int]] = []

    for path in sorted(target_root.rglob("*.txt")):
        if should_skip(path, backup_root):
            continue
        changed, replacement_count = convert_file(
            path=path,
            backup_root=backup_root,
            target_root=target_root,
            tolerant=args.tolerant,
        )
        if changed:
            changed_files.append(path.relative_to(target_root))
            if replacement_count > 0:
                tolerant_replacements.append(
                    (path.relative_to(target_root), replacement_count)
                )

    print("")
    if changed_files:
        print("Modified files:")
        for rel_path in changed_files:
            print(f"- {rel_path}")
    else:
        print("No files were modified.")

    if args.tolerant and tolerant_replacements:
        print("")
        print("Files decoded in tolerant mode (invalid bytes replaced):")
        for rel_path, replacement_count in tolerant_replacements:
            print(f"- {rel_path}: replaced_chars={replacement_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
