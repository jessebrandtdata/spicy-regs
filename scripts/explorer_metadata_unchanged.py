#!/usr/bin/env python3
"""Exit the explorer metadata refresh early when nothing it reads has changed.

Runs before the project's dependencies are installed: standard library plus boto3.
Reads the live ``explorer-metadata.v1.json`` and every input key it records, from R2
(the same authoritative store the publisher reads), and compares them with the
verified ``main`` revision. Prints one line and, under GitHub Actions, sets
``unchanged=true`` only for a complete exact match. Any change, absence or doubt
(including a crash of this script) leaves the full validate-and-publish path to run.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from spicy_regs.explorer_inputs import KEY, unchanged  # noqa: E402

#: Bounded reads, as the publisher bounds them; a larger object is doubt, not a match.
READ_LIMIT = 64 * 1024 * 1024


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True, help="The CI-verified main commit the full path would publish")
    args = parser.parse_args()

    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3", endpoint_url=os.environ["R2_ENDPOINT"], aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
    )
    bucket = os.environ["R2_BUCKET_NAME"]

    def read(key: str) -> bytes | None:
        try:
            body = client.get_object(Bucket=bucket, Key=key)["Body"]
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        with body:
            raw = body.read(READ_LIMIT + 1)
        if len(raw) > READ_LIMIT:
            raise ValueError(f"{key} exceeds {READ_LIMIT} bytes")
        return raw

    try:
        previous = read(KEY)
    except Exception as error:  # doubt runs the full path
        same, reason = False, f"{KEY} could not be read ({type(error).__name__})"
    else:
        same, reason = unchanged(previous, revision=args.revision, read=read)
    print(f"Explorer metadata {reason}" + ("; skipping the rebuild" if same else "; running the full refresh"))
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"unchanged={'true' if same else 'false'}\n")


if __name__ == "__main__":
    main()
