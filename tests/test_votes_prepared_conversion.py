"""The vote families publish without receipts (ss-vse3), so native conversion refuses them."""


import pytest

from spicy_regs import native_conversion as conversion
from spicy_regs.congress_subjects import INPUT_COLUMNS
from spicy_regs.sources import publication
from tests.test_native_conversion import (
    BASE, bucket as _bucket, convert, publish_old,
)

bucket = _bucket


def rows(dataset):
    values = []
    for number in (7, 2):
        value: dict[str, str | None] = dict.fromkeys(INPUT_COLUMNS[dataset])
        value.update(vote_id=f"119-house-1-{number}", chamber="house")
        if dataset == "member_vote_terms":
            value.update(member_key=f"bioguide:X{number}", bioguide_id=f"X{number}", vote_day="2025-01-23",
                         term_match="half_open", term_index="0", term_start="2025-01-03", term_end="2027-01-03")
        else:
            value.update(congress="119", session="1")
            if dataset == "member_votes":
                value.update(member_key=f"bioguide:X{number}", bioguide_id=f"X{number}",
                             member_name="literal  repeated  name", position="Yea")
            else:
                value.update(tallies_json='{"Yea": 2}', roll_number=str(number))
        values.append(value)
    return values


@pytest.mark.parametrize("family,datasets", [
    ("roll-call-votes", ("roll_call_votes", "member_votes")),
    ("member-vote-terms", ("member_vote_terms",)),
])
def test_a_retired_vote_family_never_converts_into_receipts(tmp_path, monkeypatch, bucket, family, datasets):
    """The votes trio publishes no receipts (ss-vse3): native conversion, which writes them, refuses before any write.

    The conversion these families went through in October is kept in their history; their rollups are plain now.
    """
    old = publish_old(bucket, monkeypatch, tmp_path, family, {dataset: rows(dataset) for dataset in datasets})
    written = list(bucket.writes)
    with pytest.raises(conversion.ConversionRefused, match=f"{family} has no subject/receipt rollup") as refusal:
        convert(family, tmp_path / "prepared", publish=True)
    assert refusal.value.state == conversion.NOTHING_PUBLISHED
    assert bucket.writes == written
    assert publication.current_index(BASE)["families"][family] == old
