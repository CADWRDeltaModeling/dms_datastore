import pytest

from dms_datastore.download_dcc import _find_header, _group_words_into_lines


def _word(text, x0, top):
    return {"text": text, "x0": x0, "top": top}


def _header(top=115.0, scale=1.0):
    return [
        _word("DATE", 138 * scale, top),
        _word("TIME", 233 * scale, top),
        _word("ACTION", 318 * scale, top),
        _word("REMARKS", 400 * scale, top),
    ]


def test_find_header_returns_midpoint_breaks():
    words = [_word("DELTA", 286, 67.6)] + _header() + [_word("2026", 120, 145.5)]
    lines = _group_words_into_lines(words)
    index, breaks = _find_header(lines)
    assert index == 1
    assert breaks == pytest.approx((185.5, 275.5, 359.0))


def test_find_header_scales_with_page_layout():
    lines = _group_words_into_lines(_header(scale=0.8))
    _, breaks = _find_header(lines)
    assert breaks == pytest.approx((148.4, 220.4, 287.2))


def test_find_header_missing_raises():
    lines = _group_words_into_lines([_word("DATE", 138, 115), _word("TIME", 233, 115)])
    with pytest.raises(ValueError, match="header"):
        _find_header(lines)
