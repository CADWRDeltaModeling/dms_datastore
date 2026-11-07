# Delta Cross Channel
# Download and parse gate operations from
# https://www.usbr.gov/mp/cvo/vungvari/Ccgates.pdf

import datetime
import os
import re

import click
import pandas as pd
import requests

from . import store_utils as utils


# The PDF is visually a four-column table, but it is not rectangular data:
# year labels are embedded in the table, dates are omitted for subsequent
# operations on the same day, and remarks/action text may wrap onto later
# visual lines.  The page scale of the USBR PDF has changed over time, so column
# breaks are derived per page from the x positions of the DATE/TIME/ACTION/
# REMARKS header row rather than hard-coded.
_HEADER_LABELS = ("DATE", "TIME", "ACTION", "REMARKS")
_LINE_Y_TOLERANCE = 2.0

_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
_TIME_RE = re.compile(r"^(?:\d{3,4}|\d{1,2}:\d{2})$")
_YEAR_RE = re.compile(r"^(?:19|20)\d{2}(?:\s+Continued)?$", re.IGNORECASE)


def _join_words(words):
    return " ".join(word["text"] for word in words).strip()


def _group_words_into_lines(words, tolerance=_LINE_Y_TOLERANCE):
    """Group pdfplumber words into visual lines using their y coordinates."""
    lines = []
    for word in sorted(words, key=lambda item: (item["top"], item["x0"])):
        # Usually only the last line is relevant, but looking back a few lines
        # makes this tolerant of tiny extraction-order differences.
        for line in reversed(lines[-3:]):
            if abs(line["top"] - word["top"]) <= tolerance:
                line["words"].append(word)
                break
        else:
            lines.append({"top": word["top"], "words": [word]})

    for line in lines:
        line["words"].sort(key=lambda item: item["x0"])
    return lines


def _find_header(lines):
    """Locate the DATE/TIME/ACTION/REMARKS header among grouped lines.

    Returns
    -------
    tuple
        ``(index, (date_xmax, time_xmax, action_xmax))`` where the column
        breaks are midpoints between adjacent header labels.

    Raises
    ------
    ValueError
        If no header line is present.
    """
    for index, line in enumerate(lines):
        x0_by_text = {word["text"].upper(): word["x0"] for word in line["words"]}
        if all(label in x0_by_text for label in _HEADER_LABELS):
            xs = [x0_by_text[label] for label in _HEADER_LABELS]
            breaks = tuple((a + b) / 2.0 for a, b in zip(xs[:-1], xs[1:]))
            return index, breaks
    raise ValueError("No DATE/TIME/ACTION/REMARKS header line found")


def _parse_datetime(date_text, time_text):
    """Parse both old HHMM times and newer H:MM/HH:MM times."""
    text = f"{date_text} {time_text}"
    formats = (
        "%m/%d/%y %H%M",
        "%m/%d/%Y %H%M",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M",
    )
    for fmt in formats:
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            pass
    return pd.NaT


def _normalize_action(raw_action):
    """Normalize the small set of action spellings used in the gate log."""
    action = " ".join(str(raw_action).lower().split())

    if "partially" in action:
        return "partially open"
    if re.search(r"\bclosed?\b", action):
        return "closed"
    if re.search(r"\bopen(?:ed)?\b", action):
        return "open"
    if action == "-":
        # One historical row records a planned closure that the operator
        # missed.  Calling it "closed" would invent a state change.
        return "no action"
    return action


def parse_dcc_pdf(pdfname):
    """
    Parse the USBR Delta Cross Channel gate-operation PDF.

    The PDF must be treated as a stateful event log rather than a conventional
    dataframe.  A dated row establishes ``current_date``; subsequent rows that
    contain only TIME/ACTION inherit that date.  Text-only lines are attached
    to the preceding event according to whether they fall in the ACTION or
    REMARKS column.

    Returns
    -------
    pandas.DataFrame
        Columns: datetime, action, comments
    """
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportError(
            "pdfplumber is required for download_dcc. "
            "Install it with: pip install pdfplumber"
        ) from exc

    records = []
    current_date = None

    with pdfplumber.open(pdfname) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            lines = _group_words_into_lines(page.extract_words())
            try:
                header_index, (date_xmax, time_xmax, action_xmax) = _find_header(
                    lines
                )
            except ValueError as exc:
                raise RuntimeError(
                    f"{exc} on PDF page {page_number} of {pdfname}"
                ) from exc

            # Titles and the header row itself precede the data region.
            for line in lines[header_index + 1 :]:
                line_words = line["words"]
                full_line = _join_words(line_words)

                # Ignore year section headers such as "2008" and
                # "2008 Continued".  Dates in the actual records already
                # include a year, so we do not need to synthesize it.
                if _YEAR_RE.match(full_line):
                    continue

                date_words = [w for w in line_words if w["x0"] < date_xmax]
                time_words = [
                    w for w in line_words if date_xmax <= w["x0"] < time_xmax
                ]
                action_words = [
                    w for w in line_words if time_xmax <= w["x0"] < action_xmax
                ]
                remarks_words = [w for w in line_words if w["x0"] >= action_xmax]

                date_text = _join_words(date_words)
                time_text = _join_words(time_words)
                action_text = _join_words(action_words)
                remarks_text = _join_words(remarks_words)

                has_date = bool(_DATE_RE.match(date_text))
                has_time = bool(_TIME_RE.match(time_text))

                if has_date:
                    current_date = date_text

                if has_time:
                    if current_date is None:
                        raise RuntimeError(
                            f"Found a time without a preceding date on PDF page "
                            f"{page_number}: {full_line!r}"
                        )
                    records.append(
                        {
                            "date": current_date,
                            "time": time_text,
                            "raw_action": action_text,
                            "comments": remarks_text,
                            "page": page_number,
                        }
                    )
                    continue

                # A line with neither date nor time is continuation material.
                # Column position tells us whether it continues ACTION or
                # REMARKS.  For example, the 1990 "gate 2 / closed" entry has
                # "closed" on a second visual line in the ACTION column.
                if records and not has_date:
                    if action_text:
                        records[-1]["raw_action"] = (
                            records[-1]["raw_action"] + " " + action_text
                        ).strip()
                    if remarks_text:
                        records[-1]["comments"] = (
                            records[-1]["comments"] + " " + remarks_text
                        ).strip()

    if not records:
        raise RuntimeError(f"No gate-operation records found in {pdfname}")

    df = pd.DataFrame.from_records(records)
    df["datetime"] = [
        _parse_datetime(date_text, time_text)
        for date_text, time_text in zip(df["date"], df["time"])
    ]

    bad_datetime = df["datetime"].isna()
    if bad_datetime.any():
        examples = df.loc[bad_datetime, ["date", "time", "page"]].head(5)
        raise RuntimeError(
            "Could not parse one or more DCC date/time values. Examples:\n"
            + examples.to_string(index=False)
        )

    df["action"] = df["raw_action"].map(_normalize_action)

    # Preserve useful action detail that would otherwise disappear during
    # normalization (notably the historical "gate 2 / closed" entry).
    gate_number = df["raw_action"].str.extract(r"\b(gate\s+\d+)\b", expand=False)
    for idx in df.index[gate_number.notna()]:
        note = gate_number.loc[idx]
        comment = df.at[idx, "comments"].strip()
        if note.lower() not in comment.lower():
            df.at[idx, "comments"] = f"{note}; {comment}" if comment else note

    # A dash in REMARKS means "no remark", not literal comment text.
    df.loc[df["comments"].str.strip() == "-", "comments"] = ""
    df["comments"] = df["comments"].fillna("").str.strip()

    df = df[["datetime", "action", "comments"]]
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


def download_dcc(base_dir):
    """
    Download the Delta Cross Channel gate log from the US Bureau of Reclamation
    and convert it to CSV.
    """
    utils.ensure_dir(base_dir)
    url = "https://www.usbr.gov/mp/cvo/vungvari/Ccgates.pdf"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.SSLError:
        print(
            "SSL certificate verification failed. "
            "Retrying with verification disabled..."
        )
        response = requests.get(url, verify=False, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Failed to download PDF from {url}: {exc}") from exc

    fname = url.split("/")[-1]
    pdfname = os.path.join(base_dir, fname)
    with open(pdfname, "wb") as fh:
        fh.write(response.content)

    df = parse_dcc_pdf(pdfname)
    print(f"Parsed {len(df)} Delta Cross Channel gate-operation records")

    conv_dir = os.path.dirname(pdfname).replace("/raw/", "/converted/")
    utils.ensure_dir(conv_dir)
    output_file = os.path.join(conv_dir, os.path.splitext(fname)[0] + ".csv")
    df.to_csv(output_file, index=False)
    print(f"Wrote {output_file}")


@click.command()
@click.option(
    "--base-dir",
    default="data/raw/dxc_gate",
    help="Base directory for downloading files",
)
def download_dcc_cli(base_dir):
    """CLI for downloading the Delta Cross Channel gate log."""
    download_dcc(base_dir)


if __name__ == "__main__":
    download_dcc_cli()
