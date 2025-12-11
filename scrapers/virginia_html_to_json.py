# parsers/virginia_html_to_json.py
# Requires: beautifulsoup4
# pip install beautifulsoup4

import os
import json
import re
from bs4 import BeautifulSoup
from typing import Union, Dict, Any, List, Optional

SUBSECTION_TITLES = [
    "Case/Defendant Information",
    "Charge Information",
    "Hearing Information",
    "Service/Process",
    "Disposition Information"
]

_WHITESPACE_RE = re.compile(r'\s+')


def _clean_text(node) -> str:
    if node is None:
        return ""
    text = node.get_text(separator=" ", strip=True) if hasattr(node, "get_text") else str(node)
    text = text.replace("\xa0", " ").replace("\u00A0", " ")
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def _load_html(html_or_path: Union[str, None]) -> str:
    if html_or_path is None:
        return ""
    if os.path.exists(html_or_path) and os.path.isfile(html_or_path):
        with open(html_or_path, "r", encoding="utf-8") as f:
            return f.read()
    return html_or_path


def _is_label_class(cls: Optional[str]) -> bool:
    if not cls:
        return False
    cls = cls.lower()
    return "label" in cls and "value" not in cls


def _is_value_class(cls: Optional[str]) -> bool:
    if not cls:
        return False
    cls = cls.lower()
    return "labelvalue" in cls or "value" in cls


def _extract_label_value_rows(table) -> Dict[str, str]:
    """
    Extract label->value mapping. ALWAYS include the label key;
    if value is empty, set it to "".
    """
    data: Dict[str, str] = {}

    for tr in table.find_all("tr"):
        # first try class-based pairing within the row
        label_cells = []
        value_cells = []
        for td in tr.find_all(["td", "th"]):
            cls = " ".join(td.get("class", [])) if td.get("class") else ""
            if _is_label_class(cls):
                label_cells.append(td)
            elif _is_value_class(cls):
                value_cells.append(td)

        if label_cells and value_cells and len(label_cells) == len(value_cells):
            for lc, vc in zip(label_cells, value_cells):
                label = _clean_text(lc).rstrip(":")
                value = _clean_text(vc)
                if label:
                    data[label] = value  # include even if empty
            continue

        # fallback: even pairing of cells
        cells = [c for c in tr.find_all(["td", "th"])]
        texts = [_clean_text(c) for c in cells]
        if not texts:
            continue

        if len(texts) >= 2 and len(texts) % 2 == 0:
            for i in range(0, len(texts), 2):
                label = texts[i].rstrip(":").strip()
                value = texts[i+1].strip()
                if label:
                    data[label] = value  # include even if empty
            continue

        # fallback: colon-split inside single cells
        for t in texts:
            if ":" in t:
                parts = t.split(":", 1)
                label = parts[0].strip()
                value = parts[1].strip()
                if label:
                    data[label] = value  # include even if empty

    return data


def _extract_grid_rows(table) -> List[Dict[str, str]]:
    """
    Extract table rows using header row (gridheader/subgridheader) when present.
    - The header row is never treated as a data row.
    - If headers exist, create dicts keyed by those headers for each data row.
    - Include rows even if some cells are empty, but skip rows that are completely empty.
    """
    rows_out: List[Dict[str, str]] = []

    # Prefer explicit header row with class 'gridheader' or 'subgridheader'
    header_tr = table.find("tr", class_=lambda c: c and ("gridheader" in c or "subgridheader" in c))
    headers: List[str] = []

    if header_tr:
        headers = [_clean_text(th) for th in header_tr.find_all(["td", "th"])]
        # data rows are the siblings after header_tr (this avoids treating header as data)
        data_trs = header_tr.find_next_siblings("tr")
    else:
        # fallback: try first row as headers if it looks like headers
        all_trs = table.find_all("tr")
        if not all_trs:
            return rows_out
        first_texts = [_clean_text(c) for c in all_trs[0].find_all(["td", "th"])]
        # treat first row as header only if it appears header-like (multiple distinct values)
        if first_texts and len(set(first_texts)) > 1:
            headers = first_texts
            data_trs = all_trs[1:]
        else:
            # no header row — every tr is a data row
            data_trs = all_trs

    for tr in data_trs:
        cells = [c for c in tr.find_all(["td", "th"])]
        values = [_clean_text(c) for c in cells]

        if headers:
            row: Dict[str, str] = {}
            # ensure we produce all header keys (use "" if missing)
            for i, h in enumerate(headers):
                val = values[i] if i < len(values) else ""
                row[h] = val
        else:
            # generic column names if no headers
            row = {f"col{i+1}": v for i, v in enumerate(values)}

        # include row even if some values empty; skip only if all empty
        if any(v != "" for v in row.values()):
            rows_out.append(row)

    return rows_out


def parse_case_div(html_or_path: Union[str, None]) -> Dict[str, Any]:
    """
    Parse the HTML and return structured JSON with the five sections.
    - Always return the five section keys.
    - For label→value sections, keys are present with empty string values when missing.
    - For grid sections, return list of rows (may be empty).
    """
    html = _load_html(html_or_path)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # Prepare defaults: dict for label sections, list for grid sections
    out: Dict[str, Any] = {}
    for k in SUBSECTION_TITLES:
        out[k] = [] if ("Hearing" in k or "Service" in k) else {}

    # Find header <td class="subheader"> elements (toggle controls)
    header_tds = []
    for td in soup.find_all("td"):
        cls = " ".join(td.get("class", [])) if td.get("class") else ""
        if "subheader" in cls or td.get("id") == "togglecontrol":
            header_tds.append(td)
        else:
            # also include exact text matches
            txt = _clean_text(td)
            for title in SUBSECTION_TITLES:
                if txt.strip() == title:
                    header_tds.append(td)
                    break

    # For each header td map to its content table (the next sibling <tr> typically)
    for td in header_tds:
        header_text = _clean_text(td)
        matched_title = None
        for title in SUBSECTION_TITLES:
            if title.lower() in header_text.lower() or header_text.lower() in title.lower():
                matched_title = title
                break
        if not matched_title:
            # try partial match by first word
            for title in SUBSECTION_TITLES:
                if title.split("/")[0].lower() in header_text.lower():
                    matched_title = title
                    break
        if not matched_title:
            continue

        # find the content table:
        parent_tr = td.find_parent("tr")
        inner_table = None
        if parent_tr:
            next_tr = parent_tr.find_next_sibling("tr")
            if next_tr:
                inner_table = next_tr.find("table")
                if inner_table is None:
                    inner_table = next_tr.find("table", recursive=True)
        if inner_table is None:
            # fallback: first table after the td
            inner_table = td.find_next("table")

        if inner_table is None:
            # ensure key present (already initialized), continue
            continue

        # parse according to type
        if "Hearing" in matched_title or "Service" in matched_title:
            parsed = _extract_grid_rows(inner_table)
        else:
            # for label→value we want to include keys even when value empty
            parsed = _extract_label_value_rows(inner_table)

        # always set parsed content (may be {} or []), don't skip when empty
        out[matched_title] = parsed

    return out


# -------------------------
# Normalization helpers
# -------------------------
def _normalize_label_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    """Trim whitespace of keys inside a dict and prefer non-empty values."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        nk = k.strip() if isinstance(k, str) else k
        # prefer first non-empty occurrence
        if nk in out:
            if (out[nk] == "" or out[nk] is None) and (v not in ("", None)):
                out[nk] = v
        else:
            out[nk] = v
    return out


def _normalize_parsed(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize label keys for label→value sections and ensure grids remain intact.
    """
    out: Dict[str, Any] = {}
    for section, content in parsed.items():
        if isinstance(content, dict):
            out[section] = _normalize_label_keys(content)
        elif isinstance(content, list):
            new_rows = []
            for row in content:
                if isinstance(row, dict):
                    new_rows.append(_normalize_label_keys(row))
                else:
                    new_rows.append(row)
            out[section] = new_rows
        else:
            out[section] = content
    return out


def save_parsed_json(
    case_number: str,
    parsed: Dict[str, Any],
    output_dir: str,
    court_name: str,
    state: str,
    search_fips: int,
    case_first: bool = True,
    flip: Optional[bool] = None
) -> str:
    """
    Save parsed JSON with metadata on top.

    Default behavior: case_first=True -> 'Case/Defendant Information' appears before 'Charge Information'.
    Backwards-compatible: if `flip` is provided, it takes precedence:
      - flip=True  => Charge first (legacy behavior)
      - flip=False => Case first
    """
    import datetime
    import os
    import json

    safe_case = case_number.replace("/", "_").replace(" ", "_")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_case}_parsed_{timestamp}.json"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    # normalize parsed keys (trim whitespace, prefer non-empty values)
    parsed_clean = _normalize_parsed(parsed or {})

    # Determine final ordering: flip takes precedence (legacy), otherwise case_first controls it.
    if flip is not None:
        charge_first = bool(flip)
    else:
        charge_first = not bool(case_first)

    reordered: Dict[str, Any] = {}
    if charge_first:
        # Charge first, then Case
        if "Charge Information" in parsed_clean:
            reordered["Charge Information"] = parsed_clean.pop("Charge Information")
        if "Case/Defendant Information" in parsed_clean:
            reordered["Case/Defendant Information"] = parsed_clean.pop("Case/Defendant Information")
    else:
        # Case first, then Charge
        if "Case/Defendant Information" in parsed_clean:
            reordered["Case/Defendant Information"] = parsed_clean.pop("Case/Defendant Information")
        if "Charge Information" in parsed_clean:
            reordered["Charge Information"] = parsed_clean.pop("Charge Information")

    # append remaining sections in original order
    for k, v in parsed_clean.items():
        reordered[k] = v

    parsed_clean = reordered

    # metadata
    metadata = {
        "state": state,
        "courtName": court_name,
        "searchFipsCode": search_fips,
        "download_date": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }

    # write file (metadata first)
    out: Dict[str, Any] = {}
    out.update(metadata)
    out.update(parsed_clean)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    return filepath