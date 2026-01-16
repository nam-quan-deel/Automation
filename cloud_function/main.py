#!/usr/bin/env python3
"""Cloud Function: sync Okta group with long-term leave users from Deel."""

from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Dict, Iterator, List, Mapping, Optional, Set, Tuple

import requests
import google.auth
from google.cloud import secretmanager


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

# Deel settings
DEEL_API_BASE_URL = os.getenv("DEEL_API_BASE_URL", "https://api.letsdeel.com/rest/v2").rstrip("/")
DEEL_PAGE_SIZE = int(os.getenv("DEEL_PAGE_SIZE", "100"))
DEEL_START_DATE = os.getenv("DEEL_START_DATE", "")

# Okta settings
OKTA_ORG_URL = os.getenv("OKTA_ORG_URL", "").rstrip("/")
OKTA_GROUP_ID = os.getenv("OKTA_GROUP_ID", "")

# Secret names (in Secret Manager)
DEEL_API_TOKEN_SECRET = os.getenv("DEEL_API_TOKEN_SECRET", "")
OKTA_API_TOKEN_SECRET = os.getenv("OKTA_API_TOKEN_SECRET", "")
OKTA_GROUP_ID_SECRET = os.getenv("OKTA_GROUP_ID_SECRET", "")
SECRET_PROJECT_ID = os.getenv("SECRET_PROJECT_ID", "")

# Long-term rules
LONG_TERM_MIN_DAYS = int(os.getenv("LONG_TERM_MIN_DAYS", "30"))
LONG_TERM_MIN_AMOUNT = float(os.getenv("LONG_TERM_MIN_AMOUNT", "30"))

_secret_cache: Dict[str, str] = {}


def _get_project_id() -> str:
    project_id = SECRET_PROJECT_ID or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
    if project_id:
        return project_id
    try:
        _, project_id = google.auth.default()
        return project_id or ""
    except Exception:
        return ""


def _get_secret_value(secret_name: str) -> Optional[str]:
    if not secret_name:
        return None
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]
    if secret_name.startswith("projects/"):
        name = f"{secret_name}/versions/latest"
    else:
        project_id = _get_project_id()
        if not project_id:
            raise RuntimeError("GOOGLE_CLOUD_PROJECT not set; cannot access Secret Manager.")
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": name})
    value = response.payload.data.decode("utf-8")
    _secret_cache[secret_name] = value
    return value


def _today_iso_start() -> str:
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    start = dt.datetime.combine(now.date(), dt.time.min, tzinfo=dt.timezone.utc)
    return start.isoformat().replace("+00:00", "Z")


def parse_date(value: Any) -> Optional[dt.date]:
    if not value:
        return None
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            return dt.datetime.fromisoformat(cleaned).date()
        except Exception:
            try:
                return dt.date.fromisoformat(cleaned)
            except Exception:
                logging.warning("Could not parse date value: %s", value)
                return None
    return None


def fetch_time_offs(token: str) -> Iterator[Mapping[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    start_date = DEEL_START_DATE or _today_iso_start()
    logging.info("Requesting start_date=%s", start_date)

    url = f"{DEEL_API_BASE_URL}/time_offs"
    next_cursor: Optional[str] = None

    while True:
        if next_cursor:
            params: Dict[str, Any] = {"next": next_cursor}
        else:
            params = {
                "status": "APPROVED",
                "page_size": DEEL_PAGE_SIZE,
                "start_date": start_date,
            }

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        items = body.get("data") or body.get("time_offs") or body.get("items") or body.get("results")
        if items is None and isinstance(body, list):
            items = body
        if not items:
            logging.info("No items returned for url=%s params=%s", resp.url, params)
            break

        logging.info("Fetched %d items (cursor=%s)", len(items), next_cursor or "first")
        for item in items:
            yield item

        has_next = body.get("has_next_page")
        next_cursor = body.get("next") or body.get("cursor")

        if has_next and next_cursor:
            continue

        if len(items) < DEEL_PAGE_SIZE:
            break
        break


def extract_email(entry: Mapping[str, Any]) -> Optional[str]:
    direct_keys = ("user_email", "email", "work_email")
    nested_keys = (
        "worker",
        "user",
        "person",
        "profile",
        "hris_profile",
        "recipient_profile",
        "requester_profile",
    )

    for key in direct_keys:
        email = entry.get(key)
        if email:
            return str(email).lower()

    for key in nested_keys:
        nested = entry.get(key) or {}
        if isinstance(nested, Mapping):
            for nested_key in ("email", "work_email", "personal_email"):
                email = nested.get(nested_key)
                if email:
                    return str(email).lower()
    return None


def get_interval_and_amount(
    entry: Mapping[str, Any], window_start: dt.date, window_end: dt.date
) -> Optional[Tuple[dt.date, dt.date, float]]:
    start = parse_date(entry.get("start_date"))
    end = parse_date(entry.get("end_date")) or start
    if not (start and end and start <= window_end and end >= window_start):
        return None

    amount_raw = entry.get("amount")
    if amount_raw is None:
        return None
    try:
        amount_val = float(amount_raw)
    except (TypeError, ValueError):
        logging.debug("Non-numeric amount encountered: %s", amount_raw)
        return None

    return (start, end, amount_val)


def merge_intervals(intervals: List[Tuple[dt.date, dt.date]]) -> List[Tuple[dt.date, dt.date]]:
    if not intervals:
        return []
    intervals_sorted = sorted(intervals, key=lambda item: item[0])
    merged: List[Tuple[dt.date, dt.date]] = []
    current_start, current_end = intervals_sorted[0]
    for start, end in intervals_sorted[1:]:
        if start <= current_end + dt.timedelta(days=1):
            current_end = max(current_end, end)
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    merged.append((current_start, current_end))
    return merged


def compute_long_term_emails(entries: Iterator[Mapping[str, Any]]) -> Set[str]:
    today = dt.date.today()
    user_intervals: Dict[str, List[Tuple[dt.date, dt.date]]] = {}
    user_amounts: Dict[str, List[Tuple[dt.date, dt.date, float]]] = {}

    for entry in entries:
        email = extract_email(entry)
        if not email:
            continue
        interval_amount = get_interval_and_amount(entry, today, today)
        if not interval_amount:
            continue
        start, end, amount_val = interval_amount
        user_intervals.setdefault(email, []).append((start, end))
        user_amounts.setdefault(email, []).append((start, end, amount_val))

    long_term_emails: Set[str] = set()
    for email, intervals in user_intervals.items():
        merged = merge_intervals(intervals)
        for interval_start, interval_end in merged:
            interval_days = (interval_end - interval_start).days + 1
            if interval_days < LONG_TERM_MIN_DAYS:
                continue
            total_amount = 0.0
            for start, end, amount_val in user_amounts.get(email, []):
                if start <= interval_end and end >= interval_start:
                    total_amount += amount_val
            if total_amount >= LONG_TERM_MIN_AMOUNT:
                long_term_emails.add(email)
                break
    return long_term_emails


def _okta_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"SSWS {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _okta_api_base() -> str:
    return f"{OKTA_ORG_URL.rstrip('/')}/api/v1"


def _okta_list_group_users(token: str) -> Dict[str, str]:
    members: Dict[str, str] = {}
    url = f"{_okta_api_base()}/groups/{OKTA_GROUP_ID}/users"
    params = {"limit": 200}
    while True:
        resp = requests.get(url, headers=_okta_headers(token), params=params, timeout=30)
        resp.raise_for_status()
        users = resp.json()
        for user in users:
            profile = user.get("profile") or {}
            email = profile.get("email") or profile.get("login")
            if email:
                members[str(email).lower()] = user.get("id")

        link = resp.headers.get("link") or resp.headers.get("Link")
        if link and 'rel="next"' in link:
            next_url = link.split("<", 1)[-1].split(">", 1)[0]
            if next_url:
                url = next_url
                params = None
                continue
        break
    return members


def _okta_lookup_user_id(token: str, email: str) -> Optional[str]:
    params = {"search": f'profile.email eq "{email}"'}
    resp = requests.get(f"{_okta_api_base()}/users", headers=_okta_headers(token), params=params, timeout=30)
    resp.raise_for_status()
    users = resp.json()
    if not users:
        return None
    return users[0].get("id")


def sync_okta_group(long_term_emails: Set[str], token: str) -> None:
    current_members = _okta_list_group_users(token)
    current_emails = set(current_members.keys())

    to_add = long_term_emails - current_emails
    to_remove = current_emails - long_term_emails

    for email in sorted(to_add):
        user_id = _okta_lookup_user_id(token, email)
        if not user_id:
            logging.warning("Okta user not found for email: %s", email)
            continue
        resp = requests.put(
            f"{_okta_api_base()}/groups/{OKTA_GROUP_ID}/users/{user_id}",
            headers=_okta_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        logging.info("Added %s to Okta group %s", email, OKTA_GROUP_ID)

    for email in sorted(to_remove):
        user_id = current_members.get(email)
        if not user_id:
            continue
        resp = requests.delete(
            f"{_okta_api_base()}/groups/{OKTA_GROUP_ID}/users/{user_id}",
            headers=_okta_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        logging.info("Removed %s from Okta group %s", email, OKTA_GROUP_ID)


def time_off_tracking(request):  # noqa: ANN001
    """Cloud Function entrypoint."""
    logging.info("Fetching Deel time-off data")
    global OKTA_GROUP_ID
    if not OKTA_ORG_URL:
        return ("OKTA_ORG_URL not set", 500)

    okta_group_id = _get_secret_value(OKTA_GROUP_ID_SECRET) or OKTA_GROUP_ID
    if not okta_group_id:
        return ("OKTA_GROUP_ID not set", 500)
    OKTA_GROUP_ID = okta_group_id

    okta_token = _get_secret_value(OKTA_API_TOKEN_SECRET)
    if not okta_token:
        return ("OKTA_API_TOKEN not set", 500)

    deel_token = _get_secret_value(DEEL_API_TOKEN_SECRET)
    if not deel_token:
        return ("DEEL_API_TOKEN not set", 500)

    long_term_emails = compute_long_term_emails(fetch_time_offs(deel_token))
    if long_term_emails:
        logging.info(
            "Long-term leave users today (%d): %s",
            len(long_term_emails),
            ", ".join(sorted(long_term_emails)),
        )
    else:
        logging.info("Long-term leave users today: none")

    sync_okta_group(long_term_emails, okta_token)
    return {
        "long_term_count": len(long_term_emails),
        "long_term_emails": sorted(long_term_emails),
    }
