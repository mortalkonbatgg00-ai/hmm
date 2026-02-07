import json
import os
import threading
import time
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

import requests
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

WASENDER_BASE_URL = os.environ.get("WASENDER_BASE_URL", "https://www.wasenderapi.com")
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY")
DEBUG_WEBHOOK = os.environ.get("DEBUG_WEBHOOK") == "1"
BASE_DIR = Path(__file__).resolve().parent
WEBHOOK_DUMP_DIR = BASE_DIR / "webhook_dumps"
WHATSAPP_RATE_LIMIT_SECONDS = 60
_last_whatsapp_sent_at = 0.0
_whatsapp_rate_limited_until = 0.0

OWNER_NOTIFICATION_NUMBER = "+9647779304531"
NOTIFICATION_INTERVAL_SECONDS = 10  # TEST MODE: notification check every 1 minute
NOTIFICATION_LOOKAHEAD_DAYS = 30
STATUS_LABELS = {"overdue": "متأخر", "expiring": "قارب على الانتهاء"}
SUPPORTED_MESSAGE_EVENTS = {
    "messages.upsert",
    "messages.received",
    "messages.personal.received",
}


def _ensure_webhook_dump_dir():
    try:
        WEBHOOK_DUMP_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as exc:
        print(f"[WebhookDebug] Failed to prepare dump directory: {exc}")
        return False


def _persist_webhook_payload(request_id, event_name, payload):
    if not DEBUG_WEBHOOK:
        return
    if not _ensure_webhook_dump_dir():
        return
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    safe_event = (event_name or "unknown").replace("/", "-").replace("\\", "-")
    filename = f"{timestamp}_{request_id}_{safe_event}.json"
    file_path = WEBHOOK_DUMP_DIR / filename
    summary_path = WEBHOOK_DUMP_DIR / "last.json"
    try:
        with file_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[WebhookDebug] Failed to write payload dump {file_path}: {exc}")
    summary_payload = {
        "request_id": request_id,
        "event": event_name,
        "timestamp": timestamp,
        "file": filename,
    }
    try:
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary_payload, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[WebhookDebug] Failed to write summary dump {summary_path}: {exc}")


def _evaluate_candidate_value(value, path):
    text = str(value).strip()
    if not text:
        return None
    lower = text.lower()
    if "@lid" in lower:
        return None
    has_domain = "@s.whatsapp.net" in lower or "@c.us" in lower
    digits = "".join(char for char in text if char.isdigit())
    starts_plus_digits = text.startswith("+") and digits
    starts_double_zero = text.startswith("00") and digits
    has_length_window = 10 <= len(digits) <= 15
    if not (has_domain or starts_plus_digits or starts_double_zero or has_length_window):
        return None
    iraq_hint = False
    iraq_prefixes = ("+964", "964", "0")
    for prefix in iraq_prefixes:
        if text.startswith(prefix):
            iraq_hint = True
            break
    if digits.startswith("964"):
        iraq_hint = True
    score = (
        1 if has_domain else 0,
        1 if has_length_window else 0,
        1 if iraq_hint else 0,
        len(digits),
    )
    return {
        "path": path,
        "value": text,
        "digits": digits,
        "score": score,
        "has_domain": has_domain,
    }


def extract_sender_candidates(payload):
    candidates = []

    def _walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                _walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for index, value in enumerate(node):
                _walk(value, f"{path}[{index}]")
        elif isinstance(node, (str, int, float)):
            candidate = _evaluate_candidate_value(node, path)
            if candidate:
                candidates.append(candidate)

    _walk(payload, "$")
    return candidates


def _select_best_candidate(candidates):
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item["score"][0],
            item["score"][1],
            item["score"][2],
            item["score"][3],
        ),
    )


def _normalize_event_key(event_name):
    if not event_name:
        return ""
    return (
        str(event_name)
        .strip()
        .lower()
        .replace("_", ".")
        .replace("-", ".")
    )


def fetch_rentals_from_supabase():
    now = datetime.now(timezone.utc)
    upcoming_limit = now + timedelta(days=NOTIFICATION_LOOKAHEAD_DAYS)
    print(
        "[RentalNotifier] Fetching rentals. "
        f"Now={now.isoformat()} UpcomingLimit={upcoming_limit.isoformat()}"
    )
    try:
        unpaid_values = [False, 0, "0", "false", "False"]
        overdue_resp = (
            supabase.table("rentals")
            .select("*")
            .lt("end_datetime", now.isoformat())
            .in_("paid", unpaid_values)
            .execute()
        )
        expiring_resp = (
            supabase.table("rentals")
            .select("*")
            .gte("end_datetime", now.isoformat())
            .lte("end_datetime", upcoming_limit.isoformat())
            .in_("paid", unpaid_values)
            .execute()
        )
    except Exception as exc:
        print(f"[RentalNotifier] Error fetching rentals from Supabase: {exc}")
        return {"overdue": [], "expiring": []}

    overdue = overdue_resp.data or []
    expiring = expiring_resp.data or []

    print(
        "[RentalNotifier] Rentals fetched "
        f"(overdue={len(overdue)} expiring={len(expiring)})"
    )
    return {"overdue": overdue, "expiring": expiring}


def notification_already_sent(rental_id, notification_type, reference_time=None):
    if rental_id is None:
        return True
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)
    day_start = reference_time.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    day_end = day_start + timedelta(days=1)

    try:
        response = (
            supabase.table("notifications_log")
            .select("sent_at")
            .eq("rental_id", rental_id)
            .eq("notification_type", notification_type)
            .gte("sent_at", day_start.isoformat())
            .lt("sent_at", day_end.isoformat())
            .execute()
        )
    except Exception as exc:
        print(
            "[RentalNotifier] Failed to check notifications_log "
            f"for rental {rental_id}: {exc}"
        )
        return False

    exists = bool(response.data)
    if exists:
        print(
            f"[RentalNotifier] Notification already sent today "
            f"for rental {rental_id} ({notification_type})"
        )
    return exists


def log_notification(rental_id, notification_type):
    try:
        supabase.table("notifications_log").insert(
            {
                "rental_id": rental_id,
                "notification_type": notification_type,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }
        ).execute()
        print(
            f"[RentalNotifier] Logged notification for rental {rental_id} "
            f"({notification_type})"
        )
    except Exception as exc:
        print(
            "[RentalNotifier] Failed to log notification "
            f"for rental {rental_id}: {exc}"
        )


def _parse_datetime(value):
    if not value:
        return None
    try:
        # Normalize string
        if isinstance(value, str):
            value = value.replace("Z", "+00:00")

        dt = datetime.fromisoformat(value)

        # If datetime is naive (no timezone), assume local time
        if dt.tzinfo is None:
            local_dt = dt.astimezone()        # attach local timezone
            return local_dt.astimezone(timezone.utc)

        # If already timezone-aware, convert to UTC
        return dt.astimezone(timezone.utc)

    except Exception as exc:
        print(f"[RentalNotifier] Failed to parse datetime: {value} ({exc})")
        return None


def _format_client_identity(rental):
    return (
        rental.get("client_name")
        or rental.get("client_phone")
        or (f"زبون رقم {rental.get('client_id')}" if rental.get("client_id") else "زبون")
    )


def _build_message(rental, status_label, end_dt):
    client_identity = _format_client_identity(rental)
    property_name = rental.get("property_name") or "عقار"
    formatted_end = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_text = STATUS_LABELS.get(status_label.lower(), status_label)
    return (
        f"تنبيه عقد إيجار: {status_text}\n"
        f"الزبون: {client_identity}\n"
        f"العقار: {property_name}\n"
        f"تاريخ الانتهاء: {formatted_end}"
    )


def _normalize_iraqi_number(phone):
    if not phone:
        return None
    digits = "".join(char for char in str(phone) if char.isdigit())
    if not digits:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = "964" + digits[1:]
    if not digits.startswith("964"):
        digits = "964" + digits
    return f"+{digits}"


def _extract_sender_details(payload):
    def _first_dict(value):
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    return item
        return None

    search_spaces = []
    if isinstance(payload, dict):
        search_spaces.append(payload)
        for key in ("data", "message", "payload"):
            candidate = _first_dict(payload.get(key))
            if candidate:
                search_spaces.append(candidate)
        data_section = _first_dict(payload.get("data"))
        if data_section:
            for key in ("message", "payload"):
                candidate = _first_dict(data_section.get(key))
                if candidate:
                    search_spaces.append(candidate)
        for key in ("messages", "entries", "contacts"):
            candidate = _first_dict(payload.get(key))
            if candidate:
                search_spaces.append(candidate)

    phone = None
    name = None
    phone_keys = ("from", "sender", "phone", "client_phone", "number")
    name_keys = ("senderName", "name", "client_name", "contact_name")

    for obj in search_spaces:
        for key in phone_keys:
            value = obj.get(key)
            if value:
                phone = value
                break
        if phone:
            break

    for obj in search_spaces:
        for key in name_keys:
            value = obj.get(key)
            if value:
                name = value
                break
        if name:
            break

    return phone, name


def _coerce_message_dict(data_section):
    if isinstance(data_section, dict):
        options = [data_section.get("messages"), data_section.get("message")]
        for option in options:
            if isinstance(option, dict):
                return option
            if isinstance(option, list):
                for item in option:
                    if isinstance(item, dict):
                        return item
    if isinstance(data_section, list):
        for item in data_section:
            if isinstance(item, dict):
                return item
    return None


def _extract_from_me_flag(message):
    if not isinstance(message, dict):
        return False
    key_block = message.get("key")
    if isinstance(key_block, dict):
        flag = key_block.get("fromMe")
        if flag is not None:
            return bool(flag)
    flag = message.get("fromMe")
    return bool(flag) if flag is not None else False


def _generate_request_id():
    return uuid.uuid4().hex[:8]


def normalize_iraqi_phone_from_jid(value, *, debug=False, log_prefix=""):
    """
    Normalize any incoming WhatsApp identifier to +9647XXXXXXXXX (13 digits).
    Strips WhatsApp-specific suffixes like @s.whatsapp.net or @lid, keeps digits only,
    upgrades local Iraqi numbers beginning with 07 to international format,
    and rejects any non-Iraqi mobile numbers.
    """
    if value is None:
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Rejected value=None (empty value)")
        return None, "", "empty value"
    text = str(value).strip()
    lower_text = text.lower()
    if "@lid" in lower_text:
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Rejected {text} due to @lid")
        return None, "", "lid_rejected"
    if "@" in text:
        text = text.split("@", 1)[0]
    digits = "".join(char for char in text if char.isdigit())
    if not digits:
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Rejected {value} (no digits)")
        return None, "", "no digits found"

    if digits.startswith("00"):
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Stripping 00 prefix from {digits}")
        digits = digits[2:]
        if not digits:
            if debug:
                print(f"{log_prefix} [PhoneNormalize] Rejected {value} (no digits after country code)")
            return None, "", "no digits found"
    if digits.startswith("0") and 10 <= len(digits) <= 11:
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Upgrading local number {digits} to Iraq code")
        digits = "964" + digits[1:]
    normalized = None
    reason = None
    if digits.startswith("964"):
        normalized = f"+{digits}"
    elif 10 <= len(digits) <= 15:
        normalized = f"+{digits}"
    else:
        reason = "length_out_of_range"

    if normalized and not normalized.startswith("+"):
        normalized = f"+{normalized}"

    if normalized:
        if debug:
            print(f"{log_prefix} [PhoneNormalize] Normalized {value} -> {normalized}")
        return normalized, digits, None

    if debug:
        print(f"{log_prefix} [PhoneNormalize] Rejected {value} ({reason or 'not iraqi mobile'})")
    return None, digits, reason or "not iraqi mobile"


def _resolve_sender_name(payload, data_section, message):
    name_sources = [
        ("data.messages[0].pushName", message.get("pushName")),
        ("data.pushName", data_section.get("pushName")),
        ("payload.pushName", payload.get("pushName")),
    ]
    for path, value in name_sources:
        if value:
            return value, path
    return "Unknown", "default"


def _send_whatsapp_message(message_body):
    # Previously blocked during webhook context via global flag; removed
    # to allow background processing. Ensure callers handle rate/permission.
    if not WASENDER_API_KEY:
        print("[RentalNotifier] WasenderAPI key missing; cannot send message")
        return False
    now = time.time()
    if _whatsapp_rate_limited_until and now < _whatsapp_rate_limited_until:
        wait_seconds = int(_whatsapp_rate_limited_until - now)
        print(
            "[RentalNotifier] WhatsApp rate limit active; "
            f"skipping message for {wait_seconds}s"
        )
        return False
    if _last_whatsapp_sent_at and (now - _last_whatsapp_sent_at) < WHATSAPP_RATE_LIMIT_SECONDS:
        remaining = int(WHATSAPP_RATE_LIMIT_SECONDS - (now - _last_whatsapp_sent_at))
        print(
            "[RentalNotifier] WhatsApp rate limit reached; "
            f"skipping message for {remaining}s"
        )
        return False

    to_number = OWNER_NOTIFICATION_NUMBER
    normalized_owner = _normalize_iraqi_number(to_number)
    if not normalized_owner:
        print("[RentalNotifier] Invalid WhatsApp number; cannot send message")
        return False
    if normalized_owner != OWNER_NOTIFICATION_NUMBER:
        print("[SECURITY] BLOCKED: attempt to send notification to non-owner number")
        return False
    print("[Notifier] Sending rental notification to OWNER +9647701658645")

    payload = {
        "to": normalized_owner,
        "text": message_body,
    }

    try:
        response = requests.post(
            f"{WASENDER_BASE_URL.rstrip('/')}/api/send-message",
            json=payload,
            headers={
                "Authorization": f"Bearer {WASENDER_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except requests.RequestException as exc:
        print(f"[RentalNotifier] WasenderAPI request failed: {exc}")
        return False

    if response.ok:
        print("[RentalNotifier] WhatsApp notification sent successfully")
        global _last_whatsapp_sent_at
        _last_whatsapp_sent_at = time.time()
        return True
    if response.status_code == 429:
        global _whatsapp_rate_limited_until
        _whatsapp_rate_limited_until = time.time() + WHATSAPP_RATE_LIMIT_SECONDS
        print(
            "[RentalNotifier] WhatsApp rate limit (429) received; "
            f"skipping messages for {WHATSAPP_RATE_LIMIT_SECONDS}s"
        )
        return False
    print(
        "[RentalNotifier] WasenderAPI responded with error "
        f"(status={response.status_code} body={response.text})"
    )
    return False


def _process_rental_list(rentals, notification_type):
    print(
        f"[RentalNotifier] Processing {len(rentals)} rentals "
        f"for notification_type={notification_type}"
    )
    sent_count = 0
    for rental in rentals:
        rental_id = rental.get("id")
        end_dt = _parse_datetime(rental.get("end_datetime"))
        if rental_id is None or not end_dt:
            print(
                f"[RentalNotifier] Skipping rental due to missing data "
                f"(id={rental_id} end_dt={end_dt})"
            )
            continue

        if notification_already_sent(rental_id, notification_type):
            continue

        message = _build_message(rental, notification_type, end_dt)
        if _send_whatsapp_message(message):
            sent_count += 1
            log_notification(rental_id, notification_type)
        else:
            print(
                f"[RentalNotifier] Failed to send WhatsApp message for rental {rental_id}"
            )

    print(
        f"[RentalNotifier] Finished processing {notification_type} rentals "
        f"(sent={sent_count})"
    )


def check_and_send_notifications():
    print(
        "[RentalNotifier] Starting notification cycle "
        f"at {datetime.now(timezone.utc).isoformat()}"
    )
    rentals = fetch_rentals_from_supabase()
    _process_rental_list(rentals.get("overdue", []), "overdue")
    _process_rental_list(rentals.get("expiring", []), "expiring")


def _notification_worker():
    while True:
        try:
            check_and_send_notifications()
        except Exception as exc:
            print(f"[RentalNotifier] Unexpected error during notification cycle: {exc}")
        time.sleep(NOTIFICATION_INTERVAL_SECONDS)


def start_rental_notification_scheduler():
    print(
        "[RentalNotifier] Scheduler starting "
        f"with interval {NOTIFICATION_INTERVAL_SECONDS} seconds"
    )
    worker = threading.Thread(
        target=_notification_worker, name="rental-notifier", daemon=True
    )
    worker.start()


start_rental_notification_scheduler()


def process_wasender_payload(payload):
    """Process Wasender webhook payload asynchronously.
    This function performs the original webhook handling logic but does not
    return HTTP responses. It is safe to run in a background thread.
    """
    req_id = _generate_request_id()
    log_prefix = f"[WasenderWebhook][{req_id}]"
    event_name = payload.get("event")
    normalized_event = _normalize_event_key(event_name)
    data_section = payload.get("data") or {}
    print(f"{log_prefix} Event received: {event_name}")
    _persist_webhook_payload(req_id, normalized_event or event_name, payload)

    if normalized_event == "contacts.upsert":
        print(f"{log_prefix} [ContactsUpsert] Full payload: {payload}")
        if not isinstance(data_section, dict):
            print(f"{log_prefix} [ContactsUpsert] Missing data section; skipping")
            return

        contact_id = data_section.get("id")
        if not contact_id:
            print(f"{log_prefix} [ContactsUpsert] Missing contact id; skipping")
            return

        if isinstance(contact_id, str) and contact_id.endswith("@whatsapp.net"):
            contact_id = contact_id[: -len("@whatsapp.net")]

        digits_only = "".join(char for char in str(contact_id) if char.isdigit())
        normalized_phone = _normalize_iraqi_number(digits_only)
        print(f"{log_prefix} [ContactsUpsert] Raw id: {contact_id}")
        print(f"{log_prefix} [ContactsUpsert] Normalized phone: {normalized_phone}")
        if not normalized_phone:
            print(f"{log_prefix} [ContactsUpsert] Failed to normalize phone; skipping")
            return

        print(f"{log_prefix} [ContactsUpsert] Contact detected: {normalized_phone}")
        contact_name = data_section.get("name") or data_section.get("pushName") or "Unknown"

        try:
            existing = (
                supabase.table("clients")
                .select("id")
                .eq("phone", normalized_phone)
                .limit(1)
                .execute()
            )
        except Exception as exc:
            print(f"{log_prefix} [ContactsUpsert] Failed to query clients table: {exc}")
            return

        try:
            if existing.data:
                client_id = existing.data[0].get("id")
                supabase.table("clients").update({"name": contact_name}).eq(
                    "id", client_id
                ).execute()
                print(f"{log_prefix} [ContactsUpsert] Client updated: {normalized_phone}")
            else:
                supabase.table("clients").insert(
                    {"phone": normalized_phone, "name": contact_name}
                ).execute()
                print(f"{log_prefix} [ContactsUpsert] Client inserted: {normalized_phone}")
        except Exception as exc:
            print(f"{log_prefix} [ContactsUpsert] Failed to upsert client: {exc}")

        return

    if normalized_event in SUPPORTED_MESSAGE_EVENTS:
        message_tag = "[MessageUpsert]" if normalized_event == "messages.upsert" else "[MessageEvent]"
        print(f"{log_prefix} {message_tag} Full payload received")
        print(f"{log_prefix} {message_tag} Payload: {payload}")
        if not isinstance(data_section, dict):
            print(f"{log_prefix} {message_tag} Missing data section; skipping")
            return

        message = _coerce_message_dict(data_section)
        if not isinstance(message, dict):
            print(f"{log_prefix} {message_tag} messages block missing or invalid; skipping")
            return
        print(f"{log_prefix} {message_tag} messages keys={list(message.keys())}")

        if _extract_from_me_flag(message):
            print(f"{log_prefix} {message_tag} Outbound message detected; skipping client insert")
            return

        push_name, _ = _resolve_sender_name(payload, data_section, message)
        push_name = push_name or "Unknown"
        raw_push = message.get("pushName")
        print(f"{log_prefix} {message_tag} pushName raw: {raw_push} chosen: {push_name}")

        normalized_phone = None
        source_used = None
        digits = ""

        cleaned_candidate = message.get("cleanedSenderPn")
        print(f"{log_prefix} {message_tag} Testing cleanedSenderPn: {cleaned_candidate}")
        normalized_phone, digits, reason = normalize_iraqi_phone_from_jid(
            cleaned_candidate, debug=DEBUG_WEBHOOK, log_prefix=log_prefix
        )
        if normalized_phone:
            source_used = "cleanedSenderPn"
            print(f"{log_prefix} {message_tag} Phone accepted from cleanedSenderPn: {normalized_phone}")

        if normalized_phone is None:
            sender_candidate = message.get("senderPn")
            print(f"{log_prefix} {message_tag} Testing senderPn: {sender_candidate}")
            normalized_phone, digits, reason = normalize_iraqi_phone_from_jid(
                sender_candidate, debug=DEBUG_WEBHOOK, log_prefix=log_prefix
            )
            if normalized_phone:
                source_used = "senderPn"
                print(f"{log_prefix} {message_tag} Phone accepted from senderPn: {normalized_phone}")
            elif sender_candidate is not None:
                print(f"{log_prefix} {message_tag} senderPn rejected ({reason}), digits={digits}")

        if normalized_phone is None:
            remote_jid = message.get("remoteJid")
            print(f"{log_prefix} {message_tag} Testing remoteJid: {remote_jid}")
            if isinstance(remote_jid, str) and "@lid" in remote_jid.lower():
                print(f"{log_prefix} {message_tag} remoteJid contains @lid; ignoring")
            else:
                normalized_phone, digits, reason = normalize_iraqi_phone_from_jid(
                    remote_jid, debug=DEBUG_WEBHOOK, log_prefix=log_prefix
                )
                if normalized_phone:
                    source_used = "remoteJid"
                    print(f"{log_prefix} {message_tag} Phone accepted from remoteJid: {normalized_phone}")
                elif remote_jid is not None:
                    print(f"{log_prefix} {message_tag} remoteJid rejected ({reason}), digits={digits}")

        candidates = extract_sender_candidates(payload)
        best_candidate = _select_best_candidate(candidates)
        if DEBUG_WEBHOOK:
            print(f"{log_prefix} {message_tag} [DeepScan] Candidates found: {len(candidates)}")
            for cand in candidates:
                print(
                    f"{log_prefix} {message_tag} [DeepScan] path={cand['path']} "
                    f"value={cand['value']} digits={cand['digits']} score={cand['score']}"
                )
            if best_candidate:
                print(
                    f"{log_prefix} {message_tag} [DeepScan] Best candidate path={best_candidate['path']} "
                    f"value={best_candidate['value']}"
                )

        if normalized_phone is None and best_candidate:
            deep_value = best_candidate["value"]
            normalized_phone, digits, reason = normalize_iraqi_phone_from_jid(
                deep_value, debug=DEBUG_WEBHOOK, log_prefix=log_prefix
            )
            if normalized_phone:
                source_used = f"deep_scan:{best_candidate['path']}"
                print(
                    f"{log_prefix} {message_tag} Phone accepted from deep scan ({best_candidate['path']}): "
                    f"{normalized_phone}"
                )
            elif DEBUG_WEBHOOK:
                print(
                    f"{log_prefix} {message_tag} [DeepScan] Candidate rejected ({reason}); digits={digits}"
                )

        if normalized_phone is None:
            print(f"{log_prefix} {message_tag} No reliable phone found; full payload logged for debugging")
            print(f"{log_prefix} {message_tag} Payload: {payload}")
            return

        try:
            existing = (
                supabase.table("clients")
                .select("id")
                .eq("phone", normalized_phone)
                .limit(1)
                .execute()
            )
            if DEBUG_WEBHOOK:
                print(
                    f"{log_prefix} {message_tag} Supabase select resp data={existing.data} "
                    f"error={getattr(existing, 'error', None)}"
                )
        except Exception as exc:
            print(f"{log_prefix} {message_tag} Failed to query clients table: {exc}")
            return

        if existing.data:
            print(f"{log_prefix} {message_tag} Client resolved: {normalized_phone}")
        else:
            try:
                insert_resp = (
                    supabase.table("clients")
                    .insert({"phone": normalized_phone, "name": push_name})
                    .execute()
                )
                if DEBUG_WEBHOOK:
                    print(
                        f"{log_prefix} {message_tag} Supabase insert resp data={insert_resp.data} "
                        f"error={getattr(insert_resp, 'error', None)}"
                    )
                print(f"{log_prefix} {message_tag} Client created on first message: {normalized_phone}")
            except Exception as exc:
                print(f"{log_prefix} {message_tag} Failed to insert client: {exc}")
                return

        print(f"{log_prefix} {message_tag} Client resolved for message: {normalized_phone}")
        return


@app.route("/", methods=["GET"])
def health_check():
    return "RealesrateCRM Webhook is running", 200


@app.route("/wasender/webhook", methods=["POST"])
def wasender_webhook():
    payload = request.get_json(silent=True) or {}
    print("[WasenderWebhook] Payload received")
    try:
        threading.Thread(target=process_wasender_payload, args=(payload,), daemon=True).start()
    except Exception as exc:
        print(f"[WasenderWebhook] Failed to start background processor: {exc}")
    return "", 200


@app.route("/upload_image", methods=["POST"])
def upload_image():
    """Accept multipart/form-data file under key 'image', upload to ImgBB, return public URL as JSON."""
    file = request.files.get("image")
    if not file:
        return json.dumps({"ok": False, "error": "file_missing"}), 400, {"Content-Type": "application/json"}

    imgbb_key = os.environ.get("IMG_BB_API_KEY")
    if not imgbb_key:
        return json.dumps({"ok": False, "error": "missing_imgbb_key"}), 500, {"Content-Type": "application/json"}

    try:
        file_bytes = file.read()
        encoded = base64.b64encode(file_bytes).decode("ascii")
        resp = requests.post(
            "https://api.imgbb.com/1/upload",
            data={"key": imgbb_key, "image": encoded},
            timeout=30,
        )
    except Exception as exc:
        return json.dumps({"ok": False, "error": str(exc)}), 500, {"Content-Type": "application/json"}

    if resp.status_code != 200:
        body = ""
        try:
            body = resp.text
        except Exception:
            pass
        return json.dumps({"ok": False, "error": f"imgbb_status_{resp.status_code}", "body": body}), 500, {"Content-Type": "application/json"}

    try:
        data = resp.json()
    except ValueError:
        return json.dumps({"ok": False, "error": "invalid_imgbb_response"}), 500, {"Content-Type": "application/json"}

    url = None
    if isinstance(data, dict):
        url = data.get("data", {}).get("display_url") or data.get("data", {}).get("url")

    if not url:
        return json.dumps({"ok": False, "error": "no_url_in_response", "resp": data}), 500, {"Content-Type": "application/json"}

    return json.dumps({"ok": True, "url": url}), 200, {"Content-Type": "application/json"}


if DEBUG_WEBHOOK:

    @app.route("/debug/last-webhook", methods=["GET"])
    def debug_last_webhook():
        summary_path = WEBHOOK_DUMP_DIR / "last.json"
        if not summary_path.exists():
            return "Not Found", 404
        try:
            contents = summary_path.read_text(encoding="utf-8")
        except Exception as exc:
            print(f"[WebhookDebug] Failed to read last webhook summary: {exc}")
            return "Error", 500
        return contents, 200, {"Content-Type": "application/json"}


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
