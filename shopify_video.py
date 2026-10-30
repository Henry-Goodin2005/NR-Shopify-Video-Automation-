#!/usr/bin/env python3
"""
NetSuite item videos -> Shopify product media.

Companion to the eBay pipeline in NR-Ebay-Video-Automation. Same shape,
different destination: a scheduled run picks up items that have a video file
and a Shopify product, moves the bytes, and attaches the result.

WHY THIS IS A HOSTED SERVICE AND NOT A WORKFLOW ACTION
------------------------------------------------------
Shopify will not fetch an external URL for video. Tested twice on 2026-10-26:
the NetSuite media.nl URL and a clean public .mp4 from Google's own bucket both
came back "Invalid video url". Images ARE fetched that way - nr_wa_shopify_item_
create.js has been sending media.nl image URLs since September - but video is
not. EXTERNAL_VIDEO is YouTube and Vimeo only.

So the bytes have to move, and SuiteScript cannot move them: File.getContents()
holds content in memory with a 10 MB cap (file.load itself goes to 2 GB, but
there is no streaming upload), and NR's videos run around 20 MB. Hence Python
on a schedule, same as eBay.

A run does two things, in this order:

  1. reconcile  anything left at UPLOADED by an earlier run - ask Shopify once
                and move it on. Transcoding outlasts a single run routinely.
  2. upload     every queued item - stage, send, attach, poll.

Status lives on the item (custitem_shopify_video_status); history lives in
customrecord_nr_video_log, the same record the eBay pipeline writes to.

Modes
-----
  python shopify_video.py
      The batch run above. This is what the schedule calls.

  python shopify_video.py --test-netsuite
      Print the queue. Changes nothing.

  python shopify_video.py --diagnose
      Counting queries showing what this role can see, plus a Shopify token
      mint. Changes nothing.

  python shopify_video.py --reconcile
      Step 1 only.

  python shopify_video.py --item <itemId>
      Run one item by hand, queue filter ignored.

  python shopify_video.py --set-status <itemId> <STATUS>
      Force a status, verified. Used to re-queue a failed item.

  python shopify_video.py --delete <productGid> <mediaGid>
      Remove a video from a product. Cleanup after testing.
"""
import os
import sys
import time
import tempfile

import requests

from dotenv import load_dotenv
load_dotenv()

from requests_oauthlib import OAuth1
from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256

# ------------------------------------------------------------ NetSuite ----

NS_ACCOUNT = os.environ["NS_ACCOUNT"]                      # 7154584 or 7154584_SB2
NS_HOST    = NS_ACCOUNT.lower().replace("_", "-")
NS_REST    = f"https://{NS_HOST}.suitetalk.api.netsuite.com/services/rest"
NS_UI      = f"https://{NS_HOST}.app.netsuite.com"          # File Cabinet lives here

# -------------------------------------------------------------- Shopify ----

SHOP_DOMAIN = os.environ.get("SHOPIFY_SHOP_DOMAIN", "national-rarities.myshopify.com")
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2026-07")
CLIENT_ID   = os.environ["SHOPIFY_CLIENT_ID"]

TOKEN_URL   = f"https://{SHOP_DOMAIN}/admin/oauth/access_token"
GRAPHQL_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"

# Shopify's documented ceilings. Checked before staging so an oversized file
# fails with a clear message here rather than an opaque one from Google.
MAX_VIDEO_BYTES = 1024 * 1024 * 1024      # 1 GB

POLL_ATTEMPTS = 20
POLL_DELAY    = 15

# Channel marker on the shared log record. Create custrecord_nrvl_channel
# (Free-Form Text) on customrecord_nr_video_log before relying on this -
# NetSuite silently drops writes to fields that do not exist, so until it is
# created the log rows are still written, just without the channel.
CHANNEL = "SHOPIFY"


# ---------------------------------------------------------------- queue ----

# Items with a video file and a Shopify product that have not been attempted.
#
# IS NULL on the status is the idempotency guard: an item is picked up once.
# A failure sets FAILED and stays there until a human re-queues it, which is
# what stops a bad file looping and attaching duplicate videos to a product.
#
# isonline is SELECTED but not filtered on. A file that is not Available
# Without Login cannot be fetched, but filtering it out here would make the
# item silently invisible; better to pick it up, fail loudly and log why.
QUEUE_SQL = """
SELECT
    i.id                              AS item_id,
    i.itemid                          AS sku,
    i.custitem_shopify_product_id     AS product_gid,
    f.id                              AS file_id,
    f.name                            AS file_name,
    f.filesize                        AS file_size,
    f.isonline                        AS is_online,
    f.url                             AS file_url
FROM item i
JOIN file f ON f.id = i.custitem_nr_video
WHERE i.custitem_shopify_product_id IS NOT NULL
  AND i.custitem_shopify_video_status IS NULL
"""

# Items left mid-flight by an earlier run.
UPLOADED_SQL = """
SELECT
    i.id                           AS item_id,
    i.itemid                       AS sku,
    i.custitem_shopify_product_id  AS product_gid,
    i.custitem_shopify_video_id    AS media_gid
FROM item i
WHERE i.custitem_shopify_video_status = 'UPLOADED'
  AND i.custitem_shopify_video_id IS NOT NULL
"""

ONE_ITEM_SQL = """
SELECT
    i.id                              AS item_id,
    i.itemid                          AS sku,
    i.custitem_shopify_product_id     AS product_gid,
    f.id                              AS file_id,
    f.name                            AS file_name,
    f.filesize                        AS file_size,
    f.isonline                        AS is_online,
    f.url                             AS file_url
FROM item i
JOIN file f ON f.id = i.custitem_nr_video
WHERE i.id = {item_id}
"""


def ns_auth():
    """OAuth 1.0a / HMAC-SHA256, the way NetSuite's TBA wants it."""
    return OAuth1(
        client_key=os.environ["NS_CONSUMER_KEY"],
        client_secret=os.environ["NS_CONSUMER_SECRET"],
        resource_owner_key=os.environ["NS_TOKEN_ID"],
        resource_owner_secret=os.environ["NS_TOKEN_SECRET"],
        signature_method=SIGNATURE_HMAC_SHA256,
        realm=NS_ACCOUNT.upper(),
    )


def ns_query(sql, timeout=180):
    """
    Any SuiteQL query.

    Timeout is generous because the queue joins item to file, and under a
    scoped role NetSuite evaluates row-level permissions across the whole File
    Cabinet. The eBay script learned this the hard way at 90 seconds.
    """
    r = requests.post(
        f"{NS_REST}/query/v1/suiteql",
        auth=ns_auth(),
        headers={"Content-Type": "application/json", "Prefer": "transient"},
        json={"q": sql},
        timeout=timeout,
    )
    if r.status_code != 200:
        print(f"SuiteQL -> HTTP {r.status_code}\n{r.text[:800]}")
        r.raise_for_status()
    return r.json().get("items", [])


def ns_set_fields(item_id, values, verify=True):
    """
    Write fields onto the item, then read them back.

    NetSuite's REST API returns 204 for writes it silently discards - a field
    it will not expose, a permission it will not honour. A success code proves
    the request was accepted, not that anything changed. This bit BCS-421
    three separate ways, so every write that matters is checked.
    """
    r = requests.patch(
        f"{NS_REST}/record/v1/inventoryItem/{item_id}",
        auth=ns_auth(),
        headers={"Content-Type": "application/json"},
        json=values,
        timeout=60,
    )
    if r.status_code not in (200, 204):
        print(f"  ! write failed: HTTP {r.status_code} {r.text[:400]}")
        return False

    if not verify:
        return True

    cols = ", ".join(values.keys())
    try:
        back = ns_query(f"SELECT {cols} FROM item WHERE id = {int(item_id)}", timeout=60)
    except Exception as e:
        print(f"  ! wrote HTTP {r.status_code} but could not verify: {e}")
        return False

    row = back[0] if back else {}
    bad = [f"{k}: wanted {v!r}, found {row.get(k)!r}"
           for k, v in values.items() if str(row.get(k)) != str(v)]
    if bad:
        print(f"  ! NetSuite returned HTTP {r.status_code} but did not store:")
        for line in bad:
            print(f"      {line}")
        return False
    return True


def ns_set_status(item_id, status, note=None):
    """
    Current state on the item. History goes to the log record, because a field
    is overwritten by the next run and the second failure would erase the first.
    """
    if not ns_set_fields(item_id, {"custitem_shopify_video_status": status}):
        return False
    print(f"  NetSuite: item {item_id} -> {status}")

    # The note is a convenience, written separately and unverified on purpose.
    # It must never be able to fail a run or mask the status write.
    if note:
        ns_set_fields(item_id,
                      {"custitem_shopify_video_last_result": str(note)[:290]},
                      verify=False)
    return True


def ns_log(item_id, media_gid, result, message="", file_size=None):
    """
    One row per attempt in customrecord_nr_video_log - the same record the eBay
    pipeline uses, distinguished by the channel field.

    Best effort: a failed log write is reported but never fails the run. The
    upload is the job; the log is the record of it.
    """
    body = {
        "name": f"{result} - item {item_id} (shopify)",
        "custrecord_nrvl_item": {"id": str(item_id)},
        "custrecord_nrvl_video_id": media_gid or "",
        "custrecord_nrvl_result": result,
        "custrecord_nrvl_message": str(message)[:3900],
        "custrecord_nrvl_channel": CHANNEL,
    }
    if file_size:
        body["custrecord_nrvl_file_size"] = int(file_size)

    try:
        r = requests.post(
            f"{NS_REST}/record/v1/customrecord_nr_video_log",
            auth=ns_auth(),
            headers={"Content-Type": "application/json"},
            json=body,
            timeout=60,
        )
        if r.status_code not in (200, 201, 204):
            print(f"  ! log write failed: HTTP {r.status_code} {r.text[:300]}")
            return False
        print(f"  logged: {result}")
        return True
    except Exception as e:
        print(f"  ! log write failed: {type(e).__name__}: {e}")
        return False


# -------------------------------------------------------------- Shopify ----

_token_cache = None


def shopify_token():
    """
    Admin API token via the client credentials grant, minted per execution.

    >> FORM ENCODED, NOT JSON. <<
    A JSON body is accepted and then silently fails to parse, so client_secret
    arrives empty and Shopify answers "Missing or invalid client secret" - which
    reads as a bad credential and sends you hunting in the wrong place. Cost an
    hour on 2026-10-29.

    The client secret is the durable credential and lives in the environment;
    the token it mints expires in ~24h, which is why nothing stores one.
    """
    global _token_cache
    if _token_cache:
        return _token_cache

    r = requests.post(
        TOKEN_URL,
        data={
            "client_id": CLIENT_ID,
            "client_secret": os.environ["SHOPIFY_CLIENT_SECRET"],
            "grant_type": "client_credentials",
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Shopify token request HTTP {r.status_code}: {r.text[:400]}")

    body = r.json()
    if not body.get("access_token"):
        raise RuntimeError(f"Shopify returned no access_token: {r.text[:400]}")

    # Scopes are logged, the token is not. Note write_files is NOT required -
    # stagedUploadsCreate works on the product scopes, confirmed by testing.
    print(f"  Shopify token minted, scopes={body.get('scope')}")
    _token_cache = body["access_token"]
    return _token_cache


def graphql(query, variables=None, timeout=60):
    """
    One GraphQL call.

    A GraphQL failure arrives as HTTP 200 with an `errors` array, so checking
    the status code alone silently swallows every schema and permission error.
    """
    r = requests.post(
        GRAPHQL_URL,
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": shopify_token(),
        },
        json={"query": query, "variables": variables or {}},
        timeout=timeout,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Shopify HTTP {r.status_code}: {r.text[:400]}")

    body = r.json()
    if body.get("errors"):
        raise RuntimeError(f"Shopify GraphQL errors: {body['errors']}")
    return body["data"]


STAGED_UPLOADS_CREATE = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets { url resourceUrl parameters { name value } }
    userErrors { field message }
  }
}
"""

PRODUCT_UPDATE = """
mutation attachVideo($input: ProductInput!, $media: [CreateMediaInput!]) {
  productUpdate(input: $input, media: $media) {
    product { id media(first: 50) { nodes { id alt status mediaContentType } } }
    userErrors { field message }
  }
}
"""

PRODUCT_MEDIA = """
query productMedia($id: ID!) {
  node(id: $id) {
    ... on Product {
      media(first: 50) {
        nodes { id status mediaContentType mediaErrors { code message details } }
      }
    }
  }
}
"""

PRODUCT_DELETE_MEDIA = """
mutation deleteMedia($productId: ID!, $mediaIds: [ID!]!) {
  productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
    deletedMediaIds
    mediaUserErrors { field message }
  }
}
"""


def download(url, name):
    """
    Stream the video out of the File Cabinet to a temp file.

    The ftyp check is lifted from the eBay script, where it caught NetSuite
    serving a login page for a file that was not Available Without Login. An
    HTML error page uploads perfectly happily and fails much later.
    """
    if url.startswith("/"):
        url = NS_UI + url

    tmp = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            tmp.write(chunk)
    tmp.close()

    size = os.path.getsize(tmp.name)
    with open(tmp.name, "rb") as f:
        head = f.read(12)
    if b"ftyp" not in head:
        os.remove(tmp.name)
        raise RuntimeError(
            f"That URL did not return an MP4 (first bytes: {head!r}). "
            "Check the h= hash and that the file is Available Without Login."
        )

    print(f"  downloaded {size:,} bytes  ({name})")
    return tmp.name, size


def stage_and_upload(path, size, name):
    """
    Ask Shopify for a Google Cloud Storage target and send the bytes.

    >> SIZE MUST BE THE REAL SIZE OF THE FILE BEING SENT. <<
    The signed policy pins content-length-range to this exact number on both
    bounds. NetSuite's f.filesize is NOT used for this - a file that was
    re-muxed somewhere between the Cabinet and here differs by a few KB and
    GCS rejects the upload with EntityTooLarge. Observed 2026-10-29: a stored
    1,670,889 against an actual 1,676,764.

    Staging and uploading are one function on purpose. The policy carries a
    short expiry (about four minutes observed), so there is no batching the
    stagedUploadsCreate calls ahead of the uploads.
    """
    data = graphql(STAGED_UPLOADS_CREATE, {
        "input": [{
            "filename": name,
            "mimeType": "video/mp4",
            "resource": "VIDEO",
            "fileSize": str(size),      # string, and required for video
            "httpMethod": "POST",       # video is multipart POST; images are PUT
        }]
    })
    result = data["stagedUploadsCreate"]
    if result["userErrors"]:
        raise RuntimeError(f"stagedUploadsCreate userErrors: {result['userErrors']}")

    target = result["stagedTargets"][0]

    # ORDER MATTERS: every policy parameter as a plain form field first, then
    # `file` LAST. A list of tuples preserves that; a dict does not guarantee it.
    #
    # NOTHING EXTRA: the signed policy enumerates exactly which fields may
    # appear. Adding one it does not reference - `bucket`, say - makes GCS
    # reject the whole upload. The bucket is already in the hostname, and the
    # parameters Shopify returns are the complete and correct set.
    #
    # No auth header. The signed policy is the authorization.
    fields = [(p["name"], (None, p["value"])) for p in target["parameters"]]
    with open(path, "rb") as fh:
        fields.append(("file", (name, fh, "video/mp4")))
        r = requests.post(target["url"], files=fields, timeout=900)

    # GCS answers 204 No Content on success, sometimes 201. Not 200.
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"staged upload -> HTTP {r.status_code}: {r.text[:600]}")

    print(f"  uploaded to Shopify staging (HTTP {r.status_code})")
    return target["resourceUrl"]


def attach(product_gid, resource_url, alt):
    """
    Attach the staged video to the product.

    productUpdate, NOT productSet. productSet is declarative - it sets the
    product to exactly what you pass, so a partial input would strip the
    existing images and metafields off a live product.

    Shapes, each of which cost a round trip to discover:
      - the argument is ProductInput, not ProductUpdateInput
      - media is a SEPARATE argument of type [CreateMediaInput!]
      - the field is mediaContentType, not contentType
        (`files` belongs to ProductSetInput and does not apply here)
    """
    data = graphql(PRODUCT_UPDATE, {
        "input": {"id": product_gid},
        "media": [{
            "originalSource": resource_url,
            "mediaContentType": "VIDEO",
            "alt": alt,
        }],
    })
    result = data["productUpdate"]
    if result["userErrors"]:
        raise RuntimeError(f"productUpdate userErrors: {result['userErrors']}")

    videos = [n for n in result["product"]["media"]["nodes"]
              if n["mediaContentType"] == "VIDEO"]
    if not videos:
        raise RuntimeError("productUpdate reported no error but created no video node.")

    media_gid = videos[-1]["id"]
    print(f"  attached {media_gid}")
    return media_gid


def media_status(product_gid, media_gid):
    """Current status of one media node, or None if Shopify has lost it."""
    data = graphql(PRODUCT_MEDIA, {"id": product_gid})
    nodes = ((data.get("node") or {}).get("media") or {}).get("nodes", [])
    for n in nodes:
        if n["id"] == media_gid:
            return n
    return None


def poll(product_gid, media_gid):
    """
    Wait for Shopify to transcode.

    An empty userErrors on the attach means accepted, not ready. Video takes
    minutes where images take seconds - which is why the workflow action
    script's 9-second checkMedia() budget would be useless here.

    Returns the final node, or None if it outran this run. Not an error:
    reconcile picks it up next time.
    """
    for i in range(POLL_ATTEMPTS):
        if i:
            time.sleep(POLL_DELAY)
        node = media_status(product_gid, media_gid)
        if not node:
            print(f"  [{i+1}/{POLL_ATTEMPTS}] media node not visible yet")
            continue
        print(f"  [{i+1}/{POLL_ATTEMPTS}] status={node['status']}")
        if node["status"] in ("READY", "FAILED"):
            return node
    print("  still processing - not an error, reconcile will confirm.")
    return None


# ----------------------------------------------------------------- work ----

def process(row):
    """One item, end to end. Raises on failure after recording it."""
    item_id     = row["item_id"]
    sku         = row.get("sku")
    product_gid = row["product_gid"]
    file_name   = row.get("file_name") or f"item_{item_id}.mp4"
    stored_size = int(row.get("file_size") or 0)

    print(f"\n{sku}  item={item_id}  product={product_gid}")

    if str(row.get("is_online")) != "T":
        msg = (f"file {row.get('file_id')} ({file_name}) is not Available Without "
               "Login, so Shopify's staging cannot be fed from it. Tick the box "
               "on the file record, or move the video into the item's own media "
               "folder where the flag is already set.")
        print(f"  ! {msg}")
        ns_set_status(item_id, "FAILED", msg)
        ns_log(item_id, "", "FAILED", msg, stored_size)
        raise RuntimeError(msg)

    path = None
    try:
        path, size = download(row["file_url"], file_name)

        if size > MAX_VIDEO_BYTES:
            raise RuntimeError(f"{size:,} bytes is over Shopify's 1 GB video limit.")
        if stored_size and size != stored_size:
            # Not fatal - `size` is what gets signed - but worth knowing, since
            # it means something altered the file in transit.
            print(f"  note: NetSuite recorded {stored_size:,} bytes, got {size:,}")

        resource_url = stage_and_upload(path, size, file_name)
        media_gid = attach(product_gid, resource_url, f"{sku} video")

        # Record the id before polling, so a slow or failing poll can never
        # cost us the handle on media we have already created.
        ns_set_fields(item_id, {"custitem_shopify_video_id": media_gid})
        ns_set_status(item_id, "UPLOADED", "attached to product, Shopify transcoding")
        ns_log(item_id, media_gid, "UPLOADED", "attached to product", size)

        node = poll(product_gid, media_gid)
        if node and node["status"] == "READY":
            ns_set_status(item_id, "READY", "live on the Shopify product")
            ns_log(item_id, media_gid, "READY", "live on the Shopify product", size)
        elif node and node["status"] == "FAILED":
            detail = f"Shopify rejected the video: {node.get('mediaErrors')}"
            ns_set_status(item_id, "FAILED", detail)
            ns_log(item_id, media_gid, "FAILED", detail, size)
        else:
            ns_log(item_id, media_gid, "UPLOADED",
                   "still processing when the run ended - reconcile will confirm", size)
        return True

    except Exception as e:
        ns_set_status(item_id, "FAILED", str(e))
        ns_log(item_id, "", "FAILED", str(e), stored_size)
        raise
    finally:
        if path and os.path.exists(path):
            os.remove(path)


def reconcile(rows):
    """
    Pick up anything left at UPLOADED by an earlier run.

    Without this, a video whose transcode outlasts its own run sits at UPLOADED
    forever: nothing moves it to READY and the product shows no video with no
    error anywhere. Same reasoning as the eBay script's reconcile pass, and it
    also covers a run that dies after the attach but before the poll.
    """
    if not rows:
        return

    print(f"{len(rows)} item(s) left at UPLOADED from an earlier run")
    for row in rows:
        item_id, media_gid, sku = row["item_id"], row["media_gid"], row.get("sku")
        try:
            node = media_status(row["product_gid"], media_gid)
            if not node:
                msg = "media node no longer on the product - deleted in Shopify?"
                print(f"  {sku} item={item_id}: {msg}")
                ns_set_status(item_id, "FAILED", msg)
                ns_log(item_id, media_gid, "FAILED", msg)
                continue

            print(f"  {sku}  item={item_id}  Shopify says {node['status']}")
            if node["status"] == "READY":
                ns_set_status(item_id, "READY", "live on the Shopify product (reconciled)")
                ns_log(item_id, media_gid, "READY", "live (reconciled)")
            elif node["status"] == "FAILED":
                detail = f"Shopify rejected the video: {node.get('mediaErrors')}"
                ns_set_status(item_id, "FAILED", detail)
                ns_log(item_id, media_gid, "FAILED", detail)
            # Still processing: leave it, try again next run.

        except Exception as e:
            print(f"  ! {sku} item={item_id}: {type(e).__name__}: {e}")
    print()


# ----------------------------------------------------------------- main ----

def main():
    args = sys.argv[1:]

    if args == ["--test-netsuite"]:
        rows = ns_query(QUEUE_SQL)
        print(f"{len(rows)} item(s) queued\n")
        for r in rows:
            print(f"  {r.get('sku')}  item={r['item_id']}  product={r['product_gid']}")
            print(f"    file {r['file_id']}  {r.get('file_name')}  "
                  f"{int(r.get('file_size') or 0):,} bytes  online={r.get('is_online')}")
            print(f"    {r['file_url']}\n")
        return 0

    if args == ["--diagnose"]:
        checks = [
            ("items visible at all",        "SELECT COUNT(*) AS n FROM item WHERE ROWNUM <= 1"),
            ("files visible at all",        "SELECT COUNT(*) AS n FROM file WHERE ROWNUM <= 1"),
            ("log record readable",         "SELECT COUNT(*) AS n FROM customrecord_nr_video_log WHERE ROWNUM <= 1"),
            ("items with a video file",     "SELECT COUNT(*) AS n FROM item WHERE custitem_nr_video IS NOT NULL"),
            ("items with a Shopify product","SELECT COUNT(*) AS n FROM item WHERE custitem_shopify_product_id IS NOT NULL"),
            ("items at UPLOADED",           "SELECT COUNT(*) AS n FROM item WHERE custitem_shopify_video_status = 'UPLOADED'"),
            ("the full queue query",        f"SELECT COUNT(*) AS n FROM ({QUEUE_SQL})"),
        ]
        print("what this role can see:\n")
        for label, sql in checks:
            try:
                rows = ns_query(sql, timeout=90)
                print(f"  {label:<30} {rows[0].get('n') if rows else 0}")
            except Exception as e:
                print(f"  {label:<30} {type(e).__name__}: {e}")
        print("\nShopify:")
        try:
            shopify_token()
        except Exception as e:
            print(f"  token mint FAILED: {e}")
        return 0

    if args == ["--reconcile"]:
        reconcile(ns_query(UPLOADED_SQL))
        return 0

    if len(args) == 2 and args[0] == "--item":
        rows = ns_query(ONE_ITEM_SQL.format(item_id=int(args[1])))
        if not rows:
            sys.exit(f"item {args[1]} has no video file attached, or does not exist")
        if not rows[0].get("product_gid"):
            sys.exit(f"item {args[1]} has no custitem_shopify_product_id - "
                     "it has never been synced to Shopify")
        try:
            process(rows[0])
        except Exception as e:
            sys.exit(f"\n{e}")
        return 0

    if len(args) == 3 and args[0] == "--set-status":
        return 0 if ns_set_status(args[1], args[2]) else 1

    if len(args) == 3 and args[0] == "--delete":
        data = graphql(PRODUCT_DELETE_MEDIA, {"productId": args[1], "mediaIds": [args[2]]})
        result = data["productDeleteMedia"]
        # Note: mediaUserErrors, not userErrors - this mutation uses the
        # media-specific error type and querying the wrong one looks like success.
        if result["mediaUserErrors"]:
            sys.exit(f"mediaUserErrors: {result['mediaUserErrors']}")
        print(f"deleted: {result['deletedMediaIds']}")
        return 0

    if args:
        sys.exit(__doc__)

    # Batch mode.
    stragglers = ns_query(UPLOADED_SQL)
    queued     = ns_query(QUEUE_SQL)

    if not (stragglers or queued):
        print("nothing to do - no items waiting at any stage")
        return 0

    # Backlog first: if Shopify is slow, yesterday's items are closest to done.
    reconcile(stragglers)

    if not queued:
        return 0

    print(f"{len(queued)} item(s) to upload")
    failures = 0
    for row in queued:
        try:
            process(row)
        except Exception as e:
            failures += 1
            print(f"  ! {type(e).__name__}: {e}")

    print(f"\ndone - {len(queued) - failures} succeeded, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
