#!/usr/bin/env python3
"""
Pentolhaus — sincronizzazione CardTrader ↔ Shopify

Ogni esecuzione:
  1. Legge gli ordini Shopify pagati non ancora sincronizzati e scala le quantità su CardTrader.
  2. Scarica l'inventario CardTrader e crea/aggiorna su Shopify un prodotto per ogni articolo
     (prezzo, quantità, immagine, tag). Gli articoli spariti da CardTrader vanno a quantità 0.

CardTrader è la fonte di verità. Solo libreria standard di Python (nessuna dipendenza).

Variabili d'ambiente
  CARDTRADER_TOKEN        token API CardTrader (profilo → impostazioni)            [obbligatoria]
  SHOPIFY_SHOP            sottodominio myshopify, es. 61kf9q-8u                    [obbligatoria]
  SHOPIFY_CLIENT_ID       client ID dell'app nel Dev Dashboard Shopify   } oppure
  SHOPIFY_CLIENT_SECRET   client secret dell'app                         }
  SHOPIFY_ACCESS_TOKEN    token Admin API già pronto (alternativa ai due sopra)
  DRY_RUN                 "true" = non modifica nulla, scrive solo cosa farebbe (default true)
  PRICE_MARKUP_PERCENT    ricarico % sul prezzo CardTrader, es. 10 o -5 (default 0)
  MIN_PRICE               prezzo minimo su Shopify in euro (default 0.10)
  MIN_PRICE_UNCOMMON      prezzo minimo delle non comuni (default 0.50)
  MIN_PRICE_RARE          prezzo minimo di rare, rare mitiche e superiori (default 1.00)
  ROUND_PRICES            "true" = prezzi tondi: sotto 10 € ai 10 cent, fino a 100 € ai 50 cent, sopra all'euro (default true)
  ORDERS_SINCE            data ISO: considera solo gli ordini Shopify da questa data (default: oggi)
  ONLY_SINGLES            "true" = sincronizza solo le carte singole (default true)
  CACHE_FILE              file cache dei blueprint (default .cache/blueprints.json)
  MAX_CREATE              massimo di carte nuove create per giro (default 300)
  HD_IMAGES               "true" = immagini HD: Magic da Scryfall, Riftbound da Piltover Archive,
                          le altre (Pokémon…) nella versione originale di CardTrader (default true)
  MAX_IMAGE_UPDATES       massimo di prodotti esistenti a cui cambiare immagine per giro (default 300)
"""

import json
import uuid
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

CT_BASE = "https://api.cardtrader.com/api/v2"
SHOPIFY_API_VERSION = "2026-10"
SYNC_TAG = "cardtrader"
ORDER_DONE_TAG = "ct-synced"
HD_TAG = "img-hd"
CATALOG_TAG = "catalogo"  # carte del catalogo completo, mai avute in magazzino: esaurite e senza prezzo
CATALOG_SKU = "BP-"  # SKU dei segnaposto: CT-BP-<blueprint>-<f|n>
GROUP_TAG = "gruppo-carta"  # una scheda per carta, con versioni, condizioni e lingue come varianti
GROUP_OPTIONS = ["Versione", "Condizione", "Lingua"]
VERSION_IT = {"standard": "Normale", "": "Normale", "alt art": "Alt Art", "alternate art": "Alt Art",
              "overnumbered": "Overnumbered", "signed": "Firmata", "promo": "Promo", "showcase": "Showcase"}
MF_NS = "carta"  # campi per i filtri del negozio: espansione, rarita, lingua, condizione, foil

RARITY_IT = {
    "common": "Comune", "uncommon": "Non comune", "rare": "Rara", "mythic": "Rara mitica",
    "mythic rare": "Rara mitica", "special": "Speciale", "epic": "Epica", "showcase": "Showcase",
    "promo": "Promo", "land": "Terra", "basic land": "Terra base", "token": "Pedina",
}
LANG_IT = {
    "en": "Inglese", "it": "Italiano", "jp": "Giapponese", "ja": "Giapponese", "de": "Tedesco",
    "fr": "Francese", "es": "Spagnolo", "pt": "Portoghese", "ru": "Russo", "kr": "Coreano", "ko": "Coreano",
    "zh-cn": "Cinese semplificato", "zhs": "Cinese semplificato", "zh-tw": "Cinese tradizionale", "zht": "Cinese tradizionale",
}
SCRYFALL_API = "https://api.scryfall.com"
PILTOVER_API = "https://piltoverarchive.com/api/external/v1"
BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
SKU_PREFIX = "CT-"

CONDITION_SHORT = {
    "mint": "M",
    "near mint": "NM",
    "slightly played": "SP",
    "moderately played": "MP",
    "played": "PL",
    "heavily played": "HP",
    "poor": "PO",
}


def env_bool(name, default):
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "si", "sì", "on")


def env_float(name, default):
    v = os.environ.get(name, "").strip()
    try:
        return float(v.replace(",", ".")) if v else default
    except ValueError:
        return default


DRY_RUN = env_bool("DRY_RUN", True)


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def http_json(method, url, headers=None, body=None, retries=5):
    data = None
    hdrs = {"Accept": "application/json", "User-Agent": "pentolhaus-ct-sync/1.0"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            hdrs["Content-Type"] = "application/json"
        else:
            data = body if isinstance(body, bytes) else str(body).encode("utf-8")
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", "replace")
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = 2 ** attempt + 1
                log(f"  … {e.code} da {urllib.parse.urlparse(url).netloc}, riprovo tra {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} {method} {url}: {text[:500]}")
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt + 1)
                continue
            raise RuntimeError(f"Rete non raggiungibile {url}: {e}")
    raise RuntimeError(f"Troppi tentativi falliti: {url}")


# ---------------------------------------------------------------------------
# CardTrader
# ---------------------------------------------------------------------------

class CardTrader:
    def __init__(self, token):
        self.h = {"Authorization": f"Bearer {token}"}
        self._calls = []
        self._lock = threading.Lock()

    def _throttle(self):
        # limite: 200 richieste ogni 10 secondi
        while True:
            with self._lock:
                now = time.time()
                self._calls = [t for t in self._calls if now - t < 10]
                if len(self._calls) < 170:
                    self._calls.append(now)
                    return
                wait = 10 - (now - self._calls[0]) + 0.05
            time.sleep(max(wait, 0.05))

    def get(self, path, params=None):
        self._throttle()
        url = CT_BASE + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return http_json("GET", url, self.h)

    def post(self, path, body):
        self._throttle()
        return http_json("POST", CT_BASE + path, self.h, body)

    def info(self):
        return self.get("/info")

    def products(self):
        return self.get("/products/export")

    def games(self):
        return self.get("/games")

    def categories(self):
        return self.get("/categories")

    def expansions(self):
        return self.get("/expansions")

    def blueprints_of_expansion(self, expansion_id):
        return self.get("/blueprints/export", {"expansion_id": expansion_id})

    def marketplace_by_blueprint(self, blueprint_id):
        return self.get("/marketplace/products", {"blueprint_id": blueprint_id})

    def increment(self, product_id, delta):
        return self.post(f"/products/{product_id}/increment", {"delta_quantity": int(delta)})


class Scryfall:
    """Immagini HD delle carte Magic (scryfall.com). Max ~10 richieste/s come da loro linee guida."""

    def __init__(self):
        self._last = 0.0
        self._lock = threading.Lock()

    def _throttle(self):
        with self._lock:
            wait = self._last + 0.15 - time.time()
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

    @staticmethod
    def images_of(card):
        """URL delle immagini PNG (745x1040) di una carta; 2 per le carte a doppia faccia."""
        if (card.get("image_uris") or {}).get("png"):
            return [card["image_uris"]["png"]]
        out = []
        for face in card.get("card_faces") or []:
            u = (face.get("image_uris") or {}).get("png")
            if u:
                out.append(u)
        return out

    def collection(self, ids):
        """Mappa scryfall_id -> [url immagini] per una lista di id (75 per richiesta)."""
        out = {}
        ids = list(ids)
        for i in range(0, len(ids), 75):
            chunk = ids[i:i + 75]
            self._throttle()
            d = http_json("POST", SCRYFALL_API + "/cards/collection", None,
                          {"identifiers": [{"id": x} for x in chunk]})
            for c in d.get("data") or []:
                out[c["id"]] = self.images_of(c)
            for nf in d.get("not_found") or []:
                if nf.get("id"):
                    out[nf["id"]] = []
        return out


class PiltoverArchive:
    """Immagini HD delle carte Riftbound (piltoverarchive.com), abbinate con l'ID Cardmarket."""

    def all_images(self):
        """Mappa id Cardmarket -> url immagine, per tutte le stampe Riftbound."""
        return self.all_cards()[0]

    def all_cards(self):
        """(id Cardmarket -> url immagine, id Cardmarket -> [numero, versione]) per tutte le stampe Riftbound."""
        out, info, page = {}, {}, 1
        while True:
            d = http_json("GET", f"{PILTOVER_API}/cards?page={page}&limit=100", {"User-Agent": BROWSER_UA})
            for v in d.get("data") or []:
                if v.get("cardmarketId") and v.get("imageUrl"):
                    out[str(v["cardmarketId"])] = v["imageUrl"]
                if v.get("cardmarketId"):
                    info[str(v["cardmarketId"])] = [v.get("variantNumber") or "", v.get("variantLabel") or v.get("variantType") or ""]
            if not (d.get("pagination") or {}).get("hasNext"):
                break
            page += 1
            time.sleep(0.5)
        return out, info


def ct_full_image(url):
    """L'anteprima CardTrader (preview_xxx.jpg, 180 px) → l'immagine originale (xxx.jpg)."""
    if url and "/preview_" in url:
        return url.replace("/preview_", "/", 1)
    return None


def url_ok(url):
    """True se l'immagine esiste davvero (prima di sostituire quella vecchia)."""
    for method in ("HEAD", "GET"):
        req = urllib.request.Request(url, method=method, headers={"User-Agent": BROWSER_UA, "Range": "bytes=0-0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.status in (200, 206) and (r.headers.get("Content-Type") or "image").startswith("image")
        except urllib.error.HTTPError as e:
            if e.code in (403, 405) and method == "HEAD":
                continue
            return False
        except Exception:
            return False
    return False


def as_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        for k in ("array", "data", "products", "items"):
            if isinstance(x.get(k), list):
                return x[k]
    return []


# ---------------------------------------------------------------------------
# Shopify
# ---------------------------------------------------------------------------

class Shopify:
    def __init__(self, shop, token=None, client_id=None, client_secret=None):
        shop = shop.replace("https://", "").replace(".myshopify.com", "").strip("/")
        self.domain = f"{shop}.myshopify.com"
        self.url = f"https://{self.domain}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        if not token:
            if not (client_id and client_secret):
                raise SystemExit("Manca SHOPIFY_ACCESS_TOKEN oppure SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET")
            res = http_json(
                "POST",
                f"https://{self.domain}/admin/oauth/access_token",
                {"Content-Type": "application/x-www-form-urlencoded"},
                urllib.parse.urlencode({
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "client_credentials",
                }).encode("utf-8"),
            )
            token = res.get("access_token")
            if not token:
                raise SystemExit(f"Shopify non ha restituito un token: {res}")
        self.h = {"X-Shopify-Access-Token": token}

    def gql(self, query, variables=None):
        for attempt in range(6):
            res = http_json("POST", self.url, self.h, {"query": query, "variables": variables or {}})
            errs = res.get("errors")
            if errs:
                throttled = any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs if isinstance(e, dict))
                if throttled and attempt < 5:
                    time.sleep(2 + attempt * 2)
                    continue
                raise RuntimeError(f"Errore GraphQL: {json.dumps(errs)[:800]}")
            cost = (res.get("extensions") or {}).get("cost") or {}
            avail = (cost.get("throttleStatus") or {}).get("currentlyAvailable")
            if isinstance(avail, (int, float)) and avail < 200:
                time.sleep(2)
            return res["data"]
        raise RuntimeError("Shopify: troppe richieste rallentate")

    def primary_location(self):
        d = self.gql("{ locations(first: 10) { nodes { id name isActive } } }")
        locs = [l for l in d["locations"]["nodes"] if l.get("isActive", True)]
        if not locs:
            raise SystemExit("Nessuna location attiva su Shopify")
        return locs[0]["id"]

    def online_store_publication(self):
        d = self.gql("{ publications(first: 20) { nodes { id catalog { title } } } }")
        for p in d["publications"]["nodes"]:
            title = ((p.get("catalog") or {}).get("title") or "").lower()
            if "online store" in title:
                return p["id"]
        return None

    def synced_products(self, location_id):
        """Mappa ct_product_id -> info del prodotto Shopify."""
        q = """
        query($after: String, $loc: ID!) {
          products(first: 100, after: $after, query: "tag:%s -tag:gruppo-carta") {
            pageInfo { hasNextPage endCursor }
            nodes {
              id title status tags
              media(first: 10) { nodes { id } }
              metafields(first: 10, namespace: "%s") { nodes { key value } }
              variants(first: 1) { nodes {
                id sku price
                inventoryItem { id inventoryLevel(locationId: $loc) { quantities(names: ["available"]) { quantity } } }
              } }
            }
          }
        }""" % (SYNC_TAG, MF_NS)
        out, after = {}, None
        while True:
            d = self.gql(q, {"after": after, "loc": location_id})
            conn = d["products"]
            for p in conn["nodes"]:
                vs = p["variants"]["nodes"]
                if not vs:
                    continue
                v = vs[0]
                sku = (v.get("sku") or "").strip()
                if not sku.startswith(SKU_PREFIX):
                    continue
                lvl = (v.get("inventoryItem") or {}).get("inventoryLevel")
                qty = None
                if lvl and lvl.get("quantities"):
                    qty = lvl["quantities"][0]["quantity"]
                out[sku[len(SKU_PREFIX):]] = {
                    "product_id": p["id"],
                    "status": p["status"],
                    "title": p["title"],
                    "tags": p.get("tags") or [],
                    "media_ids": [m["id"] for m in (p.get("media") or {}).get("nodes") or []],
                    "mf": {m["key"]: m["value"] for m in (p.get("metafields") or {}).get("nodes") or []},
                    "variant_id": v["id"],
                    "price": float(v["price"]),
                    "inventory_item_id": (v.get("inventoryItem") or {}).get("id"),
                    "qty": qty,
                }
            if not conn["pageInfo"]["hasNextPage"]:
                break
            after = conn["pageInfo"]["endCursor"]
        return out

    def pending_orders(self, since_iso):
        q = """
        query($after: String, $q: String!) {
          orders(first: 50, after: $after, query: $q, sortKey: CREATED_AT) {
            pageInfo { hasNextPage endCursor }
            nodes { id name createdAt cancelledAt displayFinancialStatus
              lineItems(first: 100) { nodes { sku quantity } } }
          }
        }"""
        search = f"created_at:>='{since_iso}' -tag:{ORDER_DONE_TAG}"
        out, after = [], None
        while True:
            d = self.gql(q, {"after": after, "q": search})
            conn = d["orders"]
            out.extend(conn["nodes"])
            if not conn["pageInfo"]["hasNextPage"]:
                break
            after = conn["pageInfo"]["endCursor"]
        return out

    def tag_order(self, order_id):
        d = self.gql(
            "mutation($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { userErrors { message } } }",
            {"id": order_id, "tags": [ORDER_DONE_TAG]},
        )
        errs = d["tagsAdd"]["userErrors"]
        if errs:
            raise RuntimeError(f"tagsAdd: {errs}")

    def create_product(self, item, location_id, publication_id):
        variant = {
            "optionValues": [{"optionName": "Title", "name": "Default Title"}],
            "sku": SKU_PREFIX + item["ct_id"],
            "price": f"{item['price']:.2f}",
            "inventoryPolicy": "DENY",
            "inventoryItem": {"tracked": True},
            "inventoryQuantities": [{"locationId": location_id, "name": "available", "quantity": item["qty"]}],
        }
        inp = {
            "title": item["title"],
            "descriptionHtml": item["description"],
            "vendor": item["game_name"] or "Pentolhaus",
            "productType": "Singola",
            "status": "ACTIVE",
            "tags": item["tags"],
            "productOptions": [{"name": "Title", "values": [{"name": "Default Title"}]}],
            "variants": [variant],
        }
        if item.get("mf"):
            inp["metafields"] = [{"namespace": MF_NS, "key": k, "type": "single_line_text_field", "value": v}
                                 for k, v in item["mf"].items()]
        if item.get("images"):
            inp["files"] = [{"originalSource": u, "contentType": "IMAGE", "alt": item["title"]} for u in item["images"]]
        d = self.gql(
            """mutation($input: ProductSetInput!) {
                 productSet(input: $input, synchronous: true) {
                   product { id }
                   userErrors { field message }
                 } }""",
            {"input": inp},
        )
        res = d["productSet"]
        if res["userErrors"]:
            raise RuntimeError(f"productSet: {res['userErrors']}")
        pid = res["product"]["id"]
        if publication_id:
            p = self.gql(
                """mutation($id: ID!, $pub: ID!) {
                     publishablePublish(id: $id, input: [{publicationId: $pub}]) { userErrors { message } } }""",
                {"id": pid, "pub": publication_id},
            )
            if p["publishablePublish"]["userErrors"]:
                log(f"  ! pubblicazione: {p['publishablePublish']['userErrors']}")
        return pid

    def shop_counts(self):
        """Il riepilogo dei pezzi disponibili salvato sul negozio (per le vetrine del tema)."""
        d = self.gql('{ shop { id metafield(namespace: "pentolhaus", key: "disponibili") { value } } }')
        mf = d["shop"].get("metafield") or {}
        try:
            cur = json.loads(mf.get("value") or "null")
        except ValueError:
            cur = None
        return d["shop"]["id"], cur

    def set_shop_counts(self, shop_id, value):
        d = self.gql(
            """mutation($m: [MetafieldsSetInput!]!) { metafieldsSet(metafields: $m) { userErrors { field message } } }""",
            {"m": [{"ownerId": shop_id, "namespace": "pentolhaus", "key": "disponibili", "type": "json",
                    "value": json.dumps(value, ensure_ascii=False, sort_keys=True)}]},
        )
        if d["metafieldsSet"]["userErrors"]:
            raise RuntimeError(f"metafieldsSet negozio: {d['metafieldsSet']['userErrors']}")

    # --- Schede raggruppate (una scheda per carta, versioni/condizioni/lingue come varianti) ---
    def grouped_products(self, location_id):
        """Mappa chiave gruppo -> scheda raggruppata, con le varianti per SKU."""
        q = """
        query($after: String, $loc: ID!) {
          products(first: 50, after: $after, query: "tag:%s") {
            pageInfo { hasNextPage endCursor }
            nodes {
              id title tags
              gruppo: metafield(namespace: "%s", key: "gruppo") { value }
              media(first: 30) { nodes { id alt } }
              variants(first: 100) { nodes {
                id sku price
                selectedOptions { name value }
                bp: metafield(namespace: "%s", key: "bp") { value }
                inventoryItem { id inventoryLevel(locationId: $loc) { quantities(names: ["available"]) { quantity } } }
              } }
            }
          }
        }""" % (GROUP_TAG, MF_NS, MF_NS)
        out, after = {}, None
        while True:
            d = self.gql(q, {"after": after, "loc": location_id})
            conn = d["products"]
            for p in conn["nodes"]:
                key = (p.get("gruppo") or {}).get("value")
                if not key:
                    continue
                variants = {}
                for v in p["variants"]["nodes"]:
                    lvl = (v.get("inventoryItem") or {}).get("inventoryLevel")
                    qty = lvl["quantities"][0]["quantity"] if lvl and lvl.get("quantities") else None
                    variants[(v.get("sku") or "").strip()] = {
                        "id": v["id"], "price": float(v["price"]), "qty": qty,
                        "inventory_item_id": (v.get("inventoryItem") or {}).get("id"),
                        "bp": (v.get("bp") or {}).get("value") or "",
                        "options": {o["name"]: o["value"] for o in v.get("selectedOptions") or []},
                    }
                out[key] = {"product_id": p["id"], "title": p["title"], "tags": p.get("tags") or [],
                            "media": {(m.get("alt") or ""): m["id"] for m in (p.get("media") or {}).get("nodes") or []},
                            "variants": variants}
            if not conn["pageInfo"]["hasNextPage"]:
                break
            after = conn["pageInfo"]["endCursor"]
        return out

    @staticmethod
    def _variant_input(v, location_id, for_set):
        out = {
            "optionValues": [{"optionName": n, "name": v["options"][n]} for n in GROUP_OPTIONS],
            "price": f"{v['price']:.2f}",
            "inventoryPolicy": "DENY",
            "inventoryItem": {"tracked": True, "sku": v["sku"]} if not for_set else {"tracked": True},
            "metafields": [{"namespace": MF_NS, "key": "bp", "type": "single_line_text_field", "value": v["bp"] or "-"}],
        }
        if for_set:
            out["sku"] = v["sku"]
            out["inventoryQuantities"] = [{"locationId": location_id, "name": "available", "quantity": v["qty"]}]
        else:
            out["inventoryQuantities"] = [{"locationId": location_id, "availableQuantity": v["qty"]}]
        return out

    def create_group(self, g, location_id, publication_id):
        variants, files, seen = [], [], set()
        for v in g["variants"]:
            vi = self._variant_input(v, location_id, True)
            if v.get("image"):
                alt = v["options"]["Versione"]
                if alt not in seen:
                    seen.add(alt)
                    files.append({"originalSource": v["image"], "contentType": "IMAGE", "alt": alt})
                vi["file"] = {"originalSource": v["image"], "contentType": "IMAGE", "alt": alt}
            variants.append(vi)
        opts = []
        for n in GROUP_OPTIONS:
            vals = []
            for v in g["variants"]:
                if v["options"][n] not in vals:
                    vals.append(v["options"][n])
            opts.append({"name": n, "values": [{"name": x} for x in vals]})
        inp = {
            "title": g["title"], "handle": g["handle"], "descriptionHtml": g["description"],
            "vendor": g["game_name"] or "Pentolhaus", "productType": "Singola", "status": "ACTIVE",
            "tags": g["tags"], "productOptions": opts, "variants": variants,
            "metafields": [{"namespace": MF_NS, "key": k, "type": "single_line_text_field", "value": v}
                           for k, v in g["mf"].items()],
        }
        if files:
            inp["files"] = files
        d = self.gql("""mutation($input: ProductSetInput!) {
                 productSet(input: $input, synchronous: true) { product { id } userErrors { field message } } }""",
                     {"input": inp})
        res = d["productSet"]
        if res["userErrors"]:
            raise RuntimeError(f"productSet: {res['userErrors']}")
        pid = res["product"]["id"]
        if publication_id:
            self.gql("""mutation($id: ID!, $pub: ID!) {
                 publishablePublish(id: $id, input: [{publicationId: $pub}]) { userErrors { message } } }""",
                     {"id": pid, "pub": publication_id})
        return pid

    def add_group_variants(self, product_id, new_variants, media_by_alt, location_id):
        media, variants = [], []
        added = set()
        for v in new_variants:
            vi = self._variant_input(v, location_id, False)
            alt = v["options"]["Versione"]
            if media_by_alt.get(alt):
                vi["mediaId"] = media_by_alt[alt]
            elif v.get("image"):
                if alt not in added:
                    media.append({"originalSource": v["image"], "mediaContentType": "IMAGE", "alt": alt})
                    added.add(alt)
                vi["mediaSrc"] = [v["image"]]
            variants.append(vi)
        d = self.gql("""mutation($pid: ID!, $v: [ProductVariantsBulkInput!]!, $m: [CreateMediaInput!]) {
                 productVariantsBulkCreate(productId: $pid, variants: $v, media: $m) { userErrors { field message } } }""",
                     {"pid": product_id, "v": variants, "m": media or None})
        errs = d["productVariantsBulkCreate"]["userErrors"]
        if errs:
            raise RuntimeError(f"productVariantsBulkCreate: {errs}")

    def delete_group_variants(self, product_id, variant_ids):
        d = self.gql("""mutation($pid: ID!, $ids: [ID!]!) {
                 productVariantsBulkDelete(productId: $pid, variantsIds: $ids) { userErrors { field message } } }""",
                     {"pid": product_id, "ids": variant_ids})
        errs = d["productVariantsBulkDelete"]["userErrors"]
        if errs:
            raise RuntimeError(f"productVariantsBulkDelete: {errs}")

    def set_tags(self, product_id, add=None, remove=None):
        if add:
            self.gql("mutation($id: ID!, $t: [String!]!) { tagsAdd(id: $id, tags: $t) { userErrors { message } } }",
                     {"id": product_id, "t": add})
        if remove:
            self.gql("mutation($id: ID!, $t: [String!]!) { tagsRemove(id: $id, tags: $t) { userErrors { message } } }",
                     {"id": product_id, "t": remove})

    def legacy_products(self, game_tag_name):
        """Schede vecchie (una per copia) di un gioco ora raggruppato: id e SKU."""
        q = """query($after: String) { products(first: 100, after: $after, query: "tag:%s tag:%s -tag:%s") {
                 pageInfo { hasNextPage endCursor } nodes { id variants(first: 1) { nodes { sku } } } } }""" % (
            SYNC_TAG, game_tag_name, GROUP_TAG)
        out, after = [], None
        while True:
            d = self.gql(q, {"after": after})
            for p in d["products"]["nodes"]:
                vs = p["variants"]["nodes"]
                out.append((p["id"], (vs[0].get("sku") if vs else "") or ""))
            if not d["products"]["pageInfo"]["hasNextPage"]:
                break
            after = d["products"]["pageInfo"]["endCursor"]
        return out

    def delete_product(self, product_id):
        d = self.gql(
            "mutation($id: ID!) { productDelete(input: {id: $id}) { deletedProductId userErrors { message } } }",
            {"id": product_id},
        )
        if d["productDelete"]["userErrors"]:
            raise RuntimeError(f"productDelete: {d['productDelete']['userErrors']}")

    def set_metafields(self, entries):
        """entries: lista di (product_id, chiave, valore). Max 25 per chiamata."""
        for i in range(0, len(entries), 25):
            chunk = entries[i:i + 25]
            d = self.gql(
                """mutation($m: [MetafieldsSetInput!]!) { metafieldsSet(metafields: $m) { userErrors { field message } } }""",
                {"m": [{"ownerId": pid, "namespace": MF_NS, "key": k, "type": "single_line_text_field", "value": v}
                       for pid, k, v in chunk]},
            )
            if d["metafieldsSet"]["userErrors"]:
                raise RuntimeError(f"metafield: {d['metafieldsSet']['userErrors']}")

    def update_price(self, product_id, variant_id, price):
        d = self.gql(
            """mutation($pid: ID!, $v: [ProductVariantsBulkInput!]!) {
                 productVariantsBulkUpdate(productId: $pid, variants: $v) { userErrors { message } } }""",
            {"pid": product_id, "v": [{"id": variant_id, "price": f"{price:.2f}"}]},
        )
        if d["productVariantsBulkUpdate"]["userErrors"]:
            raise RuntimeError(f"prezzo: {d['productVariantsBulkUpdate']['userErrors']}")

    def replace_images(self, product_id, old_media_ids, urls, alt):
        """Aggiunge le nuove immagini, poi cancella le vecchie e segna il prodotto come HD."""
        d = self.gql(
            """mutation($p: ProductUpdateInput!, $m: [CreateMediaInput!]) {
                 productUpdate(product: $p, media: $m) { userErrors { field message } } }""",
            {"p": {"id": product_id}, "m": [{"originalSource": u, "mediaContentType": "IMAGE", "alt": alt} for u in urls]},
        )
        if d["productUpdate"]["userErrors"]:
            raise RuntimeError(f"immagini: {d['productUpdate']['userErrors']}")
        if old_media_ids:
            d = self.gql(
                "mutation($ids: [ID!]!) { fileDelete(fileIds: $ids) { userErrors { field message } } }",
                {"ids": old_media_ids},
            )
            if d["fileDelete"]["userErrors"]:
                log(f"  ! vecchie immagini non cancellate: {d['fileDelete']['userErrors']}")
        d = self.gql(
            "mutation($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { userErrors { message } } }",
            {"id": product_id, "tags": [HD_TAG]},
        )
        if d["tagsAdd"]["userErrors"]:
            raise RuntimeError(f"tagsAdd: {d['tagsAdd']['userErrors']}")

    def set_quantity(self, inventory_item_id, location_id, qty, current):
        q = {"inventoryItemId": inventory_item_id, "locationId": location_id, "quantity": int(qty)}
        q["changeFromQuantity"] = current if current is not None else None
        d = self.gql(
            """mutation($input: InventorySetQuantitiesInput!) {
                 inventorySetQuantities(input: $input) @idempotent(key: "%s") { userErrors { field message code } } }""" % uuid.uuid4(),
            {"input": {"name": "available", "reason": "correction", "quantities": [q]}},
        )
        errs = d["inventorySetQuantities"]["userErrors"]
        if errs:
            raise RuntimeError(f"quantità: {errs}")


# ---------------------------------------------------------------------------
# Dati CardTrader → prodotto Shopify
# ---------------------------------------------------------------------------

def slug(s):
    out = []
    for ch in (s or "").lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).strip("-")


def game_tag(game_name):
    g = (game_name or "").lower()
    if "magic" in g:
        return "magic"
    if "pok" in g:
        return "pokemon"
    if "riftbound" in g or "league of legends" in g:
        return "riftbound"
    return slug(game_name) or None


def find_rarity(*dicts):
    """La rarità dalle proprietà CardTrader (mtg_rarity, pokemon_rarity, riftbound_rarity…)."""
    for d in dicts:
        for k, v in (d or {}).items():
            if "rarity" in k.lower() and v:
                return str(v)
    return None


def rarity_tier(rarity):
    """'rare' per rare/mitiche/epiche/showcase/secret…, 'uncommon' per le non comuni, altrimenti None."""
    r = (rarity or "").lower()
    if not r:
        return None
    if "uncommon" in r:
        return "uncommon"
    if any(w in r for w in ("rare", "mythic", "epic", "showcase", "legend", "secret")):
        return "rare"
    return None


def pick_props(props):
    props = props or {}
    lang, foil, condition, number = None, False, None, None
    for k, v in props.items():
        kl = k.lower()
        if kl == "condition":
            condition = v
        elif kl.endswith("language") and v:
            lang = str(v)
        elif "foil" in kl and v is True:
            foil = True
        elif kl in ("reverse", "reverse_holo", "pokemon_reverse") and v is True:
            foil = True
        elif kl == "collector_number" and v:
            number = str(v)
    return condition, lang, foil, number


def foil_options(editable):
    """Versioni vendibili di un blueprint: 'both', 'foil' o 'nonfoil' (dalle proprietà modificabili CardTrader)."""
    for prop in editable or []:
        if "foil" in (prop.get("name") or "").lower():
            vals = {str(v).lower() for v in (prop.get("possible_values") or [])}
            has_t, has_f = "true" in vals, "false" in vals
            if has_t and not has_f:
                return "foil"
            if has_f and not has_t:
                return "nonfoil"
            return "both" if has_t else "nonfoil"
    return "nonfoil"


def catalog_key(blueprint_id, foil, options):
    """Chiave di una versione: foil/non foil separate solo se il blueprint le prevede entrambe."""
    if options == "both":
        return f"{blueprint_id}-{'f' if foil else 'n'}"
    return f"{blueprint_id}-x"


def fixed_foil(fixed):
    """Foil fisso dalle proprietà fisse del blueprint (True/False), None se non indicato."""
    for k, v in (fixed or {}).items():
        if "foil" in k.lower() and isinstance(v, bool):
            return v
    return None


class BlueprintCache:
    def __init__(self, path, ct):
        self.path = path
        self.ct = ct
        self.data = {"blueprints": {}, "expansions": {}, "scryfall": {}, "piltover": {}, "piltover_at": 0, "bad_urls": {}}
        if path and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                pass
        self.data.setdefault("blueprints", {})
        self.data.setdefault("expansions", {})
        self.data.setdefault("scryfall", {})
        self.data.setdefault("piltover", {})
        self.data.setdefault("piltover_at", 0)
        self.data.setdefault("piltover_info", {})
        self.data.setdefault("bad_urls", {})
        self.data.setdefault("catalog_at", {})
        self._loaded_exp = set()
        self._lock = threading.Lock()

    def save(self):
        if not self.path:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        snapshot = {"blueprints": self.data["blueprints"].copy(), "expansions": self.data["expansions"].copy(),
                    "scryfall": self.data["scryfall"].copy(), "piltover": self.data["piltover"].copy(),
                    "piltover_at": self.data["piltover_at"], "bad_urls": self.data["bad_urls"].copy(),
                    "piltover_info": self.data["piltover_info"].copy(),
                    "catalog_at": self.data["catalog_at"].copy()}
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)

    def load_expansions(self):
        for e in as_list(self.ct.expansions()):
            self.data["expansions"][str(e["id"])] = {"name": e.get("name"), "code": e.get("code"), "game_id": e.get("game_id")}

    def _load_expansion_blueprints(self, exp_id):
        with self._lock:
            if exp_id in self._loaded_exp:
                return
            self._loaded_exp.add(exp_id)
        for b in as_list(self.ct.blueprints_of_expansion(exp_id)):
            img = b.get("image_url") or ""
            if img.startswith("/"):
                img = "https://www.cardtrader.com" + img
            self.data["blueprints"][str(b["id"])] = {
                "name": b.get("name"),
                "version": b.get("version"),
                "expansion_id": b.get("expansion_id"),
                "game_id": b.get("game_id"),
                "image": img or None,
                "scryfall_id": b.get("scryfall_id") or "",
                "cm_ids": [str(x) for x in (b.get("card_market_ids") or []) if x],
                "rarity": find_rarity(b.get("fixed_properties")) or "",
                "category_id": b.get("category_id"),
                "foil": foil_options(b.get("editable_properties")),
                "fixed_foil": fixed_foil(b.get("fixed_properties")),
            }

    def prefetch(self, blueprint_ids):
        """Recupera in parallelo i blueprint non ancora in cache."""
        missing = sorted({str(b) for b in blueprint_ids if b is not None and str(b) not in self.data["blueprints"]})
        if not missing:
            return
        log(f"  Recupero le schede di {len(missing)} carte nuove (solo la prima volta)…")
        done = [0]
        def work(bid):
            self.get(bid)
            with self._lock:
                done[0] += 1
                if done[0] % 50 == 0:
                    log(f"    … {done[0]}/{len(missing)}")
                    self.save()
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(work, missing))
        log(f"  Schede recuperate: {len(missing)}")

    def refresh_scryfall_ids(self, blueprint_ids):
        """I blueprint salvati prima dell'aggiornamento non hanno lo scryfall_id: ricarico le loro espansioni."""
        exps = set()
        for b in blueprint_ids:
            bp = self.data["blueprints"].get(str(b))
            if bp and ("scryfall_id" not in bp or "cm_ids" not in bp or "rarity" not in bp) and bp.get("expansion_id"):
                exps.add(bp["expansion_id"])
        if not exps:
            return
        log(f"  Aggiorno le schede (ID Scryfall/Cardmarket, rarità) di {len(exps)} espansioni (solo la prima volta)…")
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(self._load_expansion_blueprints, sorted(exps)))

    def scryfall_images(self, scryfall, blueprint_ids):
        """Mappa scryfall_id -> [url HD], scaricando da Scryfall solo quelli non in cache."""
        sids = set()
        for b in blueprint_ids:
            sid = (self.data["blueprints"].get(str(b)) or {}).get("scryfall_id")
            if sid:
                sids.add(sid)
        missing = sorted(s for s in sids if s not in self.data["scryfall"])
        if missing:
            log(f"Scryfall: cerco le immagini HD di {len(missing)} carte…")
            try:
                self.data["scryfall"].update(scryfall.collection(missing))
            except Exception as e:
                log(f"  ! Scryfall non raggiungibile ({e}): per ora tengo le immagini CardTrader")
        return self.data["scryfall"]

    def piltover_images(self, piltover, need_ids):
        """Mappa id Cardmarket -> url Piltover Archive; riscarica se vecchia di 7 giorni o se mancano carte (max 1 volta ogni 6 ore)."""
        age = time.time() - float(self.data.get("piltover_at") or 0)
        missing = [i for i in need_ids if i not in self.data["piltover"]]
        if age > 7 * 86400 or (missing and age > 6 * 3600) or not self.data.get("piltover_info"):
            log(f"Piltover Archive: scarico l'elenco delle carte Riftbound…")
            try:
                self.data["piltover"], self.data["piltover_info"] = piltover.all_cards()
                self.data["piltover_at"] = time.time()
                log(f"  {len(self.data['piltover'])} stampe trovate")
            except Exception as e:
                log(f"  ! Piltover Archive non raggiungibile ({e}): uso le immagini originali di CardTrader")
        return self.data["piltover"]

    def get(self, blueprint_id):
        key = str(blueprint_id)
        if key in self.data["blueprints"]:
            return self.data["blueprints"][key]
        # Il blueprint non dice l'espansione nell'export prodotti: la ricavo dal marketplace
        try:
            mk = self.ct.marketplace_by_blueprint(blueprint_id)
        except Exception as e:
            log(f"  ! blueprint {blueprint_id}: {e}")
            return None
        offers = mk.get(key) if isinstance(mk, dict) else None
        if not offers and isinstance(mk, dict) and mk:
            offers = next(iter(mk.values()))
        exp_id = None
        for o in offers or []:
            exp = o.get("expansion") or {}
            if exp.get("id"):
                exp_id = exp["id"]
                self.data["expansions"].setdefault(str(exp_id), {"name": exp.get("name_en"), "code": exp.get("code"), "game_id": o.get("game_id")})
                break
        if exp_id:
            self._load_expansion_blueprints(exp_id)
        return self.data["blueprints"].get(key)


def pick_images(bp, gt, scry, pa, hd_on):
    """Sceglie l'immagine migliore: Scryfall (Magic), Piltover Archive (Riftbound), originale CardTrader (resto)."""
    preview = [bp["image"]] if bp.get("image") else []
    out = {"images": preview, "fallback": preview, "hd": False, "trusted": False}
    if not hd_on:
        return out
    sf = scry.get(bp.get("scryfall_id") or "")
    if sf:
        return dict(out, images=sf, hd=True, trusted=True)
    if gt == "riftbound":
        for cm in bp.get("cm_ids") or []:
            if pa.get(cm):
                return dict(out, images=[pa[cm]], hd=True)
    full = ct_full_image(bp.get("image"))
    if full:
        return dict(out, images=[full], hd=True)
    return out


def round_price(price):
    """Prezzo "tondo", al valore più vicino: 3,86→3,90 · 33,25→33,50 · 599,11→599,00."""
    from decimal import Decimal, ROUND_HALF_UP
    step = Decimal("0.10") if price < 10 else (Decimal("0.50") if price < 100 else Decimal("1"))
    units = (Decimal(str(round(price, 4))) / step).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(units * step)


def build_items(ct, cache, markup, min_price, only_singles, scryfall=None, piltover=None, rounding=True, min_by_tier=None):
    games = {g["id"]: (g.get("display_name") or g.get("name")) for g in as_list(ct.games())}
    single_cats = set()
    try:
        for c in as_list(ct.categories()):
            if "single" in (c.get("name") or "").lower():
                single_cats.add(c["id"])
    except Exception as e:
        log(f"  ! categorie non lette ({e}): sincronizzo tutto")
        only_singles = False

    log("CardTrader: scarico l'inventario…")
    products = as_list(ct.products())
    log(f"CardTrader: {len(products)} articoli in totale")
    wanted = [p for p in products if not only_singles or p.get("category_id") in single_cats]
    live_bps = [p.get("blueprint_id") for p in wanted if int(p.get("quantity") or 0) > 0]
    cache.prefetch(live_bps)
    hd, pa = {}, {}
    cache.refresh_scryfall_ids(live_bps)
    if scryfall is not None:
        hd = cache.scryfall_images(scryfall, live_bps)
    if piltover is not None:
        need = set()
        for p in wanted:
            bp = cache.data["blueprints"].get(str(p.get("blueprint_id"))) or {}
            if int(p.get("quantity") or 0) > 0 and game_tag(games.get(p.get("game_id")) or games.get(bp.get("game_id"))) == "riftbound":
                need.update(bp.get("cm_ids") or [])
        if need:
            pa = cache.piltover_images(piltover, need)

    items = {}
    skipped = 0
    for p in products:
        if only_singles and p.get("category_id") not in single_cats:
            skipped += 1
            continue
        qty = int(p.get("quantity") or 0)
        ct_id = str(p["id"])
        bp = cache.data["blueprints"].get(str(p.get("blueprint_id"))) or (cache.get(p.get("blueprint_id")) if qty > 0 else None) or {}
        exp = cache.data["expansions"].get(str(bp.get("expansion_id"))) or {}
        name = p.get("name_en") or bp.get("name") or f"Carta {p.get('blueprint_id')}"
        game_name = games.get(p.get("game_id")) or games.get(bp.get("game_id"))
        condition, lang, foil, number = pick_props(p.get("properties_hash"))

        cond_short = CONDITION_SHORT.get((condition or "").lower(), condition)
        parts = [name]
        if exp.get("name"):
            parts.append(exp["name"])
        details = [x for x in (cond_short, (lang or "").upper() or None, "Foil" if foil else None) if x]
        title = " · ".join(parts)
        if details:
            title += " (" + ", ".join(details) + ")"

        cents = p.get("price_cents")
        if isinstance(cents, dict):
            cents = cents.get("cents")
        price = (float(cents or 0) / 100.0) * (1 + markup / 100.0)
        price = round_price(price) if rounding else round(price, 2)
        rarity = find_rarity(p.get("properties_hash"), {"rarity": bp.get("rarity")})
        tier = rarity_tier(rarity)
        price = max(price, min_price, (min_by_tier or {}).get(tier, 0))

        tags = [SYNC_TAG, "singola" if p.get("category_id") in single_cats else "cardtrader-altro"]
        gt = game_tag(game_name)
        if gt:
            tags.append(gt)
        if exp.get("code"):
            tags.append("espansione-" + slug(exp["code"]))
        if lang:
            tags.append("lingua-" + slug(lang))
        if foil:
            tags.append("foil")
        if cond_short:
            tags.append("condizione-" + slug(cond_short))

        rows = [
            ("Gioco", game_name),
            ("Espansione", f"{exp.get('name')} ({(exp.get('code') or '').upper()})" if exp.get("name") else None),
            ("Numero", number),
            ("Condizione", condition),
            ("Lingua", (lang or "").upper() or None),
            ("Foil", "Sì" if foil else "No"),
        ]
        desc = "<ul>" + "".join(f"<li><strong>{k}:</strong> {v}</li>" for k, v in rows if v) + "</ul>"
        if p.get("description"):
            desc += f"<p>{p['description']}</p>"
        desc += "<p>Controllata da me, spedita in bustina + toploader.</p>"

        bp_key = catalog_key(p.get("blueprint_id"), foil, bp.get("foil")) if p.get("blueprint_id") else ""
        items[ct_id] = {
            "ct_id": ct_id,
            "bp_key": bp_key,
            "bp_id": str(p.get("blueprint_id") or ""),
            "name": name,
            "foil": foil,
            "cond_short": cond_short or "",
            "lang": lang or "",
            "title": title[:255],
            "qty": max(qty, 0),
            "price": price,
            "tags": tags,
            "game_name": game_name,
            "description": desc,
            "mf": {k: v for k, v in {
                "espansione": exp.get("name") or "",
                "rarita": RARITY_IT.get((rarity or "").lower(), rarity or ""),
                "lingua": LANG_IT.get((lang or "").lower(), (lang or "").upper()),
                "condizione": condition or "",
                "foil": "Sì" if foil else "No",
                "bp": bp_key,
            }.items() if v},
            **pick_images(bp, gt, hd, pa, scryfall is not None),
        }
    if skipped:
        log(f"  (ignorati {skipped} articoli non singoli)")
    return items


def build_catalog(ct, cache, game_keyword, piltover=None, refresh_hours=24):
    """Tutte le carte singole di un gioco (es. 'riftbound'), una voce per versione foil/non foil.
    Restituisce {chiave "blueprint-f|n": item}: segnaposto esauriti e senza prezzo."""
    games = {g["id"]: (g.get("display_name") or g.get("name")) for g in as_list(ct.games())}
    game_ids = {gid for gid, gname in games.items() if game_tag(gname) == game_keyword}
    if not game_ids:
        log(f"  ! catalogo: gioco '{game_keyword}' non trovato su CardTrader")
        return {}
    single_cats = {c["id"] for c in as_list(ct.categories())
                   if c.get("game_id") in game_ids and "single" in (c.get("name") or "").lower()}
    if not any(int(e.get("game_id") or 0) in game_ids for e in cache.data["expansions"].values()):
        cache.load_expansions()
    exp_ids = sorted(eid for eid, e in cache.data["expansions"].items() if int(e.get("game_id") or 0) in game_ids)
    last = float(cache.data["catalog_at"].get(game_keyword) or 0)
    stale = any("fixed_foil" not in bp for bp in cache.data["blueprints"].values()
                if int(bp.get("game_id") or 0) in game_ids)
    if stale or time.time() - last > refresh_hours * 3600:
        log(f"Catalogo {game_keyword}: aggiorno le schede di {len(exp_ids)} espansioni…")
        cache.load_expansions()
        exp_ids = sorted(eid for eid, e in cache.data["expansions"].items() if int(e.get("game_id") or 0) in game_ids)
        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(cache._load_expansion_blueprints, [int(x) for x in exp_ids]))
        cache.data["catalog_at"][game_keyword] = time.time()
    exp_set = {int(x) for x in exp_ids}
    bps = {bid: bp for bid, bp in cache.data["blueprints"].items()
           if int(bp.get("expansion_id") or 0) in exp_set and int(bp.get("game_id") or 0) in game_ids
           and (not single_cats or bp.get("category_id") in single_cats)}
    pa = {}
    if piltover is not None and game_keyword == "riftbound":
        need = {cm for bp in bps.values() for cm in (bp.get("cm_ids") or [])}
        pa = cache.piltover_images(piltover, need) if need else {}
    game_name = next(iter(games[g] for g in game_ids))
    out = {}
    for bid, bp in bps.items():
        exp = cache.data["expansions"].get(str(bp.get("expansion_id"))) or {}
        name = bp.get("name") or f"Carta {bid}"
        if bp.get("version"):
            name += f" ({bp['version']})"
        opts = bp.get("foil") or "nonfoil"
        rarity = bp.get("rarity") or ""
        if opts == "both":
            versions = [False, True]
        elif opts == "foil":
            versions = [True]
        else:
            versions = [bp.get("fixed_foil")]  # una sola versione: foil fisso, non foil o non indicato (None)
        for foil in versions:
            key = catalog_key(bid, foil, opts)
            title = name + (" · " + exp["name"] if exp.get("name") else "") + (" (Foil)" if foil else "")
            tags = [SYNC_TAG, CATALOG_TAG, "singola", game_keyword]
            if exp.get("code"):
                tags.append("espansione-" + slug(exp["code"]))
            if foil:
                tags.append("foil")
            rows = [("Gioco", game_name),
                    ("Espansione", f"{exp.get('name')} ({(exp.get('code') or '').upper()})" if exp.get("name") else None),
                    ("Foil", None if foil is None else ("Sì" if foil else "No"))]
            desc = ("<ul>" + "".join(f"<li><strong>{k}:</strong> {v}</li>" for k, v in rows if v) + "</ul>"
                    "<p>Al momento non ce l'ho in vetrina. Scrivimi se la cerchi: te la trovo io.</p>")
            imgs = pick_images(bp, game_keyword, {}, pa, True)
            if imgs["hd"] and pa and imgs["images"][0] in pa.values():
                imgs["trusted"] = True
            out[key] = {
                "ct_id": CATALOG_SKU + key,
                "bp_key": key,
                "bp_id": str(bid),
                "foil": foil,
                "name": bp.get("name") or f"Carta {bid}",
                "title": title[:255],
                "qty": 0,
                "price": 0.0,
                "tags": tags,
                "game_name": game_name,
                "description": desc,
                "mf": {k: v for k, v in {
                    "espansione": exp.get("name") or "",
                    "rarita": RARITY_IT.get(rarity.lower(), rarity),
                    "foil": "" if foil is None else ("Sì" if foil else "No"),
                    "bp": key,
                }.items() if v},
                **imgs,
            }
    return out


def version_info(bp, cache):
    """(numero di collezione, nome della versione) di un blueprint, da Piltover Archive."""
    info = cache.data.get("piltover_info") or {}
    num, label = "", ""
    for cm in bp.get("cm_ids") or []:
        if cm in info:
            num, label = info[cm][0] or "", info[cm][1] or ""
            break
    if not label:
        label = bp.get("version") or ""
    return num, VERSION_IT.get(label.lower(), label) or "Normale"


def num_sort_key(num):
    import re
    m = re.match(r"([A-Za-z-]*?)-?(\d+)(.*)", num or "")
    return (m.group(1), int(m.group(2)), m.group(3)) if m else ("~", 10 ** 6, num or "")


def build_groups(game_keyword, real_items, catalog, existing, cache):
    """Le schede raggruppate desiderate: {chiave gruppo: scheda con varianti}.
    real_items: copie vere da CardTrader (qty > 0); catalog: tutte le versioni del gioco;
    existing: schede raggruppate già su Shopify (per tenere le copie vendute a 0)."""
    bps = cache.data["blueprints"]
    groups = {}

    def group_for(bp_id):
        bp = bps.get(str(bp_id)) or {}
        exp = cache.data["expansions"].get(str(bp.get("expansion_id"))) or {}
        name = bp.get("name") or f"Carta {bp_id}"
        key = f"{bp.get('expansion_id')}:{name.lower()}"
        g = groups.get(key)
        if g is None:
            g = groups[key] = {"key": key, "name": name, "exp": exp, "bps": {}, "variants": [], "rarities": set()}
        g["bps"][str(bp_id)] = bp
        if bp.get("rarity"):
            g["rarities"].add(bp["rarity"])
        return g

    cat_by_key = dict(catalog)
    real_keys = set()
    for it in real_items:
        if not it.get("bp_id"):
            continue
        g = group_for(it["bp_id"])
        cimg = (cat_by_key.get(it["bp_key"]) or {}).get("images") or []
        g["variants"].append({"sku": SKU_PREFIX + it["ct_id"], "bp": it["bp_key"], "bp_id": it["bp_id"], "foil": it["foil"],
                              "cond": it.get("cond_short") or "—",
                              "lang": LANG_IT.get((it.get("lang") or "").lower(), (it.get("lang") or "").upper()) or "—",
                              "price": it["price"], "qty": it["qty"], "image": (cimg or it.get("images") or [None])[0],
                              "real": True})
        real_keys.add(it["bp_key"])
    have = {v["sku"] for g in groups.values() for v in g["variants"]}
    for ex in existing.values():
        for sku, v in ex["variants"].items():
            if sku.startswith(SKU_PREFIX) and not sku[len(SKU_PREFIX):].startswith(CATALOG_SKU) and sku not in have:
                bp_id = (v.get("bp") or "").split("-")[0]
                if not bp_id or bp_id not in bps:
                    continue
                g = group_for(bp_id)
                o = v["options"]
                cimg = (cat_by_key.get(v["bp"]) or {}).get("images") or []
                g["variants"].append({"sku": sku, "bp": v["bp"], "bp_id": bp_id, "foil": o.get("Versione", "").endswith(" Foil"),
                                      "cond": o.get("Condizione", "—"), "lang": o.get("Lingua", "—"), "price": v["price"],
                                      "qty": 0, "image": (cimg or [None])[0], "real": True, "keep_label": o.get("Versione")})
                real_keys.add(v["bp"])
    for key, it in catalog.items():
        if key in real_keys:
            continue
        g = group_for(it["bp_id"])
        g["variants"].append({"sku": SKU_PREFIX + CATALOG_SKU + key, "bp": key, "bp_id": it["bp_id"], "foil": it["foil"],
                              "cond": "—", "lang": "—", "price": 0.0, "qty": 0,
                              "image": (it.get("images") or [None])[0], "real": False})

    out = {}
    for key, g in groups.items():
        info = {bid: version_info(bp, cache) for bid, bp in g["bps"].items()}
        labels = {}
        for bid, (num, label) in info.items():
            labels.setdefault(label, []).append(bid)
        def vlabel(bid, foil):
            num, label = info[bid]
            if len(labels.get(label, [])) > 1 and num:
                label = f"{label} {num}"
            return label + (" Foil" if foil else "")
        nums = sorted((n for n, _ in info.values() if n), key=num_sort_key)
        base = next((info[b][0] for b in sorted(info, key=lambda b: num_sort_key(info[b][0]))
                     if info[b][1] == "Normale" and info[b][0]), nums[0] if nums else "")
        seen = set()
        variants = []
        order = sorted(g["variants"], key=lambda v: (num_sort_key(info[v["bp_id"]][0]), bool(v["foil"]), not v["real"], v["cond"], v["lang"]))
        for v in order:
            label = v.get("keep_label") or vlabel(v["bp_id"], v["foil"])
            cond = v["cond"]
            n = 2
            while (label, cond, v["lang"]) in seen:
                cond = f"{v['cond']} ·{n}"
                n += 1
            seen.add((label, cond, v["lang"]))
            variants.append(dict(v, options={"Versione": label, "Condizione": cond, "Lingua": v["lang"]}))
        exp = g["exp"]
        title = (f"{base} · " if base else "") + g["name"]
        rarity = sorted(g["rarities"])[0] if g["rarities"] else ""
        tags = [SYNC_TAG, GROUP_TAG, "singola", game_keyword]
        if exp.get("code"):
            tags.append("espansione-" + slug(exp["code"]))
        rows = [("Espansione", f"{exp.get('name')} ({(exp.get('code') or '').upper()})" if exp.get("name") else None),
                ("Numero", base or None)]
        desc = ("<ul>" + "".join(f"<li><strong>{k}:</strong> {v}</li>" for k, v in rows if v) + "</ul>"
                "<p>Scegli la versione qui sopra. Ogni copia è controllata da me e spedita in bustina + toploader. "
                "Se una versione è esaurita scrivimi: te la trovo io.</p>")
        out[key] = {
            "key": key, "title": title[:255], "handle": slug(f"{game_keyword} {exp.get('code') or ''} {title}")[:250],
            "description": desc, "tags": tags, "game_name": None, "sort": (exp.get("code") or "", num_sort_key(base)),
            "mf": {k: v for k, v in {"gruppo": key, "espansione": exp.get("name") or "",
                                     "rarita": RARITY_IT.get(rarity.lower(), rarity), "numero": base}.items() if v},
            "variants": variants,
        }
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ct_token = os.environ.get("CARDTRADER_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP", "").strip()
    if not ct_token or not shop:
        raise SystemExit("Mancano CARDTRADER_TOKEN o SHOPIFY_SHOP")
    markup = env_float("PRICE_MARKUP_PERCENT", 0.0)
    min_price = env_float("MIN_PRICE", 0.10)
    rounding = env_bool("ROUND_PRICES", True)
    min_by_tier = {"uncommon": env_float("MIN_PRICE_UNCOMMON", 0.50), "rare": env_float("MIN_PRICE_RARE", 1.00)}
    only_singles = env_bool("ONLY_SINGLES", True)
    since = os.environ.get("ORDERS_SINCE", "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cache_path = os.environ.get("CACHE_FILE", ".cache/blueprints.json")
    max_create = int(env_float("MAX_CREATE", 300))
    hd_images = env_bool("HD_IMAGES", True)
    max_img = int(env_float("MAX_IMAGE_UPDATES", 300))
    catalog_games = [g.strip().lower() for g in os.environ.get("CATALOG_GAMES", "riftbound").split(",") if g.strip()]
    max_catalog = int(env_float("MAX_CATALOG_CREATE", 300))
    grouped_games = [g.strip().lower() for g in os.environ.get("GROUPED_GAMES", "riftbound").split(",") if g.strip()]
    catalog_games = [g for g in catalog_games if g not in grouped_games]
    max_legacy_delete = int(env_float("MAX_LEGACY_DELETE", 600))

    log(f"=== Sync CardTrader → Shopify {'(PROVA: nessuna modifica)' if DRY_RUN else '(MODIFICHE ATTIVE)'} ===")
    ct = CardTrader(ct_token)
    me = ct.info()
    log(f"CardTrader: collegato come {me.get('name') or me.get('username') or me.get('id')}")
    sh = Shopify(shop, os.environ.get("SHOPIFY_ACCESS_TOKEN"), os.environ.get("SHOPIFY_CLIENT_ID"), os.environ.get("SHOPIFY_CLIENT_SECRET"))
    location_id = sh.primary_location()
    publication_id = sh.online_store_publication()
    log(f"Shopify: collegato a {sh.domain}")

    # 1) Ordini Shopify → scala CardTrader
    orders = sh.pending_orders(since)
    log(f"Shopify: {len(orders)} ordini da controllare")
    done_orders = 0
    for o in orders:
        if o.get("cancelledAt"):
            if not DRY_RUN:
                sh.tag_order(o["id"])
            continue
        if (o.get("displayFinancialStatus") or "").upper() not in ("PAID", "PARTIALLY_REFUNDED", "PARTIALLY_PAID", "AUTHORIZED"):
            continue
        lines = [(li["sku"][len(SKU_PREFIX):], li["quantity"]) for li in o["lineItems"]["nodes"]
                 if (li.get("sku") or "").startswith(SKU_PREFIX)
                 and not li["sku"][len(SKU_PREFIX):].startswith(CATALOG_SKU)]
        if not lines:
            if not DRY_RUN:
                sh.tag_order(o["id"])
            continue
        for ct_id, q in lines:
            log(f"Ordine Shopify: -{q} su CardTrader per articolo {ct_id}")
            if not DRY_RUN:
                try:
                    ct.increment(ct_id, -int(q))
                except RuntimeError as e:
                    # 404 = articolo già esaurito/cancellato su CardTrader: niente da scalare
                    if "HTTP 404" not in str(e):
                        raise
                    log(f"  (articolo {ct_id} non più presente su CardTrader)")
        if not DRY_RUN:
            sh.tag_order(o["id"])
        done_orders += 1

    # 2) Inventario CardTrader → Shopify
    cache = BlueprintCache(cache_path, ct)
    if not cache.data["expansions"]:
        log("CardTrader: scarico l'elenco delle espansioni…")
        cache.load_expansions()
    items = build_items(ct, cache, markup, min_price, only_singles,
                        Scryfall() if hd_images else None, PiltoverArchive() if hd_images else None, rounding, min_by_tier)

    def hd_available(it):
        """Controlla (una volta sola) che l'immagine HD esista davvero."""
        if not it["hd"]:
            return False
        if it["trusted"]:
            return True
        u = it["images"][0]
        if u in cache.data["bad_urls"]:
            return False
        if url_ok(u):
            return True
        cache.data["bad_urls"][u] = 1
        log(f"  (immagine HD non disponibile, tengo quella CardTrader: {u})")
        return False
    cache.save()
    all_items = items
    grouped_items = {g: [it for it in items.values() if g in it["tags"]] for g in grouped_games}
    items = {k: it for k, it in items.items() if not any(g in it["tags"] for g in grouped_games)}
    existing = sh.synced_products(location_id)
    placeholders = {k: v for k, v in existing.items() if k.startswith(CATALOG_SKU)}
    existing = {k: v for k, v in existing.items() if not k.startswith(CATALOG_SKU)}
    log(f"CardTrader: {len(items)} articoli · Shopify: {len(existing)} prodotti già sincronizzati"
        + (f" · {len(placeholders)} carte di catalogo" if placeholders else ""))

    created = updated_qty = updated_price = zeroed = errors = pending_new = 0
    img_done = img_pending = 0
    mf_todo = []
    for ct_id, it in items.items():
        try:
            ex = existing.get(ct_id)
            if not ex:
                if it["qty"] <= 0:
                    continue
                if created >= max_create:
                    pending_new += 1
                    continue
                log(f"+ Nuovo: {it['title']} — {it['qty']} pz a € {it['price']:.2f}")
                if not DRY_RUN:
                    if hd_available(it):
                        it = dict(it, tags=it["tags"] + [HD_TAG])
                    else:
                        it = dict(it, images=it["fallback"])
                    sh.create_product(it, location_id, publication_id)
                created += 1
                continue
            if abs(ex["price"] - it["price"]) >= 0.005:
                log(f"€ Prezzo: {it['title']}: {ex['price']:.2f} → {it['price']:.2f}")
                if not DRY_RUN:
                    sh.update_price(ex["product_id"], ex["variant_id"], it["price"])
                updated_price += 1
            if ex["qty"] != it["qty"]:
                log(f"# Quantità: {it['title']}: {ex['qty']} → {it['qty']}")
                if not DRY_RUN:
                    sh.set_quantity(ex["inventory_item_id"], location_id, it["qty"], ex["qty"])
                updated_qty += 1
            for k, v in it["mf"].items():
                if ex["mf"].get(k) != v:
                    mf_todo.append((ex["product_id"], k, v))
            if it["hd"] and HD_TAG not in ex["tags"] and it["images"][0] not in cache.data["bad_urls"]:
                if img_done >= max_img:
                    img_pending += 1
                elif hd_available(it):
                    log(f"🖼 Immagine HD: {it['title']}")
                    if not DRY_RUN:
                        sh.replace_images(ex["product_id"], ex["media_ids"], it["images"], it["title"])
                    img_done += 1
        except Exception as e:
            errors += 1
            log(f"! Errore su articolo {ct_id}: {e}")

    if mf_todo:
        n_prod = len({x[0] for x in mf_todo})
        log(f"🏷 Filtri (espansione, rarità, lingua…): aggiorno {n_prod} prodotti")
        if not DRY_RUN:
            try:
                sh.set_metafields(mf_todo)
            except Exception as e:
                errors += 1
                log(f"! Errore sui filtri: {e}")

    for ct_id, ex in existing.items():
        if ct_id in items or not ex["qty"] or any(g in ex["tags"] for g in grouped_games):
            continue
        try:
            log(f"0 Esaurito su CardTrader: {ex['title']} ({ex['qty']} → 0)")
            if not DRY_RUN:
                sh.set_quantity(ex["inventory_item_id"], location_id, 0, ex["qty"])
            zeroed += 1
        except Exception as e:
            errors += 1
            log(f"! Errore azzerando {ct_id}: {e}")

    # Riepilogo dei pezzi disponibili per gioco ed espansione (il tema lo usa nelle vetrine)
    counts = {"giochi": {}, "espansioni": {}}
    for it in all_items.values():
        if it["qty"] <= 0:
            continue
        g = next((t for t in it["tags"] if t in ("magic", "pokemon", "riftbound")), None) or game_tag(it["game_name"]) or "altro"
        counts["giochi"][g] = counts["giochi"].get(g, 0) + 1
        e = it["mf"].get("espansione")
        if e:
            counts["espansioni"].setdefault(g, {})
            counts["espansioni"][g][e] = counts["espansioni"][g].get(e, 0) + 1
    try:
        shop_id, cur_counts = sh.shop_counts()
        if cur_counts != counts:
            log(f"Vetrine: pezzi disponibili {counts['giochi']}")
            if not DRY_RUN:
                sh.set_shop_counts(shop_id, counts)
    except Exception as e:
        errors += 1
        log(f"! Errore sul riepilogo dei pezzi: {e}")

    # 3) Catalogo completo: le carte che non ho mai avuto, esaurite e senza prezzo
    cat_created = cat_removed = cat_pending = 0
    if catalog_games:
        real_keys = {it["bp_key"] for it in items.values() if it.get("bp_key")}
        real_keys |= {ex["mf"].get("bp") for ex in existing.values() if ex["mf"].get("bp")}
        for ph_sku, ph in placeholders.items():
            if ph_sku[len(CATALOG_SKU):] in real_keys:
                try:
                    log(f"- Catalogo → vera: tolgo il segnaposto di {ph['title']}")
                    if not DRY_RUN:
                        sh.delete_product(ph["product_id"])
                    cat_removed += 1
                except Exception as e:
                    errors += 1
                    log(f"! Errore togliendo il segnaposto {ph_sku}: {e}")
        for g in catalog_games:
            try:
                catalog = build_catalog(ct, cache, g, PiltoverArchive() if hd_images else None)
            except Exception as e:
                errors += 1
                log(f"! Catalogo {g}: {e}")
                continue
            todo = [it for key, it in sorted(catalog.items())
                    if key not in real_keys and (CATALOG_SKU + key) not in placeholders]
            log(f"Catalogo {g}: {len(catalog)} versioni · {len(set(catalog) & real_keys)} già in vetrina · {len(todo)} da aggiungere")
            for it in todo:
                if cat_created >= max_catalog:
                    cat_pending += 1
                    continue
                try:
                    if not DRY_RUN:
                        if hd_available(it):
                            it = dict(it, tags=it["tags"] + [HD_TAG])
                        else:
                            it = dict(it, images=it["fallback"])
                        sh.create_product(it, location_id, publication_id)
                    cat_created += 1
                except Exception as e:
                    errors += 1
                    log(f"! Errore sul catalogo {it['title']}: {e}")
        if cat_created:
            log(f"📚 Catalogo: aggiunte {cat_created} carte esaurite")

    # 4) Giochi raggruppati: una scheda per carta, con versioni, condizioni e lingue come varianti
    g_created = g_changed = g_deleted = g_pending = 0
    for g in grouped_games:
        try:
            catalog = build_catalog(ct, cache, g, PiltoverArchive() if hd_images else None)
            ex_groups = sh.grouped_products(location_id)
            groups = build_groups(g, grouped_items.get(g, []), catalog, ex_groups, cache)
        except Exception as e:
            errors += 1
            log(f"! Schede raggruppate {g}: {e}")
            continue
        log(f"Schede {g}: {len(groups)} carte · {sum(len(x['variants']) for x in groups.values())} versioni/copie · "
            f"{len(ex_groups)} già su Shopify")
        for key in sorted(groups, key=lambda k: groups[k]["sort"]):
            want = groups[key]
            have = ex_groups.get(key)
            try:
                if not have:
                    if g_created >= max_catalog:
                        g_pending += 1
                        continue
                    if not DRY_RUN:
                        sh.create_group(dict(want, game_name=next((it["game_name"] for it in catalog.values()), None)),
                                        location_id, publication_id)
                    g_created += 1
                    continue
                want_v = {v["sku"]: v for v in want["variants"]}
                new = [v for sku, v in want_v.items() if sku not in have["variants"]]
                gone = [x["id"] for sku, x in have["variants"].items() if sku not in want_v]
                touched = False
                if new:
                    log(f"+ {want['title']}: {len(new)} versioni/copie nuove")
                    if not DRY_RUN:
                        sh.add_group_variants(have["product_id"], new, have["media"], location_id)
                    touched = True
                if gone:
                    if not DRY_RUN:
                        sh.delete_group_variants(have["product_id"], gone)
                    touched = True
                for sku, v in want_v.items():
                    hv = have["variants"].get(sku)
                    if not hv:
                        continue
                    if v["real"] and abs(hv["price"] - v["price"]) >= 0.005:
                        log(f"€ Prezzo: {want['title']} ({v['options']['Versione']}): {hv['price']:.2f} → {v['price']:.2f}")
                        if not DRY_RUN:
                            sh.update_price(have["product_id"], hv["id"], v["price"])
                        touched = True
                    if hv["qty"] != v["qty"]:
                        log(f"# Quantità: {want['title']} ({v['options']['Versione']}): {hv['qty']} → {v['qty']}")
                        if not DRY_RUN:
                            sh.set_quantity(hv["inventory_item_id"], location_id, v["qty"], hv["qty"])
                        touched = True
                g_changed += int(touched)
            except Exception as e:
                errors += 1
                log(f"! Errore sulla scheda {want['title']}: {e}")
        if g_created:
            log(f"🃏 Schede {g}: create {g_created} carte raggruppate")
        # Le vecchie schede (una per copia) spariscono quando la loro copia è già nella scheda raggruppata
        if not g_pending:
            try:
                in_groups = {v["sku"] for x in groups.values() for v in x["variants"]}
                if not DRY_RUN:
                    in_groups |= {sku for x in sh.grouped_products(location_id).values() for sku in x["variants"]}
                legacy = sh.legacy_products(g)
                sold_out = {SKU_PREFIX + cid for cid, ex in existing.items() if not ex["qty"] and g in ex["tags"]}
                todo = [(pid, sku) for pid, sku in legacy
                        if sku in in_groups or sku in sold_out or sku[len(SKU_PREFIX):].startswith(CATALOG_SKU)]
                if todo:
                    log(f"- Schede {g}: tolgo {min(len(todo), max_legacy_delete)} vecchie schede singole (su {len(todo)})")
                for pid, sku in todo[:max_legacy_delete]:
                    if not DRY_RUN:
                        sh.delete_product(pid)
                    g_deleted += 1
            except Exception as e:
                errors += 1
                log(f"! Errore togliendo le vecchie schede {g}: {e}")

    log(
        f"=== Fatto: {done_orders} ordini Shopify riportati su CardTrader · {created} nuovi · "
        f"{updated_qty} quantità · {updated_price} prezzi · {img_done} immagini HD · "
        f"{len({x[0] for x in mf_todo})} filtri · {zeroed} esauriti · {cat_created} catalogo · {errors} errori ==="
    )
    if cat_pending:
        log(f"(altre {cat_pending} carte di catalogo verranno aggiunte nei prossimi giri, {max_catalog} alla volta)")
    if g_created or g_changed or g_deleted or g_pending:
        log(f"Schede raggruppate: {g_created} create · {g_changed} aggiornate · {g_deleted} vecchie tolte"
            + (f" · altre {g_pending} nei prossimi giri" if g_pending else ""))
    if pending_new:
        log(f"(altre {pending_new} carte nuove verranno create nei prossimi giri, {max_create} alla volta)")
    cache.save()
    if img_pending:
        log(f"(altre {img_pending} immagini HD verranno sostituite nei prossimi giri, {max_img} alla volta)")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
