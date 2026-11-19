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
  ORDERS_SINCE            data ISO: considera solo gli ordini Shopify da questa data (default: oggi)
  ONLY_SINGLES            "true" = sincronizza solo le carte singole (default true)
  CACHE_FILE              file cache dei blueprint (default .cache/blueprints.json)
"""

import json
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
          products(first: 100, after: $after, query: "tag:%s") {
            pageInfo { hasNextPage endCursor }
            nodes {
              id title status
              variants(first: 1) { nodes {
                id sku price
                inventoryItem { id inventoryLevel(locationId: $loc) { quantities(names: ["available"]) { quantity } } }
              } }
            }
          }
        }""" % SYNC_TAG
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
        if item.get("image"):
            inp["files"] = [{"originalSource": item["image"], "contentType": "IMAGE", "alt": item["title"]}]
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

    def update_price(self, product_id, variant_id, price):
        d = self.gql(
            """mutation($pid: ID!, $v: [ProductVariantsBulkInput!]!) {
                 productVariantsBulkUpdate(productId: $pid, variants: $v) { userErrors { message } } }""",
            {"pid": product_id, "v": [{"id": variant_id, "price": f"{price:.2f}"}]},
        )
        if d["productVariantsBulkUpdate"]["userErrors"]:
            raise RuntimeError(f"prezzo: {d['productVariantsBulkUpdate']['userErrors']}")

    def set_quantity(self, inventory_item_id, location_id, qty, current):
        q = {"inventoryItemId": inventory_item_id, "locationId": location_id, "quantity": int(qty)}
        q["changeFromQuantity"] = current if current is not None else None
        d = self.gql(
            """mutation($input: InventorySetQuantitiesInput!) {
                 inventorySetQuantities(input: $input) { userErrors { field message code } } }""",
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


class BlueprintCache:
    def __init__(self, path, ct):
        self.path = path
        self.ct = ct
        self.data = {"blueprints": {}, "expansions": {}}
        if path and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                pass
        self.data.setdefault("blueprints", {})
        self.data.setdefault("expansions", {})
        self._loaded_exp = set()
        self._lock = threading.Lock()

    def save(self):
        if not self.path:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        snapshot = {"blueprints": self.data["blueprints"].copy(), "expansions": self.data["expansions"].copy()}
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


def build_items(ct, cache, markup, min_price, only_singles):
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
    cache.prefetch(p.get("blueprint_id") for p in wanted if int(p.get("quantity") or 0) > 0)

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
        price = max(round(price, 2), min_price)

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

        items[ct_id] = {
            "ct_id": ct_id,
            "title": title[:255],
            "qty": max(qty, 0),
            "price": price,
            "tags": tags,
            "game_name": game_name,
            "description": desc,
            "image": bp.get("image"),
        }
    if skipped:
        log(f"  (ignorati {skipped} articoli non singoli)")
    return items


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
    only_singles = env_bool("ONLY_SINGLES", True)
    since = os.environ.get("ORDERS_SINCE", "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cache_path = os.environ.get("CACHE_FILE", ".cache/blueprints.json")

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
                 if (li.get("sku") or "").startswith(SKU_PREFIX)]
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
    items = build_items(ct, cache, markup, min_price, only_singles)
    cache.save()
    existing = sh.synced_products(location_id)
    log(f"CardTrader: {len(items)} articoli · Shopify: {len(existing)} prodotti già sincronizzati")

    created = updated_qty = updated_price = zeroed = errors = 0
    for ct_id, it in items.items():
        try:
            ex = existing.get(ct_id)
            if not ex:
                if it["qty"] <= 0:
                    continue
                log(f"+ Nuovo: {it['title']} — {it['qty']} pz a € {it['price']:.2f}")
                if not DRY_RUN:
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
        except Exception as e:
            errors += 1
            log(f"! Errore su articolo {ct_id}: {e}")

    for ct_id, ex in existing.items():
        if ct_id in items or not ex["qty"]:
            continue
        try:
            log(f"0 Esaurito su CardTrader: {ex['title']} ({ex['qty']} → 0)")
            if not DRY_RUN:
                sh.set_quantity(ex["inventory_item_id"], location_id, 0, ex["qty"])
            zeroed += 1
        except Exception as e:
            errors += 1
            log(f"! Errore azzerando {ct_id}: {e}")

    log(
        f"=== Fatto: {done_orders} ordini Shopify riportati su CardTrader · {created} nuovi · "
        f"{updated_qty} quantità · {updated_price} prezzi · {zeroed} esauriti · {errors} errori ==="
    )
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
