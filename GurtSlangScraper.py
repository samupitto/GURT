import csv
import os
import time
import requests
import urllib.parse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ── CONFIG ────────────────────────────────────────────────────────────────
OUTPUT_FOLDER  = #your output folder
OUTPUT_CSV     = #your output file
OUTPUT_PATH    = os.path.join(OUTPUT_FOLDER, OUTPUT_CSV)
RANDOM_URL     = "http://onlineslangdictionary.com/random-word/"
PAGES_TO_VISIT = 50000    # totale pagine da visitare
CLUSTER_SIZE   = 20     # pagine successive da ciascun punto di partenza
MAX_WORKERS    = 40      # thread paralleli
DELAY_SEC      = 0.05     # pausa tra le richieste
# ──────────────────────────────────────────────────────────────────────────

session        = requests.Session()
seen_sentences = set()
tag_cache      = {}
lock           = Lock()
current_id     = 0

def fetch_highlight_and_clean(url):
    """Scarica una singola pagina e restituisce (entries, next_url)."""
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return [], None

    results = []
    for block in soup.select("blockquote.sentence"):
        if block.select_one("span.censored"):
            continue
        b_tag = block.find("b")
        if not b_tag:
            continue
        word = b_tag.get_text(strip=True)
        for b in block.find_all("b"):
            b.unwrap()
        sentence = block.get_text(separator=" ", strip=True)
        results.append((word, sentence))

    next_link = soup.find("a", string=lambda txt: txt and "Next" in txt)
    next_url = urllib.parse.urljoin(url, next_link["href"]) if next_link and next_link.has_attr("href") else None

    return results, next_url

def check_slang_type(word):
    """Lookup su OED: 'U' se non trovato, altrimenti 'N'."""
    headers = {"User-Agent": "Mozilla/5.0"}
    for tok in word.split():
        q   = urllib.parse.quote(tok)
        url = f"https://www.oed.com/search/dictionary/?scope=Entries&q={q}"
        try:
            resp = session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
        except requests.HTTPError:
            return "U"
        soup    = BeautifulSoup(resp.text, "html.parser")
        summary = soup.select_one("div.searchSummary")
        txt     = summary.get_text(strip=True) if summary else ""
        if txt.startswith("0 result"):
            return "U"
    return "N"

def init_csv():
    """Prepara il CSV, carica ID iniziale, frasi e cache esistenti."""
    global current_id
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    if not os.path.isfile(OUTPUT_PATH):
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "slang_word", "sentence", "type"])
        current_id = 1
    else:
        with open(OUTPUT_PATH, newline="", encoding="utf-8") as f:
            lines = list(csv.reader(f))
            current_id = len(lines)
        with open(OUTPUT_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                seen_sentences.add(row["sentence"])
                tag_cache[row["slang_word"]] = row["type"]

def save_row(word, sentence, tag):
    """Appende una riga nel CSV in modo thread-safe."""
    global current_id
    with lock:
        with open(OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([current_id, word, sentence, tag])
        print(f"Salvato: ID {current_id} → {word!r}: {sentence!r} [{tag}]")
        current_id += 1

def process_cluster(cluster_id):
    """
    Ogni worker parte da RANDOM_URL, poi segue 'Next' per CLUSTER_SIZE pagine.
    Scrive direttamente su file via save_row.
    """
    url = RANDOM_URL
    for _ in range(CLUSTER_SIZE):
        entries, next_url = fetch_highlight_and_clean(url)
        for word, sentence in entries:
            with lock:
                if sentence in seen_sentences:
                   # print("seen")
                    continue
                seen_sentences.add(sentence)
                tag = tag_cache.get(word)
            if not tag:
                tag = check_slang_type(word)
                with lock:
                    tag_cache[word] = tag
            save_row(word, sentence, tag)
        if not next_url:
            break
        url = next_url
        time.sleep(DELAY_SEC)

if __name__ == "__main__":
    print(f"Inizio scraping: {PAGES_TO_VISIT} pagine, cluster di {CLUSTER_SIZE}, {MAX_WORKERS} workers.")
    init_csv()
    cluster_count = (PAGES_TO_VISIT + CLUSTER_SIZE - 1) // CLUSTER_SIZE

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_cluster, cid) for cid in range(cluster_count)]
        for future in as_completed(futures):
            # basta bloccare qui finché ogni cluster non termina
            _ = future.result()

    print(f"\nFatto! Dati salvati in '{OUTPUT_PATH}'.")
