# VYŘAZOVÁNÍ - experimentální analýza fondu

Streamlit aplikace pro práci s knihovním fondem nad daty 2023–2025. Primárně slouží pro:

- návrh kandidátů na **odpis** (příliš svazků vs nízký výkon),
- návrh kandidátů na **dokup** (málo svazků vs vysoký výkon),
- a obecně pro přehled a filtrování titulů podle signatury, jazyka, typu dokumentu, výjimek apod.

## Pro uživatele

### Co aplikace zobrazuje

Dashboard pracuje na úrovni **titulu** (ne jednotlivých výpůjček). Pro každý titul typicky uvidíš:

- **počet výpůjček** v okně `vypujcky_5_let` / `vypujcky_okno` (délka okna je nastavitelná; typicky 3 roky pro data 2023–2025)
- **počet operací ve zvoleném období** `pocet_operaci_v_obdobi` (všechny evidenční řádky operací v tomže časovém okně; odděleně od rozpadu podle OPID / výpůjček vs. vrácení)
- **počet svazků/exemplářů** `pocet_svazku`
- **výkon na svazek** `vykon_na_svazek = vypujcky_5_let / pocet_svazku`
- související metadata (signatura, jazyk, druh dokumentu, rok vydání, …)
- klasifikaci (`AUTO_KANDIDAT`, `RUCNI_POSOUZENI`, `CHRANENO_VYJIMKOU`, `PONECHAT`)

### Co znamená „pozice“ a percentil ve signatuře

Uvnitř jednoho **prefixu signatury** (`SIGN_PREFIX`, typicky první dva znaky signatury, např. z `JD 30652` → `JD`) se tituly **seřadí podle počtu výpůjček v zvoleném období** (sestupně).

- **`relativni_pozice_v_signature`** (v tabulce jako „Pořadí / počet titulů ve stejném prefixu“): např. `42/1200` znamená, že titul je **42. nejpůjčovanější** v daném prefixu mezi **1200** tituly, které ten prefix mají.
- **`percentil_v_signature`** (v tabulce jako „Percentil v rámci prefixu signatury“): říká, jak „vysoko“ je titul v tomto žebříčku **uvnitř prefixu** — vyšší hodnota = relativně půjčovanější v rámci prefixu (100 % = nejlepší v prefixu). **Není** to percentil celého fondu, jen srovnání v rámci stejné skupiny signatur.

### Automatický kandidát (`AUTO_KANDIDAT`)

V postranním panelu je posuvník **„Automatický kandidát — max. výpůjček v období“**. Tituly s počtem výpůjček v období **nejvýše** na této hranici (a bez výjimky) spadají mezi automatické kandidáty, pokud na ně pak nesedí pravidla pro ruční posouzení. Hodnota **0** znamená jen tituly s **nulou** výpůjček v období; vyšší číslo rozšíří množinu „automatických“ titulů.

### Chráněno výjimkou (`CHRANENO_VYJIMKOU`) a JSON pravidla

Kategorie **chráněno výjimkou** vzniká, když text v datech (podle zvolených sloupců v JSONu) obsahuje některé z klíčových slov. Po doplnění raw tabulek `desk*` / `och*` + `txoch` a přegenerování `vypujcky_enriched` jsou sloupce **`desk_subjects`** a **`och_subjects`** vyplněné agregovanými texty — pravidla na ně pak můžete cílit v JSONu (klíče `desk_subjects`, `och_subjects`).

**Doporučená vazba:** upřednostněte klíčová slova v **názvu titulu**, **signatuře** a **druhu dokumentu** — ta jsou v datech obvykle vyplněná. V aplikaci je k tomu rozbalovací nápověda v sekci o výjimkách.

### Co znamená „výkon na svazek“

`vykon_na_svazek` říká, jak intenzivně se využívá jeden exemplář v rámci zvoleného okna:

- **Vysoké `pocet_svazku` + nízké `vykon_na_svazek`**: titul má hodně exemplářů, ale každý z nich se půjčuje málo → typicky **Kandidát na odpis (příliš svazků)**.
- **Nízké `pocet_svazku` + vysoké `vykon_na_svazek`**: titul má málo exemplářů, ale jsou hodně vytížené → typicky **Kandidát na dokup (málo svazků)**.
- **Hodně svazků + vysoký výkon**: titul je populární i ve více exemplářích (často „držet“, případně řešit distribuci mezi pobočkami).
- **Málo svazků + nízký výkon**: nízká poptávka a zároveň málo exemplářů (obvykle není důvod dokupovat; rozhodnutí je kontextové).

### Jak data interpretovat (svazky × výkon)

V levém panelu je sekce **Svazky × výkon** a režim doporučení:

- **Kandidát na odpis (příliš svazků)**: typicky vyšší `pocet_svazku` a zároveň nízký `vykon_na_svazek`
- **Kandidát na dokup (málo svazků)**: typicky nízký `pocet_svazku` a zároveň vysoký `vykon_na_svazek`
- **Nejasné případy**: (a) hodně svazků i vysoký výkon nebo (b) málo svazků i nízký výkon – často vyžaduje kontext poboček / specializace fondu

Režimy jsou **přednastavené** (doporučené prahy se berou z rozdělení dat) a můžeš je ručně doladit posuvníky.

### Filtry a workflow

Typický postup:

- vyber `SIGN_PREFIX` (doporučeno pro rychlost)
- dofiltruj jazyk / druh dokumentu / klasifikaci / název
- v sekci **Top kandidáti (svazky × výkon)** zkontroluj kandidáty na odpis/dokup
- výsledky exportuj do CSV nebo Excelu (v souborech zůstávají **technické názvy sloupců** kvůli kompatibilitě; v dashboardu jsou české popisky)

### Z jakých dat aplikace vychází

**Doporučený vstup pro sdílený dashboard** je `titles_metrics.parquet` (malý předpočítaný soubor po titulech).  
Aplikace ho hledá ve **stejné složce jako `app.py`**.

Zdrojová data v aktuálním buildu pokrývají období **2023–2025**. Délku okna pro výpůjčky (např. 3 roky) lze v aplikaci upravit.

Volitelně je možné aplikaci pustit i nad raw/enriched Parquetem s událostmi výpůjček, ale je to výrazně pomalejší.

## Technicky (build & provoz)

### Spuštění aplikace (lokálně)

```bash
cd "/Users/vojtechvojtisek/Github/vyrazovani"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

### Doporučený build pipeline (raw TXT → dashboard Parquet)

Zdrojová data jsou eSQL dumpy v `/Users/vojtechvojtisek/Data/vyrazovani/raw`.

1. Převod `.txt` dumpů na staging Parquety:

```bash
python3 scripts/build_parquet_from_esql_txt.py \
  --raw-dir "/Users/vojtechvojtisek/Data/vyrazovani/raw" \
  --out-dir "/Users/vojtechvojtisek/Data/vyrazovani/derived"
```

Volitelné eSQL dumpy (stejný formát jako ostatní tabulky) pro **deskriptory** a **obsahovou charakteristiku (OCH)**:


| Raw soubor                            | Výstup              | Poznámka                                                                                            |
| ------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------- |
| `txoch.txt` + `txoch2.txt`            | `txoch.parquet`     | Sloučí se do jednoho souboru (řádky za sebou).                                                      |
| `och.txt`                             | `och.parquet`       | Vazba titulu na text OCH (typicky `OCH_PTR_TITUL` → `TXOCH` přes ukazatel na `txoch`).              |
| `desk.txt`, `deskt.txt`, `txdesk.txt` | jednotlivé Parquety | Deskriptory: titul → kód deskriptoru → text (`src/subject_tables.py` zkouší typické názvy sloupců). |


Při kroku 2 se z těchto Parquetů (pokud existují a jdou spárovat) vytvoří `titul_subjects.parquet` a do `vypujcky_enriched.parquet` se doplní sloupce **`desk_subjects`** a **`och_subjects`** (agregované texty oddělené ` | `). Sloupce pak projdou do `titles_metrics.parquet` a v aplikaci je lze **filtrovat** (postranní panel) a použít v **JSON pravidlech výjimek**.

2. Sestavení `vypujcky_enriched.parquet` (joiny svazky/tituly/opidy/knoddel + privacy‑safe demografie):

```bash
python3 scripts/build_vypujcky_enriched_from_derived.py \
  --derived-dir "/Users/vojtechvojtisek/Data/vyrazovani/derived" \
  --out "/Users/vojtechvojtisek/Data/vyrazovani/derived/vypujcky_enriched.parquet" \
  --include-reader-demo
```

3. Validace:

```bash
python3 scripts/validate_build.py \
  --derived-dir "/Users/vojtechvojtisek/Data/vyrazovani/derived" \
  --fact "vypujcky_enriched.parquet"
```

4. Předpočítání dashboard datasetu po titulech:

```bash
python3 scripts/build_titles_metrics.py \
  --input "/Users/vojtechvojtisek/Data/vyrazovani/derived/vypujcky_enriched.parquet" \
  --svazky "/Users/vojtechvojtisek/Data/vyrazovani/derived/svazky.parquet" \
  --output "/Users/vojtechvojtisek/Data/vyrazovani/derived/titles_metrics.parquet"
```

5. Zkopíruj `titles_metrics.parquet` vedle `app.py` (nebo nastav cestu v sidebaru / přes `st.secrets`).

### Jedním příkazem (doporučeno)

Místo ručního spouštění všech kroků můžeš použít:

```bash
bash scripts/rebuild_all.sh "/path/to/raw" "/path/to/derived"
```

Alternativně přes proměnné prostředí:

```bash
export VYRAZ_RAW_DIR="/path/to/raw"
export VYRAZ_DERIVED_DIR="/path/to/derived"
bash scripts/rebuild_all.sh
```

### Volitelně: DuckDB databáze pro analýzy

```bash
python3 scripts/build_duckdb_db.py \
  --derived-dir "/Users/vojtechvojtisek/Data/vyrazovani/derived" \
  --db "/Users/vojtechvojtisek/Data/vyrazovani/derived/vyrazovani_2023_2025.duckdb" \
  --fact "vypujcky_enriched.parquet"
```

### Co případně doladit

- `src/config.py` → `DEFAULT_ACTION_TYPES` (co se bere jako výpůjčka)
- `src/config.py` → `DEFAULT_EXCEPTION_KEYWORDS` (výjimky)

