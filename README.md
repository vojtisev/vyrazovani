# VYRAZOVANI - lokalni analyza fondu z Parquet

Samostatna Streamlit aplikace pro navrh kandidatu na vyrazeni fondu.
Projekt je oddeleny od zbytku repozitare ve slozce `VYRAZOVANI`.

## Projektova struktura

```text
VYRAZOVANI/
├── app.py
├── requirements.txt
├── README.md
├── build_portable.bat      # jednorazove sestaveni offline runtime (Windows)
├── run_dashboard.bat
├── scripts/
│   └── build_portable.ps1
└── src/
    ├── __init__.py
    ├── config.py
    ├── data_processing.py
    ├── paths.py
    └── export_utils.py
```

Slozky `runtime\` a `wheels\` vzniknou pri sestaveni offline balicku; v Gitu nejsou.

## Vstup

- Vstupem je **Parquet** soubor s vypujckami (ne CSV).
- Vychozi soubor je `vypujcky_enriched.parquet` ve **stejne slozce jako `app.py` / `run_dashboard.bat`** – aplikace ho hleda podle slozky projektu (ne podle „aktualniho adresare“), takze staci zkopirovat cely adresar a spustit skript.
- Lze zadat i **https://...** (predpodepsany odkaz) nebo **`s3://...`** (s AWS klici ve Streamlit Secrets).
- Cestu lze zmenit v levem panelu nebo nahrat mensi Parquet pres **Nahraj Parquet z disku**.

## Spusteni

```bash
cd /Users/vojtechvojtisek/OAI/MKP/VYRAZOVANI
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

### Windows ze sitoveho disku (doporuceno)

**Offline balicek (bez instalace Pythonu a bez pip z internetu u uzivatele):**  
jednou na pocitaci s internetem spustte `build_portable.bat`, pak cely adresar `VYRAZOVANI` vcetne `runtime\` zkopirujte na sit / uzivatelum. Uzivatel jen spusti `run_dashboard.bat`.  
Podrobne: `docs/PORTABLE-OFFLINE-BALICEK.md`.

**Klasicka varianta (Python v PATH na kazde stanici):**  
jednou `setup_venv.bat`, potom `run_dashboard.bat`.

Detaily jsou v `docs/SITOVY-DISK-WINDOWS.md`.  
Proxy v MKP siti je resena pres `set_proxy_mlp.bat`; postupy jsou v `docs/PIP-FIREWALL.md`.

## Co aplikace umi

- Validovat sloupce a srozumitelne hlasit chybejici.
- Pouzit fallback logiku bez padani aplikace.
- Pocitat metriky za poslednich N let (default 5).
- Klasifikovat tituly do:
  - `AUTO_KANDIDAT`
  - `RUCNI_POSOUZENI`
  - `PONECHAT`
  - `CHRANENO_VYJIMKOU`
- Vysvetlit klasifikaci pres `duvod_oznaceni`.
- Nabidnout filtry podle signatury, jazyka, typu dokumentu a klasifikace.
- Zobrazit tabulky, souhrny, grafy a exportovat CSV/XLSX (listy dle kategorii + souhrn).

## Co pripadne doladit podle reality

1. `src/config.py` -> `DEFAULT_ACTION_TYPES` (ktere akce jsou vypujcky).
2. `src/config.py` -> `DEFAULT_EXCEPTION_KEYWORDS` (lokalni pravidla vyjimek).
3. `src/data_processing.py` -> `_build_title_key`, pokud mate stabilni ID titulu.

