# Spusteni projektu ze sitoveho disku (Windows)

Projekt je pripraveny tak, aby sel kopirovat na **sitovou slozku** (napr. `\\server\sdilene\VYRAZOVANI` nebo mapovany disk `Z:\VYRAZOVANI`) a spoustet dvojklikem z Windows.

## Varianta A – offline balicek (doporuceno pro vice uzivatelu)

Na **jednom** pocitaci s internetem spravce jednou spusti **`build_portable.bat`**. Vznikne slozka **`runtime\python\`** s vestavenym Pythonem a vsemi knihovnami.

Cely adresar `VYRAZOVANI` (vcetne `runtime\`) pak staci zkopirovat na sitovy disk nebo uzivatelum – **nepotrebuji Python v PATH ani pip**. Spusteni: **`run_dashboard.bat`**.

Podrobny popis: **[PORTABLE-OFFLINE-BALICEK.md](PORTABLE-OFFLINE-BALICEK.md)**.

## Varianta B – Python na kazde stanici (klasicky venv)

### Pozadavky na stanici

- **Python 3.11+** z [python.org](https://www.python.org/downloads/) (pri instalaci zaskrtnout **Add python.exe to PATH**).
- Na Windows je vyhodny **Python Launcher** (`py -3`) - byva u instalace z python.org.

### Prvni jednorazova priprava (na kazde stanici, nebo jednou na sdilenem miste)

1. Zkopirujte celou slozku projektu na sitovy disk.
2. Otevrete slozku v Pruzkumnikovi a **dvojklikem** spustte **`setup_venv.bat`**.
   - Vytvori se virtualni prostredi **`.venv`** ve slozce projektu.
   - Nainstaluji se zavislosti z `requirements.txt`.
   - Na **pomalem sitovem ulozisti** muze prvni instalace trvat dele.

> Pokud uz mate slozku **`runtime\`** z `build_portable.bat`, **`run_dashboard.bat`** ji pouzije prednostne – `setup_venv.bat` pak neni potreba.

> **Poznamka:** Pokud antivirova politika brani spousteni skriptu z UNC cesty, pouzijte **mapovane pismeno disku** (napr. `Z:\VYRAZOVANI`) misto `\\server\...`.

## Bezne pouziti

| Soubor | Ucel |
|--------|------|
| **`run_dashboard.bat`** | Spusti Streamlit (pouzije `runtime\python` nebo `.venv`). |
| **`build_portable.bat`** | Jednorazove vytvori `runtime\python` pro offline kopirovani. |

Vsechny `.bat` soubory nejdriv prepnou adresar na slozku, kde lezi (funguje i ze sitoveho disku).

## Data

- Dejte `vypujcky_enriched.parquet` **do stejne slozky jako `run_dashboard.bat`** (koren projektu). Nemusi studovat dokumentaci – aplikace soubor sama najde (vcetne odlisne velikosti pismen v pripone).
- Pokud je soubor jinde nebo pod jinym nazvem, lze zadat cestu v levem panelu (pole „Jina cesta…“).
- **Velke Parquet soubory (napr. 1 GB+):** pro rozumnou rychlost nechte uzivatele **kopii cele slozky mit na lokálnim disku** a spoustet odtud. Prace s daty **jen ze sitoveho disku** (i kdyz aplikace bezi lokalne) casto drasticky zpomaluje nacitani – sit slouzi spise k **predani** balicku, ne k praci nad daty po SMB.

## Omezeni a tipy

- **Vice uzivatelu soucasne:** jeden sdileny `.venv` obvykle funguje, ale pri potizich je lepsi mit vlastni kopii projektu.
- **Delka cesty:** hluboke cesty na UNC nekdy zpusobuji potize - drzte projekt v kratsi ceste (mapovany disk pomaha).
- **Streamlit** otevre prohlizec na `localhost`; aplikace bezi jen na danem PC (nejde o centralni web server pro celou sit bez dalsiho nastaveni).

## Kdyz okno hned zmizi nebo "nic neudela"

1. **Otevrete CMD rucne** (Win+R -> `cmd` -> Enter), prejdete do slozky a spustte prikaz znovu:
   ```bat
   cd /d Z:\VYRAZOVANI
   setup_venv.bat
   ```
   *(Cestu `Z:\VYRAZOVANI` nahradte svou - muze byt i `\\server\sdilene\VYRAZOVANI`.)*

2. **Python v PATH** - v CMD zkuste `py -3 --version` nebo `python --version`. Kdyz to hlasi "neni rozpoznan prikaz", nainstalujte Python a zaskrtnete **Add to PATH**.

3. **UNC vs. mapovany disk** - vytvoreni `venv` na sitove ceste `\\server\...` nekdy selze (opravneni, antivirus). Zkuste **zkopirovat projekt na `C:\Projekty\VYRAZOVANI`** a spustit skripty odtud.

4. **`Connection to pypi.org timed out`** pri `pip` - firemni sit / proxy. Postup: [PIP-FIREWALL.md](PIP-FIREWALL.md).
