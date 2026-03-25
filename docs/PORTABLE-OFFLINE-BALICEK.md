# Prenosny offline balicek (Windows)

Cil: **jedna slozka** (aplikace + volitelne data) – uzivatel si ji **zkopiruje na lokálni disk** (`C:\...`) a spusti **`run_dashboard.bat`** – bez instalace Pythonu, bez `pip` z internetu. Sitovy disk muze slouzit jen k predani kopie; u velkych Parquetu je prace z lokálniho disku vyrazne prijemnejsi.

## Jak to funguje

- Do projektu patri **vestaveny Python pro Windows** (oficialni [embeddable](https://docs.python.org/3/using/windows.html#the-embeddable-package) balicek) ve slozce **`runtime\python\`**.
- Vsechny knihovny z `requirements.txt` jsou tam **predinstalovane** (jednorazove sestaveni).
- Skript **`run_dashboard.bat`** nejdriv hleda `runtime\python\python.exe`, pokud chybi, pouzije starsi variantu **`.venv`** (po `setup_venv.bat`).

**Vysledek pro uzivatele:** kopie cele slozky `VYRAZOVANI` vcetne `runtime\` = spustitelna aplikace bez dalsich kroku.

### Data o vypujckach (Parquet)

**Nejsou soucasti sestaveni** `build_portable.bat` – skript neinstaluje ani nekopiruje data, jen Python a knihovny.

- Vychozi soubor v aplikaci je `vypujcky_enriched.parquet` (soubor primo ve slozce `VYRAZOVANI`).
- Pri priprave distribuce pro uzivatele **doplnte** `*.parquet` rucne (nebo nechte uzivatele zadat cestu / URL v aplikaci).
- Do Git repozitare se velky soubor typicky **nepushuje**; patri do firemni sdilene slozky nebo do ZIPu, kterou k tomu aplikacnimu balicku pridate.

**Doporuceny zpusob pro velka data (napr. ~1 GB+):** datovy soubor **dejte primo do stejne slozky jako aplikaci** a nechte uzivatele **kopii cele slozky rozbalit na lokálni disk** (SSD na `C:\...`), ne ji spoustet primo ze sitoveho disku. Cteni Parquetu pres SMB (UNC / mapovany disk) u tak velkych souboru byva **znatelne pomalejsi** nez z lokálniho disku – stejna slozka „balicku“ tedy muze obsahovat `runtime\`, `app.py`, `src\` i `vypujcky_enriched.parquet`; sitovy disk slouzi spise k **predani** kopie, ne k praci „z nej“.

## Co kdo dela

| Kdo | Co |
|-----|-----|
| **Spravce / IT** (jednou, na PC s internetem) | Spusti `build_portable.bat` (viz nize). Volitelne `download_wheels.bat` + kopie `wheels\` na izolovanou sit. |
| **Koncovi uzivatele** | Zkopiruji slozku (vcetne `runtime\` a datoveho Parquetu, pokud je soucasti vase distribuce), spusti `run_dashboard.bat`. |

## Jednorazove sestaveni balicku (`build_portable.bat`)

1. Poetac **Windows 64-bit** (Intel/AMD). Embeddable build je **amd64** (ne ARM).
2. **PowerShell** (standardne ve Windows 10/11).
3. **Pristup k internetu** (PyPI) – jen pri sestavovani, ne pri uzivatelich.

Postup:

1. Rozbalte / zkopirujte cely projekt `VYRAZOVANI`.
2. Dvojklikem `build_portable.bat` **nebo** v CMD:
   ```bat
   cd /d C:\cesta\VYRAZOVANI
   build_portable.bat
   ```
3. Pro **preinstalaci** (napr. po zmene `requirements.txt`):
   ```bat
   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_portable.ps1 -Force
   ```

Po uspechu existuje `runtime\python\` (stovky MB kvuli Streamlitu, pandas, plotly atd.).

### Firemni proxy

Pokud `pip` z internetu nejde, odkomentujte v `build_portable.bat` radek:

```bat
call "%~dp0set_proxy_mlp.bat"
```

nebo nastavte `HTTP_PROXY` / `HTTPS_PROXY` podle `docs/PIP-FIREWALL.md`.

### Plne offline sestaveni (bez PyPI na miste)

1. Na pocitaci s internetem spustte **`download_wheels.bat`** – vytvori slozku **`wheels\`** se vsemi `.whl` soubory.
2. Zkopirujte cely projekt **i s `wheels\`** na uzavrenou sit.
3. Spustte **`build_portable.bat`**. Skript `scripts\build_portable.ps1` automaticky pouzije `pip install --no-index --find-links=wheels`, pokud je ve `wheels\` aspon jeden wheel.

## Distribuce uzivatelum

1. Zabalte **celou adresar** `VYRAZOVANI` (vcetne `app.py`, `src\`, `runtime\`, `.bat`, dat…) do ZIP nebo zkopirujte na sitovy disk.
2. **Nedavte do Gitu** cely `runtime\` – je velky; repozitar je jen zdroj, **release balicek** sestavuje spravce.

## Omezeni a tipy

- **Velikost:** cely balicek bude radove **300–600 MB** (zavisi na verzich knihoven).
- **Antivirus** muze prvni spusteni zpomalit; vyjimka pro slozku projektu je u nekterych firem potreba.
- **Kopie na UNC:** `\\server\sdilene\...` nekdy selhava; **mapovane pismeno disku** (`Z:\`) je spolehlivejsi.
- **Streamlit** otevre prohlizec na `localhost`; aplikace bezi jen na danem PC (stejne jako drive).
- **VCRedist:** Windows 10/11 obvykle maji potrebne DLL; pri chybejicim `VCRUNTIME140.dll` nainstalujte [Microsoft Visual C++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist).

## Souvisejici

- `docs/SITOVY-DISK-WINDOWS.md` – obecne spousteni ze site
- `docs/PIP-FIREWALL.md` – proxy a pip
