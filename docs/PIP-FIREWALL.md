# Pip a firemni sit (timeout na pypi.org)

Chyba typu `Connection to pypi.org timed out` znamena, ze z vaseho PC **nejde spolehlive na verejny Python Package Index** - caste u **firemnich siti**, kde je potreba **proxy**, nebo kde je omezeny HTTPS provoz ven z organizace.

V tomto projektu je pro sit MKP pripraven soubor **`set_proxy_mlp.bat`** (natvrdo `HTTP_PROXY` / `HTTPS_PROXY` podle PAC). Vola ho **`setup_venv.bat`** i **`run_dashboard.bat`** - neni potreba nic nastavovat rucne. Mimo MKP sit upravte nebo docasne vyjmete radek `call ... set_proxy_mlp.bat` z techto skriptu, pripadne obsah `set_proxy_mlp.bat`.

## 1) Zjistete proxy od IT

Casto existuje **HTTP/HTTPS proxy** (napr. `http://proxy.firma.cz:8080`). Pak v **CMD** pred `setup_venv.bat` nastavte:

```bat
set HTTP_PROXY=http://uzivatel:heslo@proxy.firma.cz:8080
set HTTPS_PROXY=http://uzivatel:heslo@proxy.firma.cz:8080
setup_venv.bat
```

Nebo trvale v uzivatelskem souboru **`%APPDATA%\pip\pip.ini`**:

```ini
[global]
proxy = http://proxy.firma.cz:8080
```

### WPAD / soubor `wpad.dat`

Adresa typu `http://wpad.mlp.cz/wpad.dat` neni sama o sobe proxy server, ale **PAC skript** (Proxy Auto-Configuration).

- `pip` PAC neumi - potrebuje konkretni tvar `http://server:port`.

Postup:

1. Zjistete skutecnou adresu proxy z PAC (nebo overte u IT).
2. Nastavte `pip.ini` nebo `HTTP_PROXY` / `HTTPS_PROXY`.
3. Otestujte napr. `pip install --upgrade pip` ve `.venv`.

## 2) Docasne jina sit

Jednorazove stahnout baliky z domaci WiFi nebo hotspotu (po souhlasu bezpecnostni politiky), spustit `setup_venv.bat`, pak pracovat zase z kancelare - nainstalovane baliky v `.venv` zustanou.

## 3) Instalace bez primeho pristupu na PyPI

Na pocitaci s internetem (stejna verze Pythonu):

```bat
mkdir wheels
pip download -r requirements.txt -d wheels
```

Slozku `wheels` zkopirujte do projektu a na cilovem PC:

```bat
call .venv\Scripts\activate.bat
pip install --no-index --find-links=wheels -r requirements.txt
```

---

Shrnuti: problem obvykle neni v aplikaci, ale v **pristupu site na PyPI**. Nejcistsi reseni je proxy od IT nebo jednorazova offline instalace baliku.
