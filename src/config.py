"""Defaultni konfigurace pro analyzu vyrazovani."""

# Pravidla výjimek: podřetězce v textu sloupců (case-insensitive).
# Pozn.: ve výchozím buildu `vypujcky_enriched.parquet` je `desk_subjects` prázdné — pro „deskriptory“
# doplňte sloupec v pipeline, nebo použijte TITUL_NAZEV / TITUL_SIGN_FULL / TITUL_DRUH_DOKUMENTU.
DEFAULT_EXCEPTION_KEYWORDS = {
    "TITUL_DRUH_DOKUMENTU": ["beletrie", "poezie", "drama"],
    "TITUL_SIGN_FULL": ["beletrie", "poezie", "drama", "pragensia"],
    "TITUL_NAZEV": ["praha", "pragensia", "pražsk"],
    # Po naplnění pipeline (desk → deskt → txdesk, och → txoch):
    "desk_subjects": [],
    "och_subjects": [],
}

DEFAULT_REQUIRED_COLUMNS = [
    "TITUL_SIGN_FULL",
    "TITUL_NAZEV",
    "TITUL_ROK_VYDANI",
    "TITUL_JAZYK",
    "TITUL_DRUH_DOKUMENTU",
    "DATE",
    "YEAR",
    "ACTION_TYPE",
    "desk_subjects",
]

DEFAULT_ACTION_TYPES = ["loan", "vypujcka", "checkout", "borrow"]
# Prefer the precomputed, title-level dataset for the Streamlit dashboard.
# The app still supports loading raw/enriched loan events if provided explicitly.
DEFAULT_PARQUET_PATH = "titles_metrics.parquet"
