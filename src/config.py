"""Defaultni konfigurace pro analyzu vyrazovani."""

DEFAULT_EXCEPTION_KEYWORDS = {
    "TITUL_DRUH_DOKUMENTU": ["beletrie", "poezie", "drama"],
    "desk_subjects": ["pragensia", "praha", "prazske"],
    "TITUL_SIGN_FULL": ["beletrie", "poezie", "drama", "pragensia"],
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
DEFAULT_PARQUET_PATH = "vypujcky_enriched.parquet"
