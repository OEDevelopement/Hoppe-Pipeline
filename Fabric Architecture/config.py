from dataclasses import dataclass

@dataclass
class Config:
    base_url: str = "https://api.hoppe-sts.com/"
    raw_path: str = "./data/raw_data"
    transformed_path: str = "./data/transformed_data"
    gaps_path: str = "./data/gaps_data"  ############# Neuer Pfad für Null-Wert-Lücken
    batch_size: int = 1000
    max_workers: int = 8  # Erhöhte Worker für bessere Parallelisierung
    days_to_keep: int = 90  # Daten werden für 90 Tage aufbewahrt
    history_days: int = 5  # Letzten 5 Tage für Historie laden