import pandas as pd
from datetime import datetime, timedelta
import random

def get_mock_violations() -> pd.DataFrame:
    """Gera e retorna os dados mockados de violações."""
    random.seed(42)
    base_time = datetime(2026, 10, 16, 0, 0)
    data = []
    
    for _ in range(41):
        hour_offset = random.randint(0, 10)
        minute_offset = random.randint(0, 59)
        time = base_time + timedelta(hours=hour_offset, minutes=minute_offset)
        cam = random.choice(["Cam 01", "Cam 02", "Cam 03"])
        grau = random.choice([1, 2, 3])
        data.append({
            "Grau de violação": grau,
            "Nome da câmera": cam,
            "Horário da violação": time
        })
    
    df = pd.DataFrame(data)
    return df.sort_values("Horário da violação").reset_index(drop=True)