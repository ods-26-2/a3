import pandas as pd
from services.kafka_engine import get_violations_state

def get_mock_violations() -> pd.DataFrame:
    """Consome e retorna os dados reais processados pelo Kafka no formato esperado pela UI."""
    data = get_violations_state()
    
    if not data:
        # Mantém a estrutura de colunas para não quebrar os componentes visuais do Streamlit
        return pd.DataFrame(columns=["Grau de violação", "Nome da câmera", "Horário da violação"])
        
    df = pd.DataFrame(data)
    return df.sort_values("Horário da violação").reset_index(drop=True)