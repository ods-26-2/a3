# Só o cliente + o mock (sem processar vídeo)
def fetch_recent_clips():
    """Retorna a lista de videoclipes de violações recentes (Mock)."""
    return [
        {"id": "clip_01", "camera": "Cam 01", "time": "10:15:22"},
        {"id": "clip_02", "camera": "Cam 03", "time": "09:40:05"}
    ]