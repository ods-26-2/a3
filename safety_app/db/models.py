from dataclasses import dataclass
from datetime import datetime

@dataclass
class ViolationModel:
    grau: int
    camera: str
    horario: datetime