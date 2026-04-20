from datetime import datetime
from typing import Optional


FORMATOS_SUPORTADOS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d/%m/%y",
    "%Y/%m/%d",
]


def parse_data(valor: Optional[str]) -> Optional[datetime]:
    """
    Converte string em datetime tentando múltiplos formatos.
    Retorna None se não conseguir converter.
    """
    if valor is None:
        return None

    valor = str(valor).strip()

    if valor == "":
        return None

    for fmt in FORMATOS_SUPORTADOS:
        try:
            return datetime.strptime(valor, fmt)
        except ValueError:
            continue

    return None


def formatar_data(dt: Optional[datetime]) -> Optional[str]:
    """
    Converte datetime para string padrão YYYY-MM-DD
    """
    if dt is None:
        return None

    return dt.strftime("%Y-%m-%d")
