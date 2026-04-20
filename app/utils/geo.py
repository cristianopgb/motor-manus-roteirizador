import math
from typing import Optional


def haversine_km(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float],
) -> Optional[float]:
    """
    Calcula a distância em KM entre dois pontos usando fórmula de Haversine.
    Retorna None se alguma coordenada for inválida.
    """

    if None in (lat1, lon1, lat2, lon2):
        return None

    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
    except (TypeError, ValueError):
        return None

    # raio da Terra em km
    R = 6371.0

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def km_rodoviario_estimado(
    km_linear: Optional[float],
    fator_rodoviario: float = 1.23,
) -> Optional[float]:
    """
    Converte km linear (reta) para km rodoviário estimado.
    """

    if km_linear is None:
        return None

    return km_linear * fator_rodoviario
