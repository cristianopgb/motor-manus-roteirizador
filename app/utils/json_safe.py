from __future__ import annotations

import math
from typing import Any, Tuple

import pandas as pd


def _normalizar_escalar(valor: Any) -> Any:
    if valor is None:
        return None

    if isinstance(valor, float):
        return valor

    if isinstance(valor, (str, bool, int)):
        return valor

    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()

    if isinstance(valor, pd.Timedelta):
        return str(valor)

    if isinstance(valor, (pd.Series, pd.Index)):
        return valor.tolist()

    if hasattr(valor, "item"):
        try:
            return valor.item()
        except Exception:
            return valor

    return valor


def sanitizar_json_safe(valor: Any) -> Tuple[Any, int]:
    """
    Percorre recursivamente estruturas e substitui valores não JSON-safe por None.
    Regras:
      - NaN -> None
      - Infinity -> None
      - -Infinity -> None
      - equivalentes numpy/pandas escalares
    Retorna (valor_sanitizado, quantidade_substituicoes).
    """
    substituicoes = 0

    def _walk(obj: Any) -> Any:
        nonlocal substituicoes

        if isinstance(obj, dict):
            return {k: _walk(v) for k, v in obj.items()}

        if isinstance(obj, list):
            return [_walk(v) for v in obj]

        if isinstance(obj, tuple):
            return [_walk(v) for v in obj]

        escalar = _normalizar_escalar(obj)

        try:
            if pd.isna(escalar):
                substituicoes += 1
                return None
        except Exception:
            pass

        if isinstance(escalar, float) and not math.isfinite(escalar):
            substituicoes += 1
            return None

        return escalar

    return _walk(valor), substituicoes
