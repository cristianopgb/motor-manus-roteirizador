from typing import Optional


def parse_float(valor: Optional[str]) -> Optional[float]:
    """
    Converte valores numéricos vindos do Excel para float.
    Trata vírgula, espaços e valores vazios.
    """
    if valor is None:
        return None

    valor = str(valor).strip()

    if valor == "":
        return None

    try:
        # troca vírgula por ponto (padrão Excel BR)
        valor = valor.replace(",", ".")
        return float(valor)
    except ValueError:
        return None


def parse_int(valor: Optional[str]) -> Optional[int]:
    """
    Converte valor para inteiro de forma segura.
    """
    if valor is None:
        return None

    valor = str(valor).strip()

    if valor == "":
        return None

    try:
        return int(float(valor))
    except ValueError:
        return None


def percentual(parte: float, total: float) -> float:
    """
    Calcula percentual seguro (evita divisão por zero)
    """
    if total == 0:
        return 0.0

    return (parte / total) * 100.0
