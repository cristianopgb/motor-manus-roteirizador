"""
M3 — Cálculo de Distâncias
Responsabilidade: calcular a distância Haversine entre dois pontos geográficos
e construir a matriz de distâncias para uso no sequenciamento.
Aplica fator rodoviário de 1.25 (desvios de rota reais vs. linha reta).
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple

FATOR_RODOVIARIO = 1.25  # distância real ≈ 25% maior que linha reta
RAIO_TERRA_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância em km entre dois pontos geográficos (linha reta)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * RAIO_TERRA_KM * math.asin(math.sqrt(a))


def distancia_rodoviaria_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância rodoviária estimada (Haversine × fator rodoviário)."""
    return haversine_km(lat1, lon1, lat2, lon2) * FATOR_RODOVIARIO


def calcular_distancia_da_filial(
    df: pd.DataFrame,
    filial_lat: float,
    filial_lon: float,
) -> pd.DataFrame:
    """
    Adiciona coluna 'km_da_filial' ao DataFrame.
    """
    if df.empty:
        return df

    df = df.copy()
    df["km_da_filial"] = df.apply(
        lambda row: distancia_rodoviaria_km(
            filial_lat, filial_lon,
            row["latitude"], row["longitude"]
        ),
        axis=1
    )
    return df


def construir_matriz_distancias(
    pontos: List[Tuple[float, float]],
) -> np.ndarray:
    """
    Constrói matriz N×N de distâncias rodoviárias entre N pontos.
    pontos: lista de (lat, lon)
    """
    n = len(pontos)
    matriz = np.zeros((n, n), dtype=float)
    for i in range(n):
        for j in range(i + 1, n):
            d = distancia_rodoviaria_km(
                pontos[i][0], pontos[i][1],
                pontos[j][0], pontos[j][1]
            )
            matriz[i][j] = d
            matriz[j][i] = d
    return matriz


def executar(
    df: pd.DataFrame,
    filial_lat: float,
    filial_lon: float,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Enriquece o DataFrame com a distância de cada entrega em relação à filial.
    """
    entrada_total = len(df)

    if df.empty:
        log = {
            "etapa": "M3 — Distâncias",
            "descricao": "Cálculo de distância rodoviária estimada (Haversine × 1.25)",
            "qtd_entrada": 0,
            "qtd_saida": 0,
            "qtd_descartada": 0,
            "detalhes": "Nenhuma carga para calcular distâncias"
        }
        return df, log

    df = calcular_distancia_da_filial(df, filial_lat, filial_lon)

    # Cargas além do raio máximo de qualquer veículo serão tratadas no M4
    log = {
        "etapa": "M3 — Distâncias",
        "descricao": "Cálculo de distância rodoviária estimada (Haversine × 1.25)",
        "qtd_entrada": entrada_total,
        "qtd_saida": len(df),
        "qtd_descartada": 0,
        "detalhes": (
            f"Distância média da filial: {df['km_da_filial'].mean():.1f} km | "
            f"Máxima: {df['km_da_filial'].max():.1f} km | "
            f"Mínima: {df['km_da_filial'].min():.1f} km"
        )
    }
    return df, log
