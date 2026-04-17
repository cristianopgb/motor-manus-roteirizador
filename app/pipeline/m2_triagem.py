"""
M2 — Triagem Operacional
Responsabilidade:
  1. Separar cargas com carro_dedicado=True (manifestos fechados individuais)
  2. Separar cargas com restrição de veículo específico
  3. Retornar três grupos: dedicados, restritos, livres
"""
from __future__ import annotations
import pandas as pd
from typing import Tuple, Dict


def executar(
    df: pd.DataFrame,
    veiculos: list,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict]:
    """
    Retorna (df_dedicados, df_restritos, df_livres, log).
    """
    entrada_total = len(df)

    if df.empty:
        log = {
            "etapa": "M2 — Triagem Operacional",
            "descricao": "Separação de cargas dedicadas, restritas e livres",
            "qtd_entrada": 0,
            "qtd_saida": 0,
            "qtd_descartada": 0,
            "detalhes": "Nenhuma carga para triar"
        }
        empty = pd.DataFrame()
        return empty, empty, empty, log

    perfis_disponiveis = {v["perfil"].upper() for v in veiculos}

    # --- Grupo 1: carro dedicado (manifesto individual por destinatário)
    mask_dedicado = df["carro_dedicado"] == True
    df_dedicados = df[mask_dedicado].copy()

    df_restante = df[~mask_dedicado].copy()

    # --- Grupo 2: restrição de veículo (perfil específico exigido)
    mask_restrito = (
        df_restante["restricao_veiculo"].notna() &
        (df_restante["restricao_veiculo"] != "") &
        (df_restante["restricao_veiculo"] != "QUALQUER")
    )
    df_restritos = df_restante[mask_restrito].copy()
    df_livres = df_restante[~mask_restrito].copy()

    # Verificar se perfis restritos existem na frota disponível
    sem_veiculo = []
    if not df_restritos.empty:
        mask_sem_veiculo = ~df_restritos["restricao_veiculo"].str.upper().isin(perfis_disponiveis)
        sem_veiculo_df = df_restritos[mask_sem_veiculo]
        if not sem_veiculo_df.empty:
            sem_veiculo = sem_veiculo_df.to_dict("records")
        df_restritos = df_restritos[~mask_sem_veiculo].copy()

    log = {
        "etapa": "M2 — Triagem Operacional",
        "descricao": "Separação de cargas dedicadas, restritas e livres",
        "qtd_entrada": entrada_total,
        "qtd_saida": len(df_dedicados) + len(df_restritos) + len(df_livres),
        "qtd_descartada": len(sem_veiculo),
        "detalhes": (
            f"Dedicados: {len(df_dedicados)} | "
            f"Restritos: {len(df_restritos)} | "
            f"Livres: {len(df_livres)} | "
            f"Sem veículo compatível: {len(sem_veiculo)}"
        )
    }

    return df_dedicados, df_restritos, df_livres, log, sem_veiculo
