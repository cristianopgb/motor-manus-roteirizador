from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, Tuple

import pandas as pd


COLUNAS_MINIMAS_M31 = [
    "status_triagem",
    "grupo_saida",
    "agendada",
    "folga_dias",
    "peso_kg",
    "peso_calculado",
    "vol_m3",
    "distancia_rodoviaria_est_km",
    "destinatario",
    "cidade",
    "uf",
    "data_agenda",
    "data_leadtime",
]

COLUNAS_BASE_HASH_PREFERENCIA = [
    "nro_documento",
    "romaneio",
    "serie_romaneio",
    "serie",
    "filial_roteirizacao",
    "filial_origem",
    "destinatario",
    "cidade",
    "uf",
    "peso_kg",
    "vol_m3",
    "peso_calculado",
    "data_agenda",
    "data_leadtime",
]


def executar_m3_1_validacao_fronteira(
    df_carteira_roteirizavel: pd.DataFrame,
    data_base_roteirizacao: datetime,
    caminhos_pipeline: Dict[str, Any] | None = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    M3.1 real adaptado ao Sistema 2.

    Objetivo:
    - Receber somente a carteira roteirizável do M3
    - Validar ausência de contaminação na fronteira do bloco 4
    - Criar a chave técnica oficial id_linha_pipeline
    - Devolver o dataframe oficial de entrada do bloco 4

    Regras alinhadas ao M3 atual:
    - roteirizável com data_agenda: folga_dias >= 0 e < 2
    - roteirizável sem data_agenda: deve ter data_leadtime preenchida
    - a verdade operacional de agenda é somente data_agenda

    Regra crítica de peso nesta fronteira:
    - peso_kg permanece como auditoria
    - peso_calculado é obrigatório como referência operacional do bloco 4 em diante
    - este módulo não recalcula peso; apenas valida a integridade da fronteira
    """

    df_input = df_carteira_roteirizavel.copy().reset_index(drop=True)

    _validar_colunas_minimas(df_input)
    _tipagem_defensiva(df_input)
    _validacoes_duras(df_input)

    colunas_base_hash = [c for c in COLUNAS_BASE_HASH_PREFERENCIA if c in df_input.columns]
    if len(colunas_base_hash) < 6:
        raise Exception(
            "Não há colunas suficientes para criar a chave técnica oficial do pipeline na fronteira do bloco 4."
        )

    df_input["id_linha_pipeline"] = df_input.apply(
        lambda row: _gerar_id_linha_pipeline(row, colunas_base_hash), axis=1
    )

    if df_input["id_linha_pipeline"].duplicated().any():
        duplicados = int(df_input["id_linha_pipeline"].duplicated().sum())
        raise Exception(
            f"A chave técnica id_linha_pipeline ficou duplicada em {duplicados} linhas. "
            "O contrato do pipeline precisa ser único nesta fronteira."
        )

    colunas_finais = ["id_linha_pipeline"] + [c for c in df_input.columns if c != "id_linha_pipeline"]
    df_input_oficial_bloco_4 = df_input[colunas_finais].copy()

    resumo_m31 = {
        "modulo": "m3_1_validacao_fronteira",
        "data_execucao": datetime.utcnow().isoformat(),
        "data_base_roteirizacao": data_base_roteirizacao.isoformat(),
        "linhas_input": int(len(df_input_oficial_bloco_4)),
        "colunas_input": int(len(df_input_oficial_bloco_4.columns)),
        "agendadas_validas": int(df_input_oficial_bloco_4["data_agenda"].notna().sum()),
        "leadtime_sem_agenda": int(df_input_oficial_bloco_4["data_agenda"].isna().sum()),
        "peso_nulo": int(df_input_oficial_bloco_4["peso_kg"].isna().sum()),
        "peso_calculado_nulo": int(df_input_oficial_bloco_4["peso_calculado"].isna().sum()),
        "volume_nulo": int(df_input_oficial_bloco_4["vol_m3"].isna().sum()),
        "km_nulo": int(df_input_oficial_bloco_4["distancia_rodoviaria_est_km"].isna().sum()),
        "veiculo_exclusivo_flag_true": int(
            df_input_oficial_bloco_4["veiculo_exclusivo_flag"].fillna(False).astype(bool).sum()
        ) if "veiculo_exclusivo_flag" in df_input_oficial_bloco_4.columns else None,
        "prioridade_embarque_1": int(
            (pd.to_numeric(df_input_oficial_bloco_4["prioridade_embarque"], errors="coerce") == 1).sum()
        ) if "prioridade_embarque" in df_input_oficial_bloco_4.columns else None,
        "ids_tecnicos_unicos": int(df_input_oficial_bloco_4["id_linha_pipeline"].nunique()),
        "colunas_base_hash": colunas_base_hash,
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    return df_input_oficial_bloco_4, {
        "resumo_m31": resumo_m31,
        "outputs_m31": {
            "df_input_oficial_bloco_4": df_input_oficial_bloco_4,
        },
    }


def _validar_colunas_minimas(df: pd.DataFrame) -> None:
    faltam = [c for c in COLUNAS_MINIMAS_M31 if c not in df.columns]
    if faltam:
        raise Exception(
            "Faltam colunas mínimas no input do Bloco 4:\n- " + "\n- ".join(faltam)
        )


def _tipagem_defensiva(df: pd.DataFrame) -> None:
    df["status_triagem"] = df["status_triagem"].astype(str).str.strip()
    df["grupo_saida"] = df["grupo_saida"].astype(str).str.strip()
    df["agendada"] = df["agendada"].fillna(False).astype(bool)

    df["data_agenda"] = pd.to_datetime(df["data_agenda"], errors="coerce")
    df["data_leadtime"] = pd.to_datetime(df["data_leadtime"], errors="coerce")

    df["folga_dias"] = pd.to_numeric(df["folga_dias"], errors="coerce")
    df["peso_kg"] = pd.to_numeric(df["peso_kg"], errors="coerce")
    df["peso_calculado"] = pd.to_numeric(df["peso_calculado"], errors="coerce")
    df["vol_m3"] = pd.to_numeric(df["vol_m3"], errors="coerce")
    df["distancia_rodoviaria_est_km"] = pd.to_numeric(df["distancia_rodoviaria_est_km"], errors="coerce")

    if "prioridade_embarque" in df.columns:
        df["prioridade_embarque"] = pd.to_numeric(df["prioridade_embarque"], errors="coerce")

    if "veiculo_exclusivo_flag" in df.columns:
        df["veiculo_exclusivo_flag"] = df["veiculo_exclusivo_flag"].fillna(False).astype(bool)


def _validacoes_duras(df: pd.DataFrame) -> None:
    problemas: list[str] = []

    invalidas_status = df.loc[df["status_triagem"] != "roteirizavel"]
    if len(invalidas_status) > 0:
        problemas.append(
            f"Linhas com status_triagem diferente de 'roteirizavel': {len(invalidas_status)}"
        )

    invalidas_grupo = df.loc[df["grupo_saida"] != "df_carteira_roteirizavel"]
    if len(invalidas_grupo) > 0:
        problemas.append(
            f"Linhas com grupo_saida diferente de 'df_carteira_roteirizavel': {len(invalidas_grupo)}"
        )

    # Regra 1: se tem data_agenda, a folga válida para roteirizável é 0 <= folga < 2
    agendadas_invalidas = df.loc[
        df["data_agenda"].notna()
        & (
            df["folga_dias"].isna()
            | (df["folga_dias"] < 0)
            | (df["folga_dias"] >= 2)
        )
    ]
    if len(agendadas_invalidas) > 0:
        problemas.append(
            f"Linhas com data_agenda fora da faixa permitida para roteirização (0 <= folga < 2): {len(agendadas_invalidas)}"
        )

    # Regra 2: se não tem data_agenda, deve ter DLE preenchido
    leadtimes_invalidos = df.loc[
        df["data_agenda"].isna() & df["data_leadtime"].isna()
    ]
    if len(leadtimes_invalidos) > 0:
        problemas.append(
            f"Linhas sem data_agenda e sem data_leadtime na fronteira do Bloco 4: {len(leadtimes_invalidos)}"
        )

    linhas_sem_peso = int(df["peso_kg"].isna().sum())
    linhas_sem_peso_calculado = int(df["peso_calculado"].isna().sum())
    linhas_sem_vol = int(df["vol_m3"].isna().sum())
    linhas_sem_km = int(df["distancia_rodoviaria_est_km"].isna().sum())

    if linhas_sem_peso > 0:
        problemas.append(f"Linhas sem peso_kg: {linhas_sem_peso}")

    if linhas_sem_peso_calculado > 0:
        problemas.append(
            f"Linhas sem peso_calculado na fronteira do Bloco 4: {linhas_sem_peso_calculado}"
        )

    if linhas_sem_vol > 0:
        problemas.append(f"Linhas sem vol_m3: {linhas_sem_vol}")

    if linhas_sem_km > 0:
        problemas.append(f"Linhas sem distancia_rodoviaria_est_km: {linhas_sem_km}")

    if problemas:
        raise Exception("A fronteira de input do Bloco 4 falhou:\n- " + "\n- ".join(problemas))


def _gerar_id_linha_pipeline(row: pd.Series, colunas_base_hash: list[str]) -> str:
    partes: list[str] = []
    for coluna in colunas_base_hash:
        valor = row[coluna]
        if pd.isna(valor):
            partes.append("")
        elif isinstance(valor, pd.Timestamp):
            partes.append(valor.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            partes.append(str(valor).strip())

    payload = "||".join(partes)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
