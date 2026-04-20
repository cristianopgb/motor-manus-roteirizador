from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd


def executar_m3_triagem(
    df_carteira_enriquecida: pd.DataFrame,
    data_base_roteirizacao: datetime,
    caminhos_pipeline: Dict[str, Any] | None = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    M3 real adaptado ao Sistema 2 (API).

    Regras oficiais de triagem:
    1) roteirizavel
       - sem data_agenda e com data_leadtime preenchida
       - com data_agenda e folga_dias >= 0 e < 2

    2) agendamento_futuro
       - com data_agenda e folga_dias >= 2

    3) agenda_vencida
       - com data_agenda e folga_dias < 0

    Observações:
    - A coluna de verdade para agenda é somente data_agenda (Agendam.)
    - A coluna textual Agenda não participa da regra
    - Não existe mais categoria aguardando_agendamento
    - folga_dias == 2 entra em agendamento_futuro

    Regra crítica preservada:
    - este módulo não recalcula peso
    - peso_kg permanece como auditoria
    - peso_calculado deve seguir preservado como referência operacional do bloco 4
    """

    carteira = df_carteira_enriquecida.copy()

    _validar_colunas_minimas(carteira)

    carteira["data_agenda"] = pd.to_datetime(carteira["data_agenda"], errors="coerce")
    carteira["data_leadtime"] = pd.to_datetime(carteira["data_leadtime"], errors="coerce")
    carteira["data_limite_considerada"] = pd.to_datetime(carteira["data_limite_considerada"], errors="coerce")
    carteira["folga_dias"] = pd.to_numeric(carteira["folga_dias"], errors="coerce")
    carteira["transit_time_dias"] = pd.to_numeric(carteira["transit_time_dias"], errors="coerce")
    carteira["dias_ate_data_alvo"] = pd.to_numeric(carteira["dias_ate_data_alvo"], errors="coerce")
    carteira["peso_kg"] = pd.to_numeric(carteira["peso_kg"], errors="coerce")
    carteira["peso_calculado"] = pd.to_numeric(carteira["peso_calculado"], errors="coerce")

    # Verdade operacional de agenda = existe data_agenda
    carteira["agendada"] = carteira["data_agenda"].notna()

    if "score_prioridade_preliminar" not in carteira.columns:
        carteira["score_prioridade_preliminar"] = 0

    if "ranking_preliminar" not in carteira.columns:
        carteira["ranking_preliminar"] = pd.Series(range(1, len(carteira) + 1), index=carteira.index)

    if "distancia_rodoviaria_est_km" not in carteira.columns:
        carteira["distancia_rodoviaria_est_km"] = np.nan

    carteira["score_prioridade_preliminar"] = pd.to_numeric(
        carteira["score_prioridade_preliminar"], errors="coerce"
    ).fillna(0)

    carteira["ranking_preliminar"] = pd.to_numeric(
        carteira["ranking_preliminar"], errors="coerce"
    )

    carteira["status_triagem"] = carteira.apply(_classificar_status_triagem, axis=1)
    carteira["motivo_triagem"] = carteira.apply(_definir_motivo_triagem, axis=1)
    carteira["grupo_saida"] = carteira["status_triagem"].apply(_definir_grupo_saida)
    carteira["prioridade_label"] = carteira.apply(_definir_prioridade_label, axis=1)
    carteira["ranking_prioridade_operacional"] = carteira.apply(_definir_ranking_operacional, axis=1)

    carteira["flag_roteirizavel"] = carteira["status_triagem"].eq("roteirizavel")
    carteira["flag_agendamento_futuro"] = carteira["status_triagem"].eq("agendamento_futuro")
    carteira["flag_agenda_vencida"] = carteira["status_triagem"].eq("agenda_vencida")

    df_carteira_triagem = carteira.copy()

    df_carteira_roteirizavel = (
        carteira.loc[carteira["status_triagem"] == "roteirizavel"]
        .sort_values(
            by=[
                "ranking_prioridade_operacional",
                "score_prioridade_preliminar",
                "distancia_rodoviaria_est_km",
            ],
            ascending=[True, False, True],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    df_carteira_agendamento_futuro = (
        carteira.loc[carteira["status_triagem"] == "agendamento_futuro"]
        .sort_values(
            by=["data_limite_considerada", "score_prioridade_preliminar"],
            ascending=[True, False],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    df_carteira_agendas_vencidas = (
        carteira.loc[carteira["status_triagem"] == "agenda_vencida"]
        .sort_values(
            by=["data_limite_considerada", "score_prioridade_preliminar"],
            ascending=[True, False],
            na_position="last",
        )
        .reset_index(drop=True)
    )

    _validar_integridade_fechamento(
        df_entrada=carteira,
        df_carteira_roteirizavel=df_carteira_roteirizavel,
        df_carteira_agendamento_futuro=df_carteira_agendamento_futuro,
        df_carteira_agendas_vencidas=df_carteira_agendas_vencidas,
    )

    resumo = _montar_resumo_m3(
        df_carteira_triagem=df_carteira_triagem,
        df_carteira_roteirizavel=df_carteira_roteirizavel,
        df_carteira_agendamento_futuro=df_carteira_agendamento_futuro,
        df_carteira_agendas_vencidas=df_carteira_agendas_vencidas,
        data_base_roteirizacao=data_base_roteirizacao,
        caminhos_pipeline=caminhos_pipeline or {},
    )

    resultado = {
        "df_carteira_triagem": df_carteira_triagem,
        "df_carteira_roteirizavel": df_carteira_roteirizavel,
        "df_carteira_agendamento_futuro": df_carteira_agendamento_futuro,
        "df_carteira_agendas_vencidas": df_carteira_agendas_vencidas,
    }

    return df_carteira_triagem, {
        "resumo_m3": resumo,
        "outputs_m3": resultado,
    }


def _validar_colunas_minimas(df: pd.DataFrame) -> None:
    colunas_minimas = [
        "data_agenda",
        "data_leadtime",
        "data_limite_considerada",
        "tipo_data_limite",
        "dias_ate_data_alvo",
        "transit_time_dias",
        "folga_dias",
        "status_folga",
        "peso_kg",
        "peso_calculado",
    ]

    faltam = [c for c in colunas_minimas if c not in df.columns]
    if faltam:
        raise Exception(
            "Faltam colunas mínimas na carteira enriquecida para executar o M3:\n- "
            + "\n- ".join(faltam)
        )


def _classificar_status_triagem(row: pd.Series) -> str:
    data_agenda = row["data_agenda"]
    data_leadtime = row["data_leadtime"]
    folga = row["folga_dias"]

    # Sem data_agenda: entra pela DLE, se existir
    if pd.isna(data_agenda):
        if pd.notna(data_leadtime):
            return "roteirizavel"
        return "excecao_triagem"

    # Com data_agenda: classifica pela folga
    if pd.notna(folga) and 0 <= folga < 2:
        return "roteirizavel"

    if pd.notna(folga) and folga >= 2:
        return "agendamento_futuro"

    if pd.notna(folga) and folga < 0:
        return "agenda_vencida"

    return "excecao_triagem"


def _definir_motivo_triagem(row: pd.Series) -> str:
    status = row["status_triagem"]
    data_agenda = row["data_agenda"]
    data_leadtime = row["data_leadtime"]
    folga = row["folga_dias"]

    if status == "roteirizavel":
        if pd.isna(data_agenda) and pd.notna(data_leadtime):
            return "leadtime_preenchido_sem_data_agenda"
        return "agendada_com_folga_positiva_menor_que_2"

    if status == "agendamento_futuro":
        if pd.notna(folga) and folga == 2:
            return "agendada_com_folga_igual_a_2"
        return "agendada_com_folga_maior_ou_igual_a_2"

    if status == "agenda_vencida":
        return "agendada_com_folga_negativa"

    if status == "excecao_triagem":
        if pd.isna(data_agenda) and pd.isna(data_leadtime):
            return "sem_data_agenda_e_sem_dle"
        if pd.notna(data_agenda) and pd.isna(folga):
            return "agendada_sem_folga_calculada"
        return "cenario_nao_mapeado_pela_regra_atual"

    return "sem_motivo"


def _definir_grupo_saida(status: str) -> str:
    if status == "roteirizavel":
        return "df_carteira_roteirizavel"
    if status == "agendamento_futuro":
        return "df_carteira_agendamento_futuro"
    if status == "agenda_vencida":
        return "df_carteira_agendas_vencidas"
    return "df_carteira_excecoes_triagem"


def _definir_prioridade_label(row: pd.Series) -> str:
    status = row["status_triagem"]
    data_agenda = row["data_agenda"]
    folga = row["folga_dias"]

    if status != "roteirizavel":
        return "fora_da_carteira_roteirizavel"

    if pd.notna(data_agenda):
        return "prioridade_1_agendada"

    if pd.notna(folga) and folga <= 0:
        return "prioridade_2_leadtime_critico"

    if pd.notna(folga) and folga > 0:
        return "prioridade_3_leadtime_com_folga"

    return "prioridade_sem_classificacao"


def _definir_ranking_operacional(row: pd.Series) -> int:
    status = row["status_triagem"]
    data_agenda = row["data_agenda"]
    folga = row["folga_dias"]

    if status != "roteirizavel":
        return 9
    if pd.notna(data_agenda):
        return 1
    if pd.notna(folga) and folga <= 0:
        return 2
    if pd.notna(folga) and folga > 0:
        return 3
    return 9


def _validar_integridade_fechamento(
    df_entrada: pd.DataFrame,
    df_carteira_roteirizavel: pd.DataFrame,
    df_carteira_agendamento_futuro: pd.DataFrame,
    df_carteira_agendas_vencidas: pd.DataFrame,
) -> None:
    qtd_entrada = len(df_entrada)
    qtd_saida = (
        len(df_carteira_roteirizavel)
        + len(df_carteira_agendamento_futuro)
        + len(df_carteira_agendas_vencidas)
        + len(df_entrada.loc[df_entrada["status_triagem"] == "excecao_triagem"])
    )

    if qtd_entrada != qtd_saida:
        raise Exception(
            f"Falha de integridade do M3: entrada={qtd_entrada} e saída={qtd_saida}."
        )

    violacoes_roteirizavel_agendadas = df_carteira_roteirizavel.loc[
        df_carteira_roteirizavel["data_agenda"].notna()
        & (
            df_carteira_roteirizavel["folga_dias"].isna()
            | (df_carteira_roteirizavel["folga_dias"] < 0)
            | (df_carteira_roteirizavel["folga_dias"] >= 2)
        )
    ]
    if len(violacoes_roteirizavel_agendadas) > 0:
        raise Exception(
            "A carteira roteirizável ficou contaminada com linhas agendadas fora da faixa permitida (0 <= folga < 2)."
        )

    violacoes_roteirizavel_leadtime = df_carteira_roteirizavel.loc[
        df_carteira_roteirizavel["data_agenda"].isna()
        & df_carteira_roteirizavel["data_leadtime"].isna()
    ]
    if len(violacoes_roteirizavel_leadtime) > 0:
        raise Exception(
            "A carteira roteirizável ficou contaminada com linhas sem data_agenda e sem DLE."
        )

    violacoes_futuro = df_carteira_agendamento_futuro.loc[
        ~(
            df_carteira_agendamento_futuro["data_agenda"].notna()
            & (df_carteira_agendamento_futuro["folga_dias"] >= 2)
        )
    ]
    if len(violacoes_futuro) > 0:
        raise Exception(
            "A carteira de agendamento futuro ficou com linhas incompatíveis com a regra (data_agenda preenchida e folga >= 2)."
        )

    violacoes_vencidas = df_carteira_agendas_vencidas.loc[
        ~(
            df_carteira_agendas_vencidas["data_agenda"].notna()
            & (df_carteira_agendas_vencidas["folga_dias"] < 0)
        )
    ]
    if len(violacoes_vencidas) > 0:
        raise Exception(
            "A carteira de agendas vencidas ficou com linhas incompatíveis com a regra (data_agenda preenchida e folga < 0)."
        )

    linhas_sem_peso_calculado_roteirizavel = int(df_carteira_roteirizavel["peso_calculado"].isna().sum())
    if linhas_sem_peso_calculado_roteirizavel > 0:
        raise Exception(
            "A carteira roteirizável ficou contaminada com linhas sem peso_calculado, "
            "o que não é permitido para a entrada do bloco 4."
        )


def _montar_resumo_m3(
    df_carteira_triagem: pd.DataFrame,
    df_carteira_roteirizavel: pd.DataFrame,
    df_carteira_agendamento_futuro: pd.DataFrame,
    df_carteira_agendas_vencidas: pd.DataFrame,
    data_base_roteirizacao: datetime,
    caminhos_pipeline: Dict[str, Any],
) -> Dict[str, Any]:
    status_counts = (
        df_carteira_triagem["status_triagem"]
        .fillna("sem_classificacao")
        .value_counts(dropna=False)
        .to_dict()
    )

    prioridade_counts = (
        df_carteira_roteirizavel["prioridade_label"]
        .fillna("sem_classificacao")
        .value_counts(dropna=False)
        .to_dict()
    )

    qtd_folga_2_futuro = int(
        (
            df_carteira_agendamento_futuro["data_agenda"].notna()
            & (pd.to_numeric(df_carteira_agendamento_futuro["folga_dias"], errors="coerce") == 2)
        ).sum()
    )

    return {
        "modulo": "M3",
        "data_base_roteirizacao": pd.to_datetime(data_base_roteirizacao).isoformat(),
        "linhas_entrada": int(len(df_carteira_triagem)),
        "linhas_saida_total": int(len(df_carteira_triagem)),
        "carteira_roteirizavel": int(len(df_carteira_roteirizavel)),
        "carteira_agendamento_futuro": int(len(df_carteira_agendamento_futuro)),
        "carteira_agendas_vencidas": int(len(df_carteira_agendas_vencidas)),
        "carteira_excecoes_triagem": int((df_carteira_triagem["status_triagem"] == "excecao_triagem").sum()),
        "agendadas_na_roteirizavel": int(df_carteira_roteirizavel["data_agenda"].notna().sum()),
        "leadtime_na_roteirizavel": int(df_carteira_roteirizavel["data_agenda"].isna().sum()),
        "agendadas_folga_igual_2_em_agendamento_futuro": qtd_folga_2_futuro,
        "status_triagem_counts": status_counts,
        "prioridade_roteirizavel_counts": prioridade_counts,
        "peso_kg_nulo_roteirizavel": int(df_carteira_roteirizavel["peso_kg"].isna().sum()),
        "peso_calculado_nulo_roteirizavel": int(df_carteira_roteirizavel["peso_calculado"].isna().sum()),
        "regra_agendada_roteirizavel": "0 <= folga_dias < 2",
        "regra_agendamento_futuro": "folga_dias >= 2",
        "regra_agenda_vencida": "folga_dias < 0",
        "caminhos_pipeline": caminhos_pipeline,
    }
