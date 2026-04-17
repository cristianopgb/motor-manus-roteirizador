# ============================================================
# ORQUESTRADOR DO PIPELINE — M1 → M7
# Converte o payload dict em DataFrames e encadeia M1→M7
# ============================================================
from __future__ import annotations

import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from app.pipeline.m1_padronizacao   import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento
from app.pipeline.m3_triagem        import executar_m3_triagem, executar_m3_1_validacao_fronteira
from app.pipeline.m4_manifestos     import executar_m4_manifestos_fechados
from app.pipeline.m5_composicao     import (
    executar_m5_1_triagem_cidades,
    executar_m5_2_composicao_cidades,
    executar_m5_3_composicao_subregioes,
    executar_m5_4_composicao_mesorregioes,
)
from app.pipeline.m6_consolidacao   import (
    executar_m6_1_consolidacao_manifestos,
    executar_m6_2_complemento_ocupacao,
)
from app.pipeline.m7_sequenciamento import executar_m7_sequenciamento


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df_clean = df.copy()
    for col in df_clean.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df_clean[col] = df_clean[col].astype(str)
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), other=None)
    return df_clean.to_dict(orient="records")


def _enc(nome: str, entrada: int, saida: int, rem: int, det: Optional[Dict] = None) -> Dict:
    return {"etapa": nome, "entrada": entrada, "saida_principal": saida, "remanescente": rem, "detalhes": det or {}}


def _payload_to_dataframes(payload: Dict[str, Any]):
    """Converte o payload dict em DataFrames para o M1."""
    # Carteira
    carteira_raw = payload.get("carteira") or []
    df_carteira = pd.DataFrame(carteira_raw) if carteira_raw else pd.DataFrame()

    # Geo
    geo_raw = payload.get("geo") or []
    df_geo = pd.DataFrame(geo_raw) if geo_raw else pd.DataFrame()

    # Veículos
    veiculos_raw = payload.get("veiculos") or []
    df_veiculos = pd.DataFrame(veiculos_raw) if veiculos_raw else pd.DataFrame()

    # Parâmetros — M1 espera um DataFrame com colunas "parametro" e "valor"
    params_raw = payload.get("parametros") or {}
    if isinstance(params_raw, dict):
        # Incluir também dados da filial nos parâmetros
        filial = payload.get("filial") or {}
        params_merged = {**params_raw}
        if filial.get("latitude") is not None:
            params_merged["latitude_filial"] = filial["latitude"]
        if filial.get("longitude") is not None:
            params_merged["longitude_filial"] = filial["longitude"]
        if filial.get("uf"):
            params_merged["uf_filial"] = filial["uf"]
        if filial.get("cidade"):
            params_merged["cidade_filial"] = filial["cidade"]
        df_parametros = pd.DataFrame([
            {"parametro": k, "valor": v}
            for k, v in params_merged.items()
            if v is not None
        ])
    else:
        df_parametros = pd.DataFrame()

    return df_carteira, df_geo, df_parametros, df_veiculos


def executar_pipeline_completo(payload: Dict[str, Any]) -> Dict[str, Any]:
    inicio = datetime.utcnow()
    encadeamento: List[Dict] = []
    erros: List[Dict] = []
    status = "sucesso"

    # ── Converter payload em DataFrames ────────────────────────
    try:
        df_carteira_raw, df_geo_raw, df_parametros_raw, df_veiculos_raw = _payload_to_dataframes(payload)
    except Exception as e:
        return {
            "status": "erro_fatal", "etapa_falha": "conversao_payload",
            "erro": str(e), "traceback": traceback.format_exc(),
            "manifestos": [], "itens": [], "nao_roteirizados": [],
            "encadeamento": [], "resumo": {}, "erros": [],
        }

    # ── M1 ─────────────────────────────────────────────────────
    try:
        r1 = executar_m1_padronizacao(df_carteira_raw, df_geo_raw, df_parametros_raw, df_veiculos_raw)
        df_carteira = r1["df_carteira_tratada"]
        df_veiculos = r1["df_veiculos_tratados"]
        df_geo      = r1["df_geo_tratado"]
        param_dict  = r1["param_dict"]
        encadeamento.append(_enc("M1_padronizacao",
            len(df_carteira_raw),
            len(df_carteira),
            max(0, len(df_carteira_raw) - len(df_carteira)),
            {"total_linhas_entrada": len(df_carteira_raw), "total_linhas_validas": len(df_carteira)}))
    except Exception as e:
        return {
            "status": "erro_fatal", "etapa_falha": "M1_padronizacao",
            "erro": str(e), "traceback": traceback.format_exc(),
            "manifestos": [], "itens": [], "nao_roteirizados": [],
            "encadeamento": [], "resumo": {}, "erros": [],
        }

    # ── M2 ─────────────────────────────────────────────────────
    try:
        r2 = executar_m2_enriquecimento(df_carteira, df_geo, param_dict)
        df_enriquecida = r2["df_carteira_enriquecida"]
        encadeamento.append(_enc("M2_enriquecimento", len(df_carteira), len(df_enriquecida), 0,
            {"data_base": str(r2.get("data_base","")), "fator_km": r2.get("fator_km",1.2), "km_dia": r2.get("km_dia",400)}))
    except Exception as e:
        erros.append({"etapa": "M2_enriquecimento", "erro": str(e)}); status = "erro_parcial"
        df_enriquecida = df_carteira

    # ── M3 ─────────────────────────────────────────────────────
    try:
        r3 = executar_m3_triagem(df_enriquecida)
        df_roteirizavel = r3["df_carteira_roteirizavel"]
        df_nao_rot_m3 = pd.concat([
            r3.get("df_agendamento_futuro", pd.DataFrame()),
            r3.get("df_agenda_vencida",     pd.DataFrame()),
            r3.get("df_excecao_triagem",    pd.DataFrame()),
        ], ignore_index=True)
        encadeamento.append(_enc("M3_triagem",
            r3["resumo_m3"]["total_entrada"],
            r3["resumo_m3"]["roteirizavel"],
            r3["resumo_m3"]["total_entrada"] - r3["resumo_m3"]["roteirizavel"],
            r3["resumo_m3"]))
    except Exception as e:
        erros.append({"etapa": "M3_triagem", "erro": str(e)}); status = "erro_parcial"
        df_roteirizavel = df_enriquecida; df_nao_rot_m3 = pd.DataFrame()

    # ── M3.1 ────────────────────────────────────────────────────
    try:
        r31 = executar_m3_1_validacao_fronteira(df_roteirizavel)
        df_bloco4  = r31["df_input_oficial_bloco_4"]
        df_rej_m31 = r31["df_rejeitados_m3_1"]
        encadeamento.append(_enc("M3_1_validacao_fronteira",
            r31["resumo_m3_1"]["total_entrada"],
            r31["resumo_m3_1"]["validos"],
            r31["resumo_m3_1"]["rejeitados"],
            r31["resumo_m3_1"]))
    except Exception as e:
        erros.append({"etapa": "M3_1_validacao_fronteira", "erro": str(e)}); status = "erro_parcial"
        df_bloco4 = df_roteirizavel; df_rej_m31 = pd.DataFrame()

    # ── M4 ─────────────────────────────────────────────────────
    try:
        r4 = executar_m4_manifestos_fechados(df_bloco4, df_veiculos)
        df_man_m4  = r4["df_manifestos_m4"]
        df_ite_m4  = r4["df_itens_m4"]
        df_rem_m4  = r4["df_remanescente_m4"]
        encadeamento.append(_enc("M4_manifestos_fechados",
            r4["resumo_m4"]["total_entrada"],
            r4["resumo_m4"]["itens_manifestados"],
            r4["resumo_m4"]["remanescente"],
            r4["resumo_m4"]))
    except Exception as e:
        erros.append({"etapa": "M4_manifestos_fechados", "erro": str(e)}); status = "erro_parcial"
        df_man_m4 = pd.DataFrame(); df_ite_m4 = pd.DataFrame(); df_rem_m4 = df_bloco4

    # ── M5.1 ────────────────────────────────────────────────────
    try:
        r51 = executar_m5_1_triagem_cidades(df_rem_m4, df_veiculos)
        df_eleg_m51   = r51["df_saldo_elegivel_m5_1"]
        df_nao_el_m51 = r51["df_nao_elegivel_m5_1"]
        df_perf_m51   = r51["df_perfis_por_cidade_m5_1"]
        encadeamento.append(_enc("M5_1_triagem_cidades",
            r51["resumo_m5_1"]["total_entrada"],
            r51["resumo_m5_1"]["elegivel"],
            r51["resumo_m5_1"]["nao_elegivel"],
            r51["resumo_m5_1"]))
    except Exception as e:
        erros.append({"etapa": "M5_1_triagem_cidades", "erro": str(e)}); status = "erro_parcial"
        df_eleg_m51 = df_rem_m4; df_nao_el_m51 = pd.DataFrame(); df_perf_m51 = pd.DataFrame()

    # ── M5.2 ────────────────────────────────────────────────────
    try:
        r52 = executar_m5_2_composicao_cidades(df_eleg_m51, df_perf_m51, df_veiculos)
        df_pm52 = r52["df_premanifestos_m5_2"]
        df_it52 = r52["df_itens_m5_2"]
        df_re52 = r52["df_remanescente_m5_2"]
        encadeamento.append(_enc("M5_2_composicao_cidades",
            r52["resumo_m5_2"]["total_entrada"],
            r52["resumo_m5_2"]["itens"],
            r52["resumo_m5_2"]["remanescente"],
            r52["resumo_m5_2"]))
    except Exception as e:
        erros.append({"etapa": "M5_2_composicao_cidades", "erro": str(e)}); status = "erro_parcial"
        df_pm52 = pd.DataFrame(); df_it52 = pd.DataFrame(); df_re52 = df_eleg_m51

    # ── M5.3 ────────────────────────────────────────────────────
    try:
        r53 = executar_m5_3_composicao_subregioes(df_re52, df_nao_el_m51, df_veiculos)
        df_pm53 = r53["df_premanifestos_m5_3"]
        df_it53 = r53["df_itens_m5_3"]
        df_re53 = r53["df_remanescente_m5_3"]
        encadeamento.append(_enc("M5_3_composicao_subregioes",
            r53["resumo_m5_3"]["total_entrada"],
            r53["resumo_m5_3"]["pre_manifestos"],
            r53["resumo_m5_3"]["remanescente"],
            r53["resumo_m5_3"]))
    except Exception as e:
        erros.append({"etapa": "M5_3_composicao_subregioes", "erro": str(e)}); status = "erro_parcial"
        df_pm53 = pd.DataFrame(); df_it53 = pd.DataFrame()
        df_re53 = pd.concat([df_re52, df_nao_el_m51], ignore_index=True)

    # ── M5.4 ────────────────────────────────────────────────────
    try:
        r54 = executar_m5_4_composicao_mesorregioes(df_re53, df_veiculos)
        df_pm54 = r54["df_premanifestos_m5_4"]
        df_it54 = r54["df_itens_m5_4"]
        df_re54 = r54["df_remanescente_m5_4"]
        encadeamento.append(_enc("M5_4_composicao_mesorregioes",
            r54["resumo_m5_4"]["total_entrada"],
            r54["resumo_m5_4"]["pre_manifestos"],
            r54["resumo_m5_4"]["remanescente"],
            r54["resumo_m5_4"]))
    except Exception as e:
        erros.append({"etapa": "M5_4_composicao_mesorregioes", "erro": str(e)}); status = "erro_parcial"
        df_pm54 = pd.DataFrame(); df_it54 = pd.DataFrame(); df_re54 = df_re53

    # ── M6.1 ────────────────────────────────────────────────────
    try:
        r61 = executar_m6_1_consolidacao_manifestos(
            df_pm52, df_it52, df_pm53, df_it53, df_pm54, df_it54,
            df_man_m4, df_ite_m4, df_veiculos,
        )
        df_man_cons = r61["df_manifestos_consolidados"]
        df_ite_cons = r61["df_itens_consolidados"]
        encadeamento.append(_enc("M6_1_consolidacao",
            r61["resumo_m6_1"]["total_manifestos"],
            r61["resumo_m6_1"]["total_manifestos"], 0, r61["resumo_m6_1"]))
    except Exception as e:
        erros.append({"etapa": "M6_1_consolidacao", "erro": str(e)}); status = "erro_parcial"
        df_man_cons = pd.DataFrame(); df_ite_cons = pd.DataFrame()

    # ── M6.2 ────────────────────────────────────────────────────
    try:
        r62 = executar_m6_2_complemento_ocupacao(df_man_cons, df_ite_cons, df_re54, df_veiculos)
        df_man_fin  = r62["df_manifestos_finais"]
        df_ite_fin  = r62["df_itens_finais"]
        df_nao_rot  = r62["df_nao_roteirizados"]
        encadeamento.append(_enc("M6_2_complemento_ocupacao",
            r62["resumo_m6_2"]["total_itens"],
            r62["resumo_m6_2"]["itens_complementados"],
            r62["resumo_m6_2"]["nao_roteirizados"],
            r62["resumo_m6_2"]))
    except Exception as e:
        erros.append({"etapa": "M6_2_complemento_ocupacao", "erro": str(e)}); status = "erro_parcial"
        df_man_fin = df_man_cons; df_ite_fin = df_ite_cons; df_nao_rot = df_re54

    # ── M7 ─────────────────────────────────────────────────────
    try:
        r7 = executar_m7_sequenciamento(df_man_fin, df_ite_fin, param_dict)
        df_man_seq = r7["df_manifestos_sequenciados"]
        df_ite_seq = r7["df_itens_sequenciados"]
        encadeamento.append(_enc("M7_sequenciamento",
            r7["resumo_m7"]["total_manifestos"],
            r7["resumo_m7"]["total_itens"], 0, r7["resumo_m7"]))
    except Exception as e:
        erros.append({"etapa": "M7_sequenciamento", "erro": str(e)}); status = "erro_parcial"
        df_man_seq = df_man_fin; df_ite_seq = df_ite_fin

    # ── Consolidar não roteirizados ─────────────────────────────
    df_nao_rot_final = pd.concat(
        [f for f in [df_nao_rot_m3, df_rej_m31, df_nao_rot] if f is not None and not f.empty],
        ignore_index=True,
    )

    # ── Resumo ──────────────────────────────────────────────────
    fim = datetime.utcnow()
    tempo_ms = int((fim - inicio).total_seconds() * 1000)

    km_total = 0.0
    if not df_man_seq.empty and "km_rota_total" in df_man_seq.columns:
        km_total = float(pd.to_numeric(df_man_seq["km_rota_total"], errors="coerce").fillna(0).sum())

    ocup_med = 0.0
    if not df_man_seq.empty and "ocupacao_oficial_perc" in df_man_seq.columns:
        ocup_med = float(pd.to_numeric(df_man_seq["ocupacao_oficial_perc"], errors="coerce").fillna(0).mean())

    return {
        "status":           status,
        "manifestos":       _df_to_records(df_man_seq),
        "itens":            _df_to_records(df_ite_seq),
        "nao_roteirizados": _df_to_records(df_nao_rot_final),
        "encadeamento":     encadeamento,
        "resumo": {
            "total_linhas_entrada":     len(df_carteira_raw),
            "total_manifestos":         len(df_man_seq),
            "total_itens_manifestados": len(df_ite_seq),
            "total_nao_roteirizados":   len(df_nao_rot_final),
            "km_total_frota":           round(km_total, 2),
            "ocupacao_media_perc":      round(ocup_med, 2),
            "tempo_processamento_ms":   tempo_ms,
        },
        "erros": erros,
    }
