from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd

from app.pipeline.m1_padronizacao import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento
from app.pipeline.m3_triagem import executar_m3_triagem
from app.pipeline.m3_1_validacao_fronteira import executar_m3_1_validacao_fronteira
from app.pipeline.m4_manifestos_fechados import executar_m4_manifestos_fechados
from app.pipeline.m5_1_triagem_cidades import executar_m5_1_triagem_cidades
from app.pipeline.m5_2_composicao_cidades import executar_m5_2_composicao_cidades
from app.pipeline.m5_3_triagem_subregioes import executar_m5_3_triagem_subregioes
from app.pipeline.m5_3_composicao_subregioes import executar_m5_3_composicao_subregioes
from app.pipeline.m5_4a_triagem_mesorregioes import executar_m5_4a_triagem_mesorregioes
from app.pipeline.m5_4b_composicao_mesorregioes import executar_m5_4b_composicao_mesorregioes
from app.pipeline.m6_1_consolidacao_manifestos import executar_m6_1_consolidacao_manifestos
from app.pipeline.m6_2_complemento_ocupacao import executar_m6_2_complemento_ocupacao
from app.pipeline.m7_sequenciamento_entregas import executar_m7_sequenciamento_entregas
from app.schemas import RoteirizacaoRequest
from app.services.payload_service import PipelineContext, normalizar_payload_para_pipeline


def _agora() -> float:
    return time.perf_counter()


def _duracao_ms(inicio: float) -> float:
    return round((time.perf_counter() - inicio) * 1000, 2)


def _safe_len(obj: Any) -> int:
    try:
        return int(len(obj))
    except Exception:
        return 0


def _is_debug(payload: RoteirizacaoRequest) -> bool:
    for attr in ("modo_debug", "debug", "retornar_debug", "incluir_debug"):
        try:
            valor = getattr(payload, attr, False)
            if isinstance(valor, bool):
                return valor
            if isinstance(valor, str):
                return valor.strip().lower() in {"1", "true", "sim", "yes"}
        except Exception:
            continue
    return False


def _log(
    modulo: str,
    status: str,
    mensagem: str,
    quantidade_entrada: int | None = None,
    quantidade_saida: int | None = None,
    tempo_ms: float | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    registro = {
        "modulo": modulo,
        "status": status,
        "mensagem": mensagem,
        "quantidade_entrada": quantidade_entrada,
        "quantidade_saida": quantidade_saida,
    }
    if tempo_ms is not None:
        registro["tempo_ms"] = tempo_ms
    if extra:
        registro["extra"] = extra
    return registro


def _snapshot_dataframe(df: pd.DataFrame, nome: str, max_colunas: int = 30) -> Dict[str, Any]:
    if df is None:
        return {
            "nome": nome,
            "linhas": 0,
            "colunas": [],
            "qtd_colunas_total": 0,
        }

    return {
        "nome": nome,
        "linhas": int(len(df)),
        "colunas": list(df.columns[:max_colunas]),
        "qtd_colunas_total": int(len(df.columns)),
    }


def _serializar_dataframe_para_records(
    df: pd.DataFrame,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    df2 = df.copy()

    if limit is not None:
        df2 = df2.head(limit)

    for col in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[col]):
            df2[col] = df2[col].astype(str)

    df2 = df2.where(pd.notnull(df2), None)
    return df2.to_dict(orient="records")


def _montar_resumo_dataframe(df: pd.DataFrame, nome: str) -> Dict[str, Any]:
    return {
        "nome": nome,
        "total_linhas": _safe_len(df),
        "qtd_colunas": int(len(df.columns)) if isinstance(df, pd.DataFrame) else 0,
    }


def _executar_m0_adapter(contexto: PipelineContext) -> Dict[str, Any]:
    inventario = {
        "rodada_id": contexto.rodada_id,
        "upload_id": contexto.upload_id,
        "usuario_id": contexto.usuario_id,
        "filial_id": contexto.filial_id,
        "tipo_roteirizacao": contexto.tipo_roteirizacao,
        "data_execucao": contexto.data_execucao.isoformat(),
        "data_base_roteirizacao": contexto.data_base.isoformat(),
        "filial": contexto.filial,
        "inputs": {
            "carteira": _snapshot_dataframe(contexto.df_carteira_raw, "df_carteira_raw"),
            "regionalidades": _snapshot_dataframe(contexto.df_geo_raw, "df_geo_raw"),
            "parametros": _snapshot_dataframe(contexto.df_parametros_raw, "df_parametros_raw"),
            "veiculos": _snapshot_dataframe(contexto.df_veiculos_raw, "df_veiculos_raw"),
        },
        "caminhos_pipeline": contexto.caminhos_pipeline,
    }

    return {
        "inventario": inventario,
        "df_carteira_raw": contexto.df_carteira_raw,
        "df_geo_raw": contexto.df_geo_raw,
        "df_parametros_raw": contexto.df_parametros_raw,
        "df_veiculos_raw": contexto.df_veiculos_raw,
    }


def executar_pipeline(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    inicio_total = _agora()
    logs: List[Dict[str, Any]] = []
    metricas_tempo: Dict[str, float] = {}
    debug = _is_debug(payload)

    # =========================================================================================
    # PAYLOAD -> CONTEXTO
    # =========================================================================================
    t0 = _agora()
    contexto = normalizar_payload_para_pipeline(payload)
    tempo_payload = _duracao_ms(t0)
    metricas_tempo["payload_service_ms"] = tempo_payload

    logs.append(
        _log(
            modulo="payload_service",
            status="ok",
            mensagem="Payload normalizado para o contexto interno do pipeline",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(contexto.df_carteira_raw),
            tempo_ms=tempo_payload,
            extra={
                "rodada_id": contexto.rodada_id,
                "filial_id": contexto.filial_id,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
            },
        )
    )

    # =========================================================================================
    # M0
    # =========================================================================================
    t0 = _agora()
    resultado_m0 = _executar_m0_adapter(contexto)
    tempo_m0 = _duracao_ms(t0)
    metricas_tempo["m0_adapter_ms"] = tempo_m0

    logs.append(
        _log(
            modulo="m0_adapter",
            status="ok",
            mensagem="M0 adaptado executado com sucesso",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(contexto.df_carteira_raw),
            tempo_ms=tempo_m0,
            extra={
                "filial": contexto.filial,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
            },
        )
    )

    # =========================================================================================
    # M1
    # =========================================================================================
    t0 = _agora()
    resultado_m1 = executar_m1_padronizacao(
        df_carteira_raw=resultado_m0["df_carteira_raw"],
        df_geo_raw=resultado_m0["df_geo_raw"],
        df_parametros_raw=resultado_m0["df_parametros_raw"],
        df_veiculos_raw=resultado_m0["df_veiculos_raw"],
    )
    tempo_m1 = _duracao_ms(t0)
    metricas_tempo["m1_padronizacao_ms"] = tempo_m1

    df_carteira_tratada = resultado_m1["df_carteira_tratada"]
    df_geo_tratado = resultado_m1["df_geo_tratado"]
    df_parametros_tratados = resultado_m1["df_parametros_tratados"]
    df_veiculos_tratados = resultado_m1["df_veiculos_tratados"]

    resumo_m1 = {
        "carteira_colunas": int(len(df_carteira_tratada.columns)),
        "geo_colunas": int(len(df_geo_tratado.columns)),
        "parametros_colunas": int(len(df_parametros_tratados.columns)),
        "veiculos_colunas": int(len(df_veiculos_tratados.columns)),
    }

    logs.append(
        _log(
            modulo="m1_padronizacao",
            status="ok",
            mensagem="M1 executado com sucesso",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(df_carteira_tratada),
            tempo_ms=tempo_m1,
            extra=resumo_m1,
        )
    )

    # =========================================================================================
    # M2
    # =========================================================================================
    t0 = _agora()
    df_carteira_enriquecida, resumo_m2 = executar_m2_enriquecimento(
        df_carteira_tratada=df_carteira_tratada,
        df_geo_tratado=df_geo_tratado,
        df_parametros_tratados=df_parametros_tratados,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m2 = _duracao_ms(t0)
    metricas_tempo["m2_enriquecimento_ms"] = tempo_m2

    logs.append(
        _log(
            modulo="m2_enriquecimento",
            status="ok",
            mensagem="M2 executado com sucesso",
            quantidade_entrada=_safe_len(df_carteira_tratada),
            quantidade_saida=_safe_len(df_carteira_enriquecida),
            tempo_ms=tempo_m2,
            extra=resumo_m2,
        )
    )

    # =========================================================================================
    # M3
    # =========================================================================================
    t0 = _agora()
    df_carteira_triagem, meta_m3 = executar_m3_triagem(
        df_carteira_enriquecida=df_carteira_enriquecida,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m3 = _duracao_ms(t0)
    metricas_tempo["m3_triagem_ms"] = tempo_m3

    outputs_m3 = meta_m3["outputs_m3"]
    resumo_m3 = meta_m3["resumo_m3"]

    df_carteira_roteirizavel = outputs_m3["df_carteira_roteirizavel"]
    df_carteira_agendamento_futuro = outputs_m3["df_carteira_agendamento_futuro"]
    df_carteira_agendas_vencidas = outputs_m3["df_carteira_agendas_vencidas"]

    logs.append(
        _log(
            modulo="m3_triagem",
            status="ok",
            mensagem="M3 executado com sucesso",
            quantidade_entrada=_safe_len(df_carteira_enriquecida),
            quantidade_saida=_safe_len(df_carteira_triagem),
            tempo_ms=tempo_m3,
            extra=resumo_m3,
        )
    )

    # =========================================================================================
    # M3.1
    # =========================================================================================
    t0 = _agora()
    df_input_oficial_bloco_4, meta_m31 = executar_m3_1_validacao_fronteira(
        df_carteira_roteirizavel=df_carteira_roteirizavel,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m31 = _duracao_ms(t0)
    metricas_tempo["m3_1_validacao_fronteira_ms"] = tempo_m31

    resumo_m31 = meta_m31["resumo_m31"]

    logs.append(
        _log(
            modulo="m3_1_validacao_fronteira",
            status="ok",
            mensagem="M3.1 executado com sucesso e input oficial do bloco 4 foi consolidado",
            quantidade_entrada=_safe_len(df_carteira_roteirizavel),
            quantidade_saida=_safe_len(df_input_oficial_bloco_4),
            tempo_ms=tempo_m31,
            extra=resumo_m31,
        )
    )

    # =========================================================================================
    # M4
    # =========================================================================================
    t0 = _agora()
    outputs_m4, meta_m4 = executar_m4_manifestos_fechados(
        df_input_oficial_bloco_4=df_input_oficial_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
        rodada_id=contexto.rodada_id,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        configuracao_frota=payload.configuracao_frota,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m4 = _duracao_ms(t0)
    metricas_tempo["m4_manifestos_fechados_ms"] = tempo_m4

    resumo_m4 = meta_m4["resumo_m4"]
    df_remanescente_roteirizavel_bloco_4 = outputs_m4["df_remanescente_roteirizavel_bloco_4"]

    df_manifestos_m4 = outputs_m4.get("df_manifestos_fechados_bloco_4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = outputs_m4.get("df_manifestos_m4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = pd.DataFrame()

    df_itens_manifestados_m4 = outputs_m4.get("df_itens_manifestos_fechados_bloco_4")
    if df_itens_manifestados_m4 is None or not isinstance(df_itens_manifestados_m4, pd.DataFrame):
        df_itens_manifestados_m4 = outputs_m4.get("df_itens_manifestados_bloco_4")
    if df_itens_manifestados_m4 is None or not isinstance(df_itens_manifestados_m4, pd.DataFrame):
        df_itens_manifestados_m4 = outputs_m4.get("df_itens_manifestados_m4")
    if df_itens_manifestados_m4 is None or not isinstance(df_itens_manifestados_m4, pd.DataFrame):
        df_itens_manifestados_m4 = pd.DataFrame()

    logs.append(
        _log(
            modulo="m4_manifestos_fechados",
            status="ok",
            mensagem="M4 executado com sucesso",
            quantidade_entrada=_safe_len(df_input_oficial_bloco_4),
            quantidade_saida=_safe_len(df_remanescente_roteirizavel_bloco_4),
            tempo_ms=tempo_m4,
            extra={
                **resumo_m4,
                "total_remanescente_global_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
            },
        )
    )

    # =========================================================================================
    # M5.1
    # =========================================================================================
    t0 = _agora()
    outputs_m5_1, meta_m5_1 = executar_m5_1_triagem_cidades(
        df_remanescente_roteirizavel_bloco_4=df_remanescente_roteirizavel_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_1 = _duracao_ms(t0)
    metricas_tempo["m5_1_triagem_cidades_ms"] = tempo_m5_1

    resumo_m5_1 = meta_m5_1["resumo_m5_1"]
    df_saldo_elegivel_composicao_m5_1 = outputs_m5_1["df_saldo_elegivel_composicao_m5_1"]
    df_perfis_elegiveis_por_cidade_m5_1 = outputs_m5_1["df_perfis_elegiveis_por_cidade_m5_1"]

    logs.append(
        _log(
            modulo="m5_1_triagem_cidades",
            status="ok",
            mensagem="M5.1 executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_roteirizavel_bloco_4),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_1),
            tempo_ms=tempo_m5_1,
            extra=resumo_m5_1,
        )
    )

    # =========================================================================================
    # M5.2
    # =========================================================================================
    t0 = _agora()
    outputs_m5_2, meta_m5_2 = executar_m5_2_composicao_cidades(
        df_saldo_elegivel_composicao_m5_1=df_saldo_elegivel_composicao_m5_1,
        df_perfis_elegiveis_por_cidade_m5_1=df_perfis_elegiveis_por_cidade_m5_1,
        rodada_id=contexto.rodada_id,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m5_2 = _duracao_ms(t0)
    metricas_tempo["m5_2_composicao_cidades_ms"] = tempo_m5_2

    resumo_m5_2 = meta_m5_2["resumo_m5_2"]
    df_premanifestos_m5_2 = outputs_m5_2["df_premanifestos_m5_2"]
    df_itens_premanifestos_m5_2 = outputs_m5_2["df_itens_premanifestos_m5_2"]
    df_remanescente_m5_2 = outputs_m5_2["df_remanescente_m5_2"]
    df_tentativas_m5_2 = outputs_m5_2["df_tentativas_m5_2"]

    logs.append(
        _log(
            modulo="m5_2_composicao_cidades",
            status="ok",
            mensagem="M5.2 executado com sucesso",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_1),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_2),
            tempo_ms=tempo_m5_2,
            extra={
                **resumo_m5_2,
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
            },
        )
    )

    # =========================================================================================
    # M5.3A
    # =========================================================================================
    t0 = _agora()
    outputs_m5_3a, meta_m5_3a = executar_m5_3_triagem_subregioes(
        df_remanescente_m5_2=df_remanescente_m5_2,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_3a = _duracao_ms(t0)
    metricas_tempo["m5_3_triagem_subregioes_ms"] = tempo_m5_3a

    resumo_m5_3a = meta_m5_3a["resumo_m5_3"]

    df_subregioes_consolidadas_m5_3 = outputs_m5_3a["df_subregioes_consolidadas_m5_3"]
    df_perfis_elegiveis_por_subregiao_m5_3 = outputs_m5_3a["df_perfis_elegiveis_por_subregiao_m5_3"]
    df_saldo_elegivel_composicao_m5_3 = outputs_m5_3a["df_saldo_elegivel_composicao_m5_3"]
    df_tentativas_triagem_subregioes_m5_3 = outputs_m5_3a["df_tentativas_triagem_subregioes_m5_3"]

    logs.append(
        _log(
            modulo="m5_3_triagem_subregioes",
            status="ok",
            mensagem="M5.3A executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_m5_2),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_3),
            tempo_ms=tempo_m5_3a,
            extra={
                **resumo_m5_3a,
                "total_subregioes_consolidadas_m5_3": _safe_len(df_subregioes_consolidadas_m5_3),
                "total_tentativas_triagem_subregioes_m5_3": _safe_len(df_tentativas_triagem_subregioes_m5_3),
            },
        )
    )

    # =========================================================================================
    # M5.3B
    # =========================================================================================
    t0 = _agora()
    outputs_m5_3b, meta_m5_3b = executar_m5_3_composicao_subregioes(
        df_saldo_elegivel_composicao_m5_3=df_saldo_elegivel_composicao_m5_3,
        df_perfis_elegiveis_por_subregiao_m5_3=df_perfis_elegiveis_por_subregiao_m5_3,
        rodada_id=contexto.rodada_id,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m5_3b = _duracao_ms(t0)
    metricas_tempo["m5_3b_composicao_subregioes_ms"] = tempo_m5_3b

    resumo_m5_3b = meta_m5_3b["resumo_m5_3b"]
    df_premanifestos_m5_3 = outputs_m5_3b["df_premanifestos_m5_3"]
    df_itens_premanifestos_m5_3 = outputs_m5_3b["df_itens_premanifestos_m5_3"]
    df_tentativas_m5_3 = outputs_m5_3b["df_tentativas_m5_3"]
    df_remanescente_m5_3 = outputs_m5_3b["df_remanescente_m5_3"]

    logs.append(
        _log(
            modulo="m5_3b_composicao_subregioes",
            status="ok",
            mensagem="M5.3B executado com sucesso",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_3),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_3),
            tempo_ms=tempo_m5_3b,
            extra={
                **resumo_m5_3b,
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
            },
        )
    )

    # =========================================================================================
    # M5.4A
    # =========================================================================================
    t0 = _agora()
    outputs_m5_4a, meta_m5_4a = executar_m5_4a_triagem_mesorregioes(
        df_remanescente_m5_3=df_remanescente_m5_3,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_4a = _duracao_ms(t0)
    metricas_tempo["m5_4a_triagem_mesorregioes_ms"] = tempo_m5_4a

    resumo_m5_4a = meta_m5_4a["resumo_m5_4a"]

    df_mesorregioes_consolidadas_m5_4 = outputs_m5_4a["df_mesorregioes_consolidadas_m5_4"]
    df_perfis_elegiveis_por_mesorregiao_m5_4 = outputs_m5_4a["df_perfis_elegiveis_por_mesorregiao_m5_4"]
    df_saldo_elegivel_composicao_m5_4 = outputs_m5_4a["df_saldo_elegivel_composicao_m5_4"]
    df_tentativas_triagem_mesorregioes_m5_4 = outputs_m5_4a["df_tentativas_triagem_mesorregioes_m5_4"]

    logs.append(
        _log(
            modulo="m5_4a_triagem_mesorregioes",
            status="ok",
            mensagem="M5.4A executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_m5_3),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_4),
            tempo_ms=tempo_m5_4a,
            extra={
                **resumo_m5_4a,
                "total_mesorregioes_consolidadas_m5_4": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_tentativas_triagem_mesorregioes_m5_4": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
            },
        )
    )

    # =========================================================================================
    # M5.4B
    # =========================================================================================
    t0 = _agora()
    outputs_m5_4b, meta_m5_4b = executar_m5_4b_composicao_mesorregioes(
        df_saldo_elegivel_composicao_m5_4=df_saldo_elegivel_composicao_m5_4,
        df_perfis_elegiveis_por_mesorregiao_m5_4=df_perfis_elegiveis_por_mesorregiao_m5_4,
        rodada_id=contexto.rodada_id,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m5_4b = _duracao_ms(t0)
    metricas_tempo["m5_4b_composicao_mesorregioes_ms"] = tempo_m5_4b

    resumo_m5_4b = meta_m5_4b["resumo_m5_4b"]
    df_premanifestos_m5_4 = outputs_m5_4b["df_premanifestos_m5_4"]
    df_itens_premanifestos_m5_4 = outputs_m5_4b["df_itens_premanifestos_m5_4"]
    df_tentativas_m5_4 = outputs_m5_4b["df_tentativas_m5_4"]
    df_remanescente_m5_4 = outputs_m5_4b["df_remanescente_m5_4"]

    logs.append(
        _log(
            modulo="m5_4b_composicao_mesorregioes",
            status="ok",
            mensagem="M5.4B executado com sucesso",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_4),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_4),
            tempo_ms=tempo_m5_4b,
            extra={
                **resumo_m5_4b,
                "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_m5_4),
            },
        )
    )

    # =========================================================================================
    # M6.1
    # =========================================================================================
    t0 = _agora()
    outputs_m6_1, meta_m6_1 = executar_m6_1_consolidacao_manifestos(
        df_manifestos_m4=df_manifestos_m4,
        df_itens_manifestados_m4=df_itens_manifestados_m4,
        df_premanifestos_m5_2=df_premanifestos_m5_2,
        df_itens_premanifestos_m5_2=df_itens_premanifestos_m5_2,
        df_premanifestos_m5_3=df_premanifestos_m5_3,
        df_itens_premanifestos_m5_3=df_itens_premanifestos_m5_3,
        df_premanifestos_m5_4=df_premanifestos_m5_4,
        df_itens_premanifestos_m5_4=df_itens_premanifestos_m5_4,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m6_1 = _duracao_ms(t0)
    metricas_tempo["m6_1_consolidacao_manifestos_ms"] = tempo_m6_1

    resumo_m6_1 = meta_m6_1["resumo_m6_1"]
    df_manifestos_base_m6 = outputs_m6_1["df_manifestos_base_m6"]
    df_itens_manifestos_base_m6 = outputs_m6_1["df_itens_manifestos_base_m6"]
    df_estatisticas_manifestos_antes_m6 = outputs_m6_1["df_estatisticas_manifestos_antes_m6"]
    df_pares_elegiveis_otimizacao_m6 = outputs_m6_1["df_pares_elegiveis_otimizacao_m6"]

    logs.append(
        _log(
            modulo="m6_1_consolidacao_manifestos",
            status="ok",
            mensagem="M6.1 executado com sucesso",
            quantidade_entrada=(
                _safe_len(df_manifestos_m4)
                + _safe_len(df_premanifestos_m5_2)
                + _safe_len(df_premanifestos_m5_3)
                + _safe_len(df_premanifestos_m5_4)
            ),
            quantidade_saida=_safe_len(df_manifestos_base_m6),
            tempo_ms=tempo_m6_1,
            extra={
                **resumo_m6_1,
                "total_itens_manifestos_base_m6": _safe_len(df_itens_manifestos_base_m6),
                "total_estatisticas_manifestos_antes_m6": _safe_len(df_estatisticas_manifestos_antes_m6),
                "total_pares_elegiveis_otimizacao_m6": _safe_len(df_pares_elegiveis_otimizacao_m6),
            },
        )
    )

    # =========================================================================================
    # M6.2
    # =========================================================================================
    t0 = _agora()
    resultado_m6_2 = executar_m6_2_complemento_ocupacao(
        df_manifestos_base_m6=df_manifestos_base_m6,
        df_estatisticas_manifestos_antes_m6=df_estatisticas_manifestos_antes_m6,
        df_itens_manifestos_base_m6=df_itens_manifestos_base_m6,
        df_remanescente_m5_4=df_remanescente_m5_4,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
        ocupacao_alvo_perc=85.0,
    )
    tempo_m6_2 = _duracao_ms(t0)
    metricas_tempo["m6_2_complemento_ocupacao_ms"] = tempo_m6_2

    outputs_m6_2 = resultado_m6_2["outputs_m6_2"]
    resumo_m6_2 = resultado_m6_2["resumo_m6_2"]

    df_manifestos_m6_2 = outputs_m6_2["df_manifestos_m6_2"]
    df_itens_manifestos_m6_2 = outputs_m6_2["df_itens_manifestos_m6_2"]
    df_remanescente_m6_2 = outputs_m6_2["df_remanescente_m6_2"]
    df_remanescente_m5_original_m6_2 = outputs_m6_2["df_remanescente_m5_original_m6_2"]
    df_tentativas_m6_2 = outputs_m6_2["df_tentativas_m6_2"]
    df_movimentos_aceitos_m6_2 = outputs_m6_2["df_movimentos_aceitos_m6_2"]

    logs.append(
        _log(
            modulo="m6_2_complemento_ocupacao",
            status="ok",
            mensagem="M6.2 executado com sucesso",
            quantidade_entrada=_safe_len(df_manifestos_base_m6),
            quantidade_saida=_safe_len(df_manifestos_m6_2),
            tempo_ms=tempo_m6_2,
            extra={
                **resumo_m6_2,
                "total_itens_manifestos_m6_2": _safe_len(df_itens_manifestos_m6_2),
                "total_remanescente_m6_2": _safe_len(df_remanescente_m6_2),
                "total_remanescente_m5_original_m6_2": _safe_len(df_remanescente_m5_original_m6_2),
                "total_tentativas_m6_2": _safe_len(df_tentativas_m6_2),
                "total_movimentos_aceitos_m6_2": _safe_len(df_movimentos_aceitos_m6_2),
            },
        )
    )

    # =========================================================================================
    # M7
    # =========================================================================================
    t0 = _agora()
    outputs_m7, meta_m7 = executar_m7_sequenciamento_entregas(
        df_manifestos_m6_2=df_manifestos_m6_2,
        df_itens_manifestos_m6_2=df_itens_manifestos_m6_2,
        df_geo_tratado=df_geo_tratado,
        df_geo_raw=contexto.df_geo_raw,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m7 = _duracao_ms(t0)
    metricas_tempo["m7_sequenciamento_entregas_ms"] = tempo_m7

    resumo_m7 = meta_m7["resumo_m7"]
    auditoria_m7 = meta_m7["auditoria_m7"]

    df_manifestos_m7 = outputs_m7["df_manifestos_m7"]
    df_itens_manifestos_sequenciados_m7 = outputs_m7["df_itens_manifestos_sequenciados_m7"]
    df_manifestos_sequenciamento_resumo_m7 = outputs_m7["df_manifestos_sequenciamento_resumo_m7"]
    df_tentativas_sequenciamento_m7 = outputs_m7["df_tentativas_sequenciamento_m7"]
    df_diagnostico_recuperacao_coordenadas_m7 = outputs_m7["df_diagnostico_recuperacao_coordenadas_m7"]

    logs.append(
        _log(
            modulo="m7_sequenciamento_entregas",
            status="ok",
            mensagem="M7 executado com sucesso",
            quantidade_entrada=_safe_len(df_itens_manifestos_m6_2),
            quantidade_saida=_safe_len(df_itens_manifestos_sequenciados_m7),
            tempo_ms=tempo_m7,
            extra={
                **resumo_m7,
                "total_manifestos_m7": _safe_len(df_manifestos_m7),
                "total_itens_manifestos_sequenciados_m7": _safe_len(df_itens_manifestos_sequenciados_m7),
                "total_manifestos_sequenciamento_resumo_m7": _safe_len(df_manifestos_sequenciamento_resumo_m7),
                "total_tentativas_sequenciamento_m7": _safe_len(df_tentativas_sequenciamento_m7),
                "total_diagnostico_recuperacao_coordenadas_m7": _safe_len(df_diagnostico_recuperacao_coordenadas_m7),
            },
        )
    )

    # =========================================================================================
    # SERIALIZAÇÃO FINAL - SOMENTE M7
    # =========================================================================================
    t0 = _agora()

    manifestos_m7 = _serializar_dataframe_para_records(df_manifestos_m7, limit=None)
    itens_manifestos_sequenciados_m7 = _serializar_dataframe_para_records(df_itens_manifestos_sequenciados_m7, limit=None)
    manifestos_sequenciamento_resumo_m7 = _serializar_dataframe_para_records(df_manifestos_sequenciamento_resumo_m7, limit=None)
    tentativas_sequenciamento_m7 = _serializar_dataframe_para_records(df_tentativas_sequenciamento_m7, limit=None)
    diagnostico_recuperacao_coordenadas_m7 = _serializar_dataframe_para_records(df_diagnostico_recuperacao_coordenadas_m7, limit=None)

    tempo_serializacao = _duracao_ms(t0)
    metricas_tempo["serializacao_resposta_ms"] = tempo_serializacao

    tempo_total = _duracao_ms(inicio_total)
    metricas_tempo["tempo_total_pipeline_ms"] = tempo_total

    resposta: Dict[str, Any] = {
        "status": "ok",
        "mensagem": "Motor executou com sucesso até o M7 sequenciamento de entregas.",
        "pipeline_real_ate": "M7",
        "modo_resposta": "auditoria_m7_sequenciamento_entregas",
        "resposta_truncada": False,
        "resumo_execucao": {
            "rodada_id": contexto.rodada_id,
            "upload_id": contexto.upload_id,
            "usuario_id": contexto.usuario_id,
            "filial_id": contexto.filial_id,
            "tipo_roteirizacao": contexto.tipo_roteirizacao,
            "data_base_roteirizacao": contexto.data_base.isoformat(),
            "tempos_ms": metricas_tempo,
        },
        "resumo_negocio": {
            "total_carteira": _safe_len(contexto.df_carteira_raw),
            "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
            "total_triagem_m3": _safe_len(df_carteira_triagem),
            "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
            "total_agendamento_futuro_m3": _safe_len(df_carteira_agendamento_futuro),
            "total_agendas_vencidas_m3": _safe_len(df_carteira_agendas_vencidas),
            "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
            "total_remanescente_global_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
            "total_manifestos_m4": _safe_len(df_manifestos_m4),
            "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
            "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
            "total_itens_manifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
            "total_subregioes_consolidadas_m5_3": _safe_len(df_subregioes_consolidadas_m5_3),
            "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
            "total_itens_roteirizados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
            "total_mesorregioes_consolidadas_m5_4": _safe_len(df_mesorregioes_consolidadas_m5_4),
            "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
            "total_itens_roteirizados_m5_4": _safe_len(df_itens_premanifestos_m5_4),
            "total_remanescente_m5_4": _safe_len(df_remanescente_m5_4),
            "total_manifestos_base_m6": _safe_len(df_manifestos_base_m6),
            "total_itens_manifestos_base_m6": _safe_len(df_itens_manifestos_base_m6),
            "total_manifestos_m6_2": _safe_len(df_manifestos_m6_2),
            "total_itens_manifestos_m6_2": _safe_len(df_itens_manifestos_m6_2),
            "total_remanescente_m6_2": _safe_len(df_remanescente_m6_2),
            "total_manifestos_m7": _safe_len(df_manifestos_m7),
            "total_itens_manifestos_sequenciados_m7": _safe_len(df_itens_manifestos_sequenciados_m7),
            "total_manifestos_sequenciamento_resumo_m7": _safe_len(df_manifestos_sequenciamento_resumo_m7),
            "total_tentativas_sequenciamento_m7": _safe_len(df_tentativas_sequenciamento_m7),
            "total_diagnostico_recuperacao_coordenadas_m7": _safe_len(df_diagnostico_recuperacao_coordenadas_m7),
            "resumo_m3": resumo_m3,
            "resumo_m31": resumo_m31,
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "resumo_m5_4a": resumo_m5_4a,
            "resumo_m5_4b": resumo_m5_4b,
            "resumo_m6_1": resumo_m6_1,
            "resumo_m6_2": resumo_m6_2,
            "resumo_m7": resumo_m7,
        },
        "contexto_rodada": {
            "filial": contexto.filial,
            "parametros_rodada": contexto.parametros_rodada,
        },
        "manifestos_m7": manifestos_m7,
        "itens_manifestos_sequenciados_m7": itens_manifestos_sequenciados_m7,
        "manifestos_sequenciamento_resumo_m7": manifestos_sequenciamento_resumo_m7,
        "tentativas_sequenciamento_m7": tentativas_sequenciamento_m7,
        "diagnostico_recuperacao_coordenadas_m7": diagnostico_recuperacao_coordenadas_m7,
        "auditoria_serializacao": {
            "manifestos_m7_total": _safe_len(df_manifestos_m7),
            "manifestos_m7_retornado": len(manifestos_m7),
            "itens_manifestos_sequenciados_m7_total": _safe_len(df_itens_manifestos_sequenciados_m7),
            "itens_manifestos_sequenciados_m7_retornado": len(itens_manifestos_sequenciados_m7),
            "manifestos_sequenciamento_resumo_m7_total": _safe_len(df_manifestos_sequenciamento_resumo_m7),
            "manifestos_sequenciamento_resumo_m7_retornado": len(manifestos_sequenciamento_resumo_m7),
            "tentativas_sequenciamento_m7_total": _safe_len(df_tentativas_sequenciamento_m7),
            "tentativas_sequenciamento_m7_retornado": len(tentativas_sequenciamento_m7),
            "diagnostico_recuperacao_coordenadas_m7_total": _safe_len(df_diagnostico_recuperacao_coordenadas_m7),
            "diagnostico_recuperacao_coordenadas_m7_retornado": len(diagnostico_recuperacao_coordenadas_m7),
        },
        "auditoria_m7": auditoria_m7,
        "logs": logs,
    }

    if debug:
        resposta["debug"] = {
            "snapshots": {
                "df_manifestos_m7": _snapshot_dataframe(df_manifestos_m7, "df_manifestos_m7"),
                "df_itens_manifestos_sequenciados_m7": _snapshot_dataframe(
                    df_itens_manifestos_sequenciados_m7,
                    "df_itens_manifestos_sequenciados_m7",
                ),
                "df_manifestos_sequenciamento_resumo_m7": _snapshot_dataframe(
                    df_manifestos_sequenciamento_resumo_m7,
                    "df_manifestos_sequenciamento_resumo_m7",
                ),
                "df_tentativas_sequenciamento_m7": _snapshot_dataframe(
                    df_tentativas_sequenciamento_m7,
                    "df_tentativas_sequenciamento_m7",
                ),
                "df_diagnostico_recuperacao_coordenadas_m7": _snapshot_dataframe(
                    df_diagnostico_recuperacao_coordenadas_m7,
                    "df_diagnostico_recuperacao_coordenadas_m7",
                ),
            },
            "resumos_dataframes": {
                "df_manifestos_m7": _montar_resumo_dataframe(df_manifestos_m7, "df_manifestos_m7"),
                "df_itens_manifestos_sequenciados_m7": _montar_resumo_dataframe(
                    df_itens_manifestos_sequenciados_m7,
                    "df_itens_manifestos_sequenciados_m7",
                ),
                "df_manifestos_sequenciamento_resumo_m7": _montar_resumo_dataframe(
                    df_manifestos_sequenciamento_resumo_m7,
                    "df_manifestos_sequenciamento_resumo_m7",
                ),
                "df_tentativas_sequenciamento_m7": _montar_resumo_dataframe(
                    df_tentativas_sequenciamento_m7,
                    "df_tentativas_sequenciamento_m7",
                ),
                "df_diagnostico_recuperacao_coordenadas_m7": _montar_resumo_dataframe(
                    df_diagnostico_recuperacao_coordenadas_m7,
                    "df_diagnostico_recuperacao_coordenadas_m7",
                ),
            },
        }

    return resposta
