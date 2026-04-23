from __future__ import annotations

import time
import uuid
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
from app.pipeline.m6_2_complemento_ocupacao import COLS_MANIFESTOS_OBRIGATORIAS, executar_m6_2_complemento_ocupacao
from app.pipeline.m7_sequenciamento_entregas import executar_m7_sequenciamento_entregas
from app.schemas import RoteirizacaoRequest
from app.services.auditoria_pipeline_service import persistir_snapshot_modulo_auditoria
from app.services.payload_service import PipelineContext, normalizar_payload_para_pipeline
from app.utils.json_safe import sanitizar_json_safe

PIPELINE_FLAGS = {
    "executar_m4": True,
    "executar_m5_1": True,
    "executar_m5_2": False,
    "executar_m5_3a": False,
    "executar_m5_3b": False,
    "executar_m5_4a": False,
    "executar_m5_4b": False,
    "executar_m6_1": False,
    "executar_m6_2": False,
    "executar_m7": False,
}


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
    records = df2.to_dict(orient="records")
    records_sanitizados, _ = sanitizar_json_safe(records)
    return records_sanitizados


def _montar_resumo_dataframe(df: pd.DataFrame, nome: str) -> Dict[str, Any]:
    return {
        "nome": nome,
        "total_linhas": _safe_len(df),
        "qtd_colunas": int(len(df.columns)) if isinstance(df, pd.DataFrame) else 0,
    }


def _tem_schema_minimo(df: Any, colunas_obrigatorias: List[str]) -> bool:
    if not isinstance(df, pd.DataFrame):
        return False
    return all(c in df.columns for c in colunas_obrigatorias)


def _copiar_ou_vazio(df: Any, colunas: List[str] | None = None) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df.copy()
    return pd.DataFrame(columns=colunas or [])


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


def _executar_pipeline_core(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    inicio_total = _agora()
    logs: List[Dict[str, Any]] = []
    metricas_tempo: Dict[str, float] = {}
    debug = _is_debug(payload)
    teste_id_auditoria = str(uuid.uuid4())
    auditoria_por_modulo: Dict[str, int] = {}
    auditoria_por_snapshot: Dict[str, int] = {}
    auditoria_flat_rastreamento: Dict[str, Any] = {"colunas_persistidas": set()}
    print("[AUDITORIA] teste_id:", teste_id_auditoria)
    print("[PIPELINE FLAGS] executar_m4=", PIPELINE_FLAGS["executar_m4"])
    print("[PIPELINE FLAGS] executar_m5_1=", PIPELINE_FLAGS["executar_m5_1"])
    print("[PIPELINE FLAGS] executar_m5_2=", PIPELINE_FLAGS["executar_m5_2"])
    print("[PIPELINE FLAGS] executar_m5_3a=", PIPELINE_FLAGS["executar_m5_3a"])
    print("[PIPELINE FLAGS] executar_m5_3b=", PIPELINE_FLAGS["executar_m5_3b"])
    print("[PIPELINE FLAGS] executar_m5_4a=", PIPELINE_FLAGS["executar_m5_4a"])
    print("[PIPELINE FLAGS] executar_m5_4b=", PIPELINE_FLAGS["executar_m5_4b"])
    print("[PIPELINE FLAGS] executar_m6_1=", PIPELINE_FLAGS["executar_m6_1"])
    print("[PIPELINE FLAGS] executar_m6_2=", PIPELINE_FLAGS["executar_m6_2"])
    print("[PIPELINE FLAGS] executar_m7=", PIPELINE_FLAGS["executar_m7"])

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
    contexto_auditoria = {
        "filial_id": contexto.filial_id,
        "usuario_id": contexto.usuario_id,
        "tipo_roteirizacao": contexto.tipo_roteirizacao,
        "data_base_roteirizacao": contexto.data_base.isoformat(),
    }
    total_payload = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="payload_service",
        ordem_modulo=0,
        df_etapa=contexto.df_carteira_raw,
        snapshot_nome="payload_service",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["payload_service"] = auditoria_por_modulo.get("payload_service", 0) + total_payload
    auditoria_por_snapshot["payload_service"] = auditoria_por_snapshot.get("payload_service", 0) + total_payload
    print(f"[AUDITORIA FLAT] snapshot=payload_service linhas={total_payload}")

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
    total_m0 = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m0_adapter",
        ordem_modulo=1,
        df_etapa=resultado_m0["df_carteira_raw"],
        snapshot_nome="m0_adapter",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m0_adapter"] = auditoria_por_modulo.get("m0_adapter", 0) + total_m0
    auditoria_por_snapshot["m0_adapter"] = auditoria_por_snapshot.get("m0_adapter", 0) + total_m0
    print(f"[AUDITORIA FLAT] snapshot=m0_adapter linhas={total_m0}")

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
    total_m1 = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m1_padronizacao",
        ordem_modulo=2,
        df_etapa=df_carteira_tratada,
        snapshot_nome="m1_padronizacao",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m1_padronizacao"] = auditoria_por_modulo.get("m1_padronizacao", 0) + total_m1
    auditoria_por_snapshot["m1_padronizacao"] = auditoria_por_snapshot.get("m1_padronizacao", 0) + total_m1
    print(f"[AUDITORIA FLAT] snapshot=m1_padronizacao linhas={total_m1}")

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
    total_m2 = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m2_enriquecimento",
        ordem_modulo=3,
        df_etapa=df_carteira_enriquecida,
        snapshot_nome="m2_enriquecimento",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m2_enriquecimento"] = auditoria_por_modulo.get("m2_enriquecimento", 0) + total_m2
    auditoria_por_snapshot["m2_enriquecimento"] = auditoria_por_snapshot.get("m2_enriquecimento", 0) + total_m2
    print(f"[AUDITORIA FLAT] snapshot=m2_enriquecimento linhas={total_m2}")

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
    total_m3 = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m3_triagem",
        ordem_modulo=4,
        df_etapa=df_carteira_triagem,
        snapshot_nome="m3_triagem",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m3_triagem"] = auditoria_por_modulo.get("m3_triagem", 0) + total_m3
    auditoria_por_snapshot["m3_triagem"] = auditoria_por_snapshot.get("m3_triagem", 0) + total_m3
    print(f"[AUDITORIA FLAT] snapshot=m3_triagem linhas={total_m3}")


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
    total_m3_1 = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m3_1_validacao_fronteira",
        ordem_modulo=5,
        df_etapa=df_input_oficial_bloco_4,
        snapshot_nome="m3_1_validacao_fronteira",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m3_1_validacao_fronteira"] = auditoria_por_modulo.get("m3_1_validacao_fronteira", 0) + total_m3_1
    auditoria_por_snapshot["m3_1_validacao_fronteira"] = auditoria_por_snapshot.get("m3_1_validacao_fronteira", 0) + total_m3_1
    print(f"[AUDITORIA FLAT] snapshot=m3_1_validacao_fronteira linhas={total_m3_1}")

    if not PIPELINE_FLAGS["executar_m4"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        print(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente na etapa base de auditoria (M3.1).",
            "pipeline_real_ate": "M3.1",
            "modo_resposta": "auditoria_base_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
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
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
            },
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M4
    # =========================================================================================
    # OBS AUDITORIA FUTURA (M4+):
    # Quando as etapas M4/M5/M6/M7 forem religadas para persistência de auditoria,
    # priorizar sempre os DATAFRAMES OFICIAIS DE ITENS de cada etapa (não apenas resumos/manifestos agregados),
    # mantendo o histórico linha a linha do dataset operacional.
    t0 = _agora()
    print(f"[M4] executando M4 com input oficial do bloco 4 linhas={_safe_len(df_input_oficial_bloco_4)}")
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

    df_manifestos_m4 = outputs_m4.get("df_manifestos_m4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = outputs_m4.get("df_manifestos_fechados_bloco_4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = pd.DataFrame()

    df_itens_m4 = outputs_m4.get("df_itens_m4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = outputs_m4.get("df_itens_manifestos_fechados_bloco_4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = outputs_m4.get("df_itens_manifestados_bloco_4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = pd.DataFrame()

    df_remanescente_m4 = outputs_m4.get("df_remanescente_m4")
    if df_remanescente_m4 is None or not isinstance(df_remanescente_m4, pd.DataFrame):
        df_remanescente_m4 = outputs_m4.get("df_remanescente_roteirizavel_bloco_4")
    if df_remanescente_m4 is None or not isinstance(df_remanescente_m4, pd.DataFrame):
        df_remanescente_m4 = pd.DataFrame()
    df_remanescente_roteirizavel_bloco_4 = df_remanescente_m4
    df_itens_manifestados_m4 = df_itens_m4

    print(f"[M4] df_manifestos_m4 linhas={_safe_len(df_manifestos_m4)}")
    print(f"[M4] df_itens_m4 linhas={_safe_len(df_itens_m4)}")
    print(f"[M4] df_remanescente_m4 linhas={_safe_len(df_remanescente_m4)}")

    logs.append(
        _log(
            modulo="m4_manifestos_fechados",
            status="ok",
            mensagem="M4 executado com sucesso",
            quantidade_entrada=_safe_len(df_input_oficial_bloco_4),
            quantidade_saida=_safe_len(df_remanescente_m4),
            tempo_ms=tempo_m4,
            extra={
                **resumo_m4,
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_m4),
                "total_remanescente_global_m4": _safe_len(df_remanescente_m4),
            },
        )
    )
    total_m4_manifestos = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_manifestos_m4,
        snapshot_nome="m4_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_manifestos
    auditoria_por_snapshot["m4_manifestos"] = auditoria_por_snapshot.get("m4_manifestos", 0) + total_m4_manifestos
    print(f"[AUDITORIA FLAT] snapshot=m4_manifestos linhas={total_m4_manifestos}")

    total_m4_itens = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_itens_m4,
        snapshot_nome="m4_itens",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_itens
    auditoria_por_snapshot["m4_itens"] = auditoria_por_snapshot.get("m4_itens", 0) + total_m4_itens
    print(f"[AUDITORIA FLAT] snapshot=m4_itens linhas={total_m4_itens}")

    total_m4_remanescente = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_remanescente_m4,
        snapshot_nome="m4_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_remanescente
    auditoria_por_snapshot["m4_remanescente"] = auditoria_por_snapshot.get("m4_remanescente", 0) + total_m4_remanescente
    print(f"[AUDITORIA FLAT] snapshot=m4_remanescente linhas={total_m4_remanescente}")

    if not PIPELINE_FLAGS["executar_m5_1"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        print(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M4 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M4",
            "modo_resposta": "auditoria_m4_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
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
                "total_entrada_bloco_4": int(resumo_m4.get("roteirizavel_entrada_m4", _safe_len(df_input_oficial_bloco_4))),
                "dedicados_encontrados_m4": int(meta_m4.get("metricas_m4", {}).get("contadores_m4", {}).get("qtd_manifestos_exclusivos", 0)),
                "total_manifestos_m4": int(resumo_m4.get("manifestos_fechados_gerados_m4", _safe_len(df_manifestos_m4))),
                "total_itens_manifestados_m4": int(resumo_m4.get("itens_manifestados_m4", _safe_len(df_itens_m4))),
                "total_remanescente_m4": int(resumo_m4.get("remanescente_roteirizavel_m4", _safe_len(df_remanescente_m4))),
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
            },
            "resumo_m4": {
                **resumo_m4,
                "total_entrada": int(resumo_m4.get("roteirizavel_entrada_m4", _safe_len(df_input_oficial_bloco_4))),
                "dedicados_encontrados": int(meta_m4.get("metricas_m4", {}).get("contadores_m4", {}).get("qtd_manifestos_exclusivos", 0)),
                "manifestos_fechados": int(resumo_m4.get("manifestos_fechados_gerados_m4", _safe_len(df_manifestos_m4))),
                "itens_manifestados": int(resumo_m4.get("itens_manifestados_m4", _safe_len(df_itens_m4))),
                "remanescente": int(resumo_m4.get("remanescente_roteirizavel_m4", _safe_len(df_remanescente_m4))),
            },
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M5.1
    # =========================================================================================
    t0 = _agora()
    print(f"[M5.1] executando M5.1 com remanescente do M4 linhas={_safe_len(df_remanescente_roteirizavel_bloco_4)}")
    outputs_m5_1, meta_m5_1 = executar_m5_1_triagem_cidades(
        df_remanescente_roteirizavel_bloco_4=df_remanescente_roteirizavel_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_1 = _duracao_ms(t0)
    metricas_tempo["m5_1_triagem_cidades_ms"] = tempo_m5_1

    resumo_m5_1 = meta_m5_1["resumo_m5_1"]
    df_cidades_consolidadas_m5_1 = outputs_m5_1["df_cidades_consolidadas_m5_1"]
    df_perfis_viaveis_por_cidade_m5_1 = outputs_m5_1["df_perfis_viaveis_por_cidade_m5_1"]
    df_saldo_elegivel_composicao_m5_1 = outputs_m5_1["df_saldo_elegivel_composicao_m5_1"]
    df_saldo_nao_elegivel_m5_1 = outputs_m5_1["df_saldo_nao_elegivel_m5_1"]
    df_perfis_elegiveis_por_cidade_m5_1 = outputs_m5_1["df_perfis_elegiveis_por_cidade_m5_1"]
    df_perfis_descartados_por_cidade_m5_1 = outputs_m5_1["df_perfis_descartados_por_cidade_m5_1"]
    df_tentativas_triagem_cidades_m5_1 = outputs_m5_1["df_tentativas_triagem_cidades_m5_1"]

    print(f"[M5.1] df_cidades_consolidadas_m5_1 linhas={_safe_len(df_cidades_consolidadas_m5_1)}")
    print(f"[M5.1] df_perfis_elegiveis_por_cidade_m5_1 linhas={_safe_len(df_perfis_elegiveis_por_cidade_m5_1)}")
    print(f"[M5.1] df_perfis_descartados_por_cidade_m5_1 linhas={_safe_len(df_perfis_descartados_por_cidade_m5_1)}")
    print(f"[M5.1] df_saldo_elegivel_composicao_m5_1 linhas={_safe_len(df_saldo_elegivel_composicao_m5_1)}")
    print(f"[M5.1] df_saldo_nao_elegivel_m5_1 linhas={_safe_len(df_saldo_nao_elegivel_m5_1)}")
    print(f"[M5.1] df_tentativas_triagem_cidades_m5_1 linhas={_safe_len(df_tentativas_triagem_cidades_m5_1)}")

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
    total_m5_1_cidades_consolidadas = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_cidades_consolidadas_m5_1,
        snapshot_nome="m5_1_cidades_consolidadas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_cidades_consolidadas
    auditoria_por_snapshot["m5_1_cidades_consolidadas"] = auditoria_por_snapshot.get("m5_1_cidades_consolidadas", 0) + total_m5_1_cidades_consolidadas
    print(f"[AUDITORIA FLAT] snapshot=m5_1_cidades_consolidadas linhas={total_m5_1_cidades_consolidadas}")

    total_m5_1_perfis_viaveis = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_viaveis_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_viaveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_viaveis
    auditoria_por_snapshot["m5_1_perfis_viaveis"] = auditoria_por_snapshot.get("m5_1_perfis_viaveis", 0) + total_m5_1_perfis_viaveis

    total_m5_1_perfis_elegiveis = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_elegiveis_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_elegiveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_elegiveis
    auditoria_por_snapshot["m5_1_perfis_elegiveis"] = auditoria_por_snapshot.get("m5_1_perfis_elegiveis", 0) + total_m5_1_perfis_elegiveis
    print(f"[AUDITORIA FLAT] snapshot=m5_1_perfis_elegiveis linhas={total_m5_1_perfis_elegiveis}")

    total_m5_1_perfis_descartados = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_descartados_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_descartados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_descartados
    auditoria_por_snapshot["m5_1_perfis_descartados"] = auditoria_por_snapshot.get("m5_1_perfis_descartados", 0) + total_m5_1_perfis_descartados

    total_m5_1_saldo_elegivel = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_saldo_elegivel_composicao_m5_1,
        snapshot_nome="m5_1_saldo_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_saldo_elegivel
    auditoria_por_snapshot["m5_1_saldo_elegivel"] = auditoria_por_snapshot.get("m5_1_saldo_elegivel", 0) + total_m5_1_saldo_elegivel
    print(f"[AUDITORIA FLAT] snapshot=m5_1_saldo_elegivel linhas={total_m5_1_saldo_elegivel}")

    total_m5_1_saldo_nao_elegivel = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_saldo_nao_elegivel_m5_1,
        snapshot_nome="m5_1_saldo_nao_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_saldo_nao_elegivel
    auditoria_por_snapshot["m5_1_saldo_nao_elegivel"] = auditoria_por_snapshot.get("m5_1_saldo_nao_elegivel", 0) + total_m5_1_saldo_nao_elegivel
    print(f"[AUDITORIA FLAT] snapshot=m5_1_saldo_nao_elegivel linhas={total_m5_1_saldo_nao_elegivel}")

    total_m5_1_tentativas = persistir_snapshot_modulo_auditoria(
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_tentativas_triagem_cidades_m5_1,
        snapshot_nome="m5_1_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_tentativas
    auditoria_por_snapshot["m5_1_tentativas"] = auditoria_por_snapshot.get("m5_1_tentativas", 0) + total_m5_1_tentativas
    print(f"[AUDITORIA FLAT] snapshot=m5_1_tentativas linhas={total_m5_1_tentativas}")

    if not PIPELINE_FLAGS["executar_m5_2"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        print(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.1 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.1",
            "modo_resposta": "auditoria_m5_1_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
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
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "linhas_entrada_m5_1": int(resumo_m5_1.get("linhas_entrada", 0)),
                "cidades_total_m5_1": int(resumo_m5_1.get("cidades_total", 0)),
                "cidades_elegiveis_m5_1": int(resumo_m5_1.get("cidades_elegiveis", 0)),
                "cidades_nao_elegiveis_m5_1": int(resumo_m5_1.get("cidades_nao_elegiveis", 0)),
                "perfis_testados_total_m5_1": int(resumo_m5_1.get("perfis_testados_total", 0)),
                "perfis_elegiveis_total_m5_1": int(resumo_m5_1.get("perfis_elegiveis_total", 0)),
                "perfis_descartados_total_m5_1": int(resumo_m5_1.get("perfis_descartados_total", 0)),
                "linhas_saldo_elegivel_composicao_m5_1": int(resumo_m5_1.get("linhas_saldo_elegivel_composicao_m5_1", 0)),
                "linhas_saldo_nao_elegivel_m5_1": int(resumo_m5_1.get("linhas_saldo_nao_elegivel_m5_1", 0)),
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
                "resumo_m4": resumo_m4,
            },
            "resumo_m5_1": resumo_m5_1,
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M5.2
    # =========================================================================================
    t0 = _agora()
    m5_2_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_1, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_1.empty
    m5_2_tem_perfis = isinstance(df_perfis_elegiveis_por_cidade_m5_1, pd.DataFrame) and not df_perfis_elegiveis_por_cidade_m5_1.empty
    if m5_2_tem_saldo and m5_2_tem_perfis:
        outputs_m5_2, meta_m5_2 = executar_m5_2_composicao_cidades(
            df_saldo_elegivel_composicao_m5_1=df_saldo_elegivel_composicao_m5_1,
            df_perfis_elegiveis_por_cidade_m5_1=df_perfis_elegiveis_por_cidade_m5_1,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        motivo_pulo = "sem_saldo_elegivel_m5_2" if not m5_2_tem_saldo else "sem_perfis_elegiveis_m5_2"
        outputs_m5_2 = {
            "df_premanifestos_m5_2": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_2": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_1).columns.tolist()),
            "df_remanescente_m5_2": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_1),
            "df_tentativas_m5_2": pd.DataFrame(columns=["resultado", "motivo"]),
        }
        meta_m5_2 = {
            "resumo_m5_2": {
                "modulo": "M5.2",
                "etapa_pulada": True,
                "motivo_etapa_pulada": motivo_pulo,
                "linhas_entrada_m5_2": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "linhas_saida_m5_2": 0,
                "remanescente_preservado_m5_2": _safe_len(df_saldo_elegivel_composicao_m5_1),
            }
        }
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
            status="ok" if m5_2_tem_saldo and m5_2_tem_perfis else "ignorado",
            mensagem="M5.2 executado com sucesso" if m5_2_tem_saldo and m5_2_tem_perfis else "M5.2 pulado por ausência de entrada válida",
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
    m5_3b_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_3, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_3.empty
    m5_3b_tem_perfis = isinstance(df_perfis_elegiveis_por_subregiao_m5_3, pd.DataFrame) and not df_perfis_elegiveis_por_subregiao_m5_3.empty
    if m5_3b_tem_saldo and m5_3b_tem_perfis:
        outputs_m5_3b, meta_m5_3b = executar_m5_3_composicao_subregioes(
            df_saldo_elegivel_composicao_m5_3=df_saldo_elegivel_composicao_m5_3,
            df_perfis_elegiveis_por_subregiao_m5_3=df_perfis_elegiveis_por_subregiao_m5_3,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        motivo_pulo = "sem_saldo_elegivel_m5_3b" if not m5_3b_tem_saldo else "sem_perfis_elegiveis_m5_3b"
        outputs_m5_3b = {
            "df_premanifestos_m5_3": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_3": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_3).columns.tolist()),
            "df_tentativas_m5_3": pd.DataFrame(columns=["resultado", "motivo"]),
            "df_remanescente_m5_3": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_3),
        }
        meta_m5_3b = {"resumo_m5_3b": {"modulo": "M5.3B", "etapa_pulada": True, "motivo_etapa_pulada": motivo_pulo, "linhas_entrada_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3), "linhas_saida_m5_3": 0, "remanescente_preservado_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3)}}
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
            status="ok" if m5_3b_tem_saldo and m5_3b_tem_perfis else "ignorado",
            mensagem="M5.3B executado com sucesso" if m5_3b_tem_saldo and m5_3b_tem_perfis else "M5.3B pulado por ausência de entrada válida",
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
    m5_4b_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_4, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_4.empty
    m5_4b_tem_perfis = isinstance(df_perfis_elegiveis_por_mesorregiao_m5_4, pd.DataFrame) and not df_perfis_elegiveis_por_mesorregiao_m5_4.empty
    if m5_4b_tem_saldo and m5_4b_tem_perfis:
        outputs_m5_4b, meta_m5_4b = executar_m5_4b_composicao_mesorregioes(
            df_saldo_elegivel_composicao_m5_4=df_saldo_elegivel_composicao_m5_4,
            df_perfis_elegiveis_por_mesorregiao_m5_4=df_perfis_elegiveis_por_mesorregiao_m5_4,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        motivo_pulo = "sem_saldo_elegivel_m5_4b" if not m5_4b_tem_saldo else "sem_perfis_elegiveis_m5_4b"
        outputs_m5_4b = {
            "df_premanifestos_m5_4": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_4": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4).columns.tolist()),
            "df_tentativas_m5_4": pd.DataFrame(columns=["resultado", "motivo"]),
            "df_remanescente_m5_4": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4),
        }
        meta_m5_4b = {"resumo_m5_4b": {"modulo": "M5.4B", "etapa_pulada": True, "motivo_etapa_pulada": motivo_pulo, "linhas_entrada_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4), "linhas_saida_m5_4": 0, "remanescente_preservado_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4)}}
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
            status="ok" if m5_4b_tem_saldo and m5_4b_tem_perfis else "ignorado",
            mensagem="M5.4B executado com sucesso" if m5_4b_tem_saldo and m5_4b_tem_perfis else "M5.4B pulado por ausência de entrada válida",
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
    manifesto_schema_valido = _tem_schema_minimo(df_manifestos_base_m6, COLS_MANIFESTOS_OBRIGATORIAS)
    manifesto_tem_id = isinstance(df_manifestos_base_m6, pd.DataFrame) and ("manifesto_id" in df_manifestos_base_m6.columns)
    manifestos_base_validos_m6_2 = (
        isinstance(df_manifestos_base_m6, pd.DataFrame)
        and manifesto_schema_valido
        and manifesto_tem_id
        and (not df_manifestos_base_m6.empty)
    )

    if manifestos_base_validos_m6_2:
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
    else:
        resultado_m6_2 = {
            "outputs_m6_2": {
                "df_manifestos_m6_2": (
                    df_manifestos_base_m6.copy()
                    if isinstance(df_manifestos_base_m6, pd.DataFrame)
                    else pd.DataFrame(columns=COLS_MANIFESTOS_OBRIGATORIAS)
                ),
                "df_itens_manifestos_m6_2": (
                    df_itens_manifestos_base_m6.copy()
                    if isinstance(df_itens_manifestos_base_m6, pd.DataFrame)
                    else pd.DataFrame()
                ),
                "df_remanescente_m6_2": df_remanescente_m5_4.copy() if isinstance(df_remanescente_m5_4, pd.DataFrame) else pd.DataFrame(),
                "df_remanescente_m5_original_m6_2": df_remanescente_m5_4.copy() if isinstance(df_remanescente_m5_4, pd.DataFrame) else pd.DataFrame(),
                "df_tentativas_m6_2": pd.DataFrame(),
                "df_movimentos_aceitos_m6_2": pd.DataFrame(),
            },
            "resumo_m6_2": {
                "modulo": "M6.2",
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "ocupacao_alvo_perc": 85.0,
                "etapa_pulada": True,
                "motivo_etapa_pulada": "sem_manifestos_base_m6_2",
                "manifestos_base_total_m6_1": _safe_len(df_manifestos_base_m6),
                "itens_manifestos_base_total_m6_1": _safe_len(df_itens_manifestos_base_m6),
                "remanescente_m5_original_total": _safe_len(df_remanescente_m5_4),
                "movimentos_aceitos_m6_2": 0,
                "tentativas_total_m6_2": 0,
                "linhas_entrada_m6_2": _safe_len(df_manifestos_base_m6),
                "linhas_saida_m6_2": 0,
                "remanescente_preservado_m6_2": _safe_len(df_remanescente_m5_4),
                "caminhos_pipeline": contexto.caminhos_pipeline or {},
            },
        }
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
            status="ok" if manifestos_base_validos_m6_2 else "ignorado",
            mensagem=(
                "M6.2 executado com sucesso"
                if manifestos_base_validos_m6_2
                else "M6.2 ignorado: não há manifestos base válidos para complemento de ocupação"
            ),
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
    if _safe_len(df_manifestos_m6_2) > 0 and _safe_len(df_itens_manifestos_m6_2) > 0:
        outputs_m7, meta_m7 = executar_m7_sequenciamento_entregas(
            df_manifestos_m6_2=df_manifestos_m6_2,
            df_itens_manifestos_m6_2=df_itens_manifestos_m6_2,
            df_geo_tratado=df_geo_tratado,
            df_geo_raw=contexto.df_geo_raw,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
        status_log_m7 = "ok"
        mensagem_log_m7 = "M7 executado com sucesso"
    else:
        outputs_m7 = {
            "df_manifestos_m7": df_manifestos_m6_2.copy() if isinstance(df_manifestos_m6_2, pd.DataFrame) else pd.DataFrame(columns=["manifesto_id"]),
            "df_itens_manifestos_sequenciados_m7": df_itens_manifestos_m6_2.copy() if isinstance(df_itens_manifestos_m6_2, pd.DataFrame) else pd.DataFrame(columns=["manifesto_id", "id_linha_pipeline"]),
            "df_manifestos_sequenciamento_resumo_m7": pd.DataFrame(columns=["manifesto_id", "status_sequenciamento_m7"]),
            "df_tentativas_sequenciamento_m7": pd.DataFrame(columns=["manifesto_id", "resultado", "motivo"]),
            "df_diagnostico_recuperacao_coordenadas_m7": pd.DataFrame(columns=["indicador", "valor"]),
        }
        meta_m7 = {
            "resumo_m7": {
                "modulo": "M7",
                "etapa_pulada": True,
                "motivo_etapa_pulada": "sem_manifestos_para_sequenciamento_m7",
                "linhas_entrada_m7": _safe_len(df_itens_manifestos_m6_2),
                "linhas_saida_m7": 0,
            },
            "auditoria_m7": {},
        }
        status_log_m7 = "ignorado"
        mensagem_log_m7 = "M7 pulado por ausência de manifestos/itens para sequenciamento"
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
            status=status_log_m7,
            mensagem=mensagem_log_m7,
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
    saldo_final_roteirizacao = _serializar_dataframe_para_records(df_remanescente_m6_2, limit=None)
    nao_roteirizados_m6_2 = saldo_final_roteirizacao
    nao_roteirizaveis_m3 = []

    manifestos_fechados = []
    manifestos_compostos = []
    for manifesto in manifestos_m7:
        origem_tipo = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
        origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
        if "manifesto_fechado" in origem_tipo or origem_modulo == "M4":
            manifestos_fechados.append(manifesto)
        else:
            manifestos_compostos.append(manifesto)

    tempo_serializacao = _duracao_ms(t0)
    metricas_tempo["serializacao_resposta_ms"] = tempo_serializacao

    tempo_total = _duracao_ms(inicio_total)
    metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
    print("[RESPONSE] total manifestos_m7 serializados:", len(manifestos_m7))
    print("[RESPONSE] total itens_manifestos_sequenciados_m7 serializados:", len(itens_manifestos_sequenciados_m7))
    print("[RESPONSE] total manifestos_sequenciamento_resumo_m7 serializados:", len(manifestos_sequenciamento_resumo_m7))
    print("[RESPONSE] total remanescentes saldo_final_roteirizacao:", len(saldo_final_roteirizacao))

    resposta: Dict[str, Any] = {
        "status": "ok",
        "mensagem": "Motor executou com sucesso até o M7 sequenciamento de entregas.",
        "pipeline_real_ate": "M7",
        "modo_resposta": "auditoria_m7_sequenciamento_entregas",
        "resposta_truncada": False,
        "teste_id_auditoria": teste_id_auditoria,
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
        "manifestos_fechados": manifestos_fechados,
        "manifestos_compostos": manifestos_compostos,
        "nao_roteirizados": nao_roteirizados_m6_2,
        "total_carteira": _safe_len(contexto.df_carteira_raw),
        "total_roteirizado": _safe_len(df_itens_manifestos_sequenciados_m7),
        "total_nao_roteirizado": _safe_len(df_remanescente_m6_2),
        "itens_manifestos_sequenciados_m7": itens_manifestos_sequenciados_m7,
        "manifestos_sequenciamento_resumo_m7": manifestos_sequenciamento_resumo_m7,
        "tentativas_sequenciamento_m7": tentativas_sequenciamento_m7,
        "diagnostico_recuperacao_coordenadas_m7": diagnostico_recuperacao_coordenadas_m7,
        "remanescentes": {
            "nao_roteirizaveis_m3": nao_roteirizaveis_m3,
            "saldo_final_roteirizacao": saldo_final_roteirizacao,
        },
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
            "remanescentes_nao_roteirizaveis_m3_total": 0,
            "remanescentes_nao_roteirizaveis_m3_retornado": len(nao_roteirizaveis_m3),
            "remanescentes_saldo_final_roteirizacao_total": _safe_len(df_remanescente_m6_2),
            "remanescentes_saldo_final_roteirizacao_retornado": len(saldo_final_roteirizacao),
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


def executar_pipeline(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    try:
        return _executar_pipeline_core(payload)
    except Exception as exc:
        erro_tecnico = str(exc)
        logs = [
            _log(
                modulo="pipeline_service",
                status="erro",
                mensagem="Falha durante execução do pipeline",
                extra={"erro_tecnico": erro_tecnico},
            )
        ]
        return {
            "status": "erro",
            "mensagem": "Falha ao executar pipeline de roteirização.",
            "pipeline_real_ate": "ERRO",
            "modo_resposta": "auditoria_m7_sequenciamento_entregas",
            "resposta_truncada": False,
            "teste_id_auditoria": None,
            "resumo_execucao": {
                "rodada_id": getattr(payload, "rodada_id", None),
                "upload_id": getattr(payload, "upload_id", None),
                "usuario_id": getattr(payload, "usuario_id", None),
                "filial_id": getattr(payload, "filial_id", None),
                "tipo_roteirizacao": getattr(payload, "tipo_roteirizacao", None),
                "data_base_roteirizacao": getattr(payload, "data_base_roteirizacao", None),
                "tempos_ms": {},
            },
            "resumo_negocio": {},
            "contexto_rodada": {
                "filial": getattr(payload, "filial", None),
                "parametros_rodada": {},
            },
            "manifestos_m7": [],
            "itens_manifestos_sequenciados_m7": [],
            "manifestos_sequenciamento_resumo_m7": [],
            "tentativas_sequenciamento_m7": [],
            "diagnostico_recuperacao_coordenadas_m7": [],
            "remanescentes": {
                "nao_roteirizaveis_m3": [],
                "saldo_final_roteirizacao": [],
            },
            "auditoria_serializacao": {
                "manifestos_m7_total": 0,
                "manifestos_m7_retornado": 0,
                "itens_manifestos_sequenciados_m7_total": 0,
                "itens_manifestos_sequenciados_m7_retornado": 0,
                "manifestos_sequenciamento_resumo_m7_total": 0,
                "manifestos_sequenciamento_resumo_m7_retornado": 0,
                "tentativas_sequenciamento_m7_total": 0,
                "tentativas_sequenciamento_m7_retornado": 0,
                "diagnostico_recuperacao_coordenadas_m7_total": 0,
                "diagnostico_recuperacao_coordenadas_m7_retornado": 0,
                "remanescentes_nao_roteirizaveis_m3_total": 0,
                "remanescentes_nao_roteirizaveis_m3_retornado": 0,
                "remanescentes_saldo_final_roteirizacao_total": 0,
                "remanescentes_saldo_final_roteirizacao_retornado": 0,
            },
            "auditoria_m7": {},
            "erro_tecnico": erro_tecnico,
            "logs": logs,
        }
