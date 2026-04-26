from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List

import httpx
import numpy as np
import pandas as pd

from app.utils.json_safe import sanitizar_json_safe

SUPABASE_TIMEOUT_SECONDS = 20.0
SUPABASE_INSERT_CHUNK_SIZE = 500

# Colunas reais levantadas dos dataframes oficiais:
# - payload_service (contexto.df_carteira_raw)
# - m0_adapter (resultado_m0["df_carteira_raw"])
# - m1_padronizacao (df_carteira_tratada)
# - m2_enriquecimento (df_carteira_enriquecida)
# - m3_triagem (df_carteira_triagem)
# - m3_1_validacao_fronteira (df_input_oficial_bloco_4)
COLUNAS_AUDITORIA_FLAT: List[str] = [
    "Filial_R", "Romane", "Filial_D", "Serie", "Nro_Doc", "Data_Des", "Data_NF", "DLE", "Agendam", "Palet",
    "Conf", "Peso", "Vlr_Merc", "Qtd", "Peso_Cub", "Peso_Calculo", "Classif", "Tomad", "Destin", "Bairro",
    "Cidad", "UF", "NF_Serie", "Tipo_Ca", "Tipo_Carga", "Qtd_NF", "Regiao", "Mesoregiao", "Sub_Regiao",
    "Ocorrencias_NF", "Remetente", "Observacao", "Ref_Cliente", "Cidade_Dest", "Agenda", "Ultima_Ocorrencia",
    "Status_R", "Latitude", "Longitude", "Prioridade", "Restricao_Veiculo", "Carro_Dedicado", "Inicio_Ent",
    "Fim_En", "filial_roteirizacao", "romaneio", "filial_origem", "serie", "nro_documento", "data_descarga",
    "data_nf", "data_leadtime", "data_agenda", "qtd_pallet", "conferencia", "peso_kg", "vlr_merc", "qtd_volumes",
    "vol_m3", "peso_calculado", "classifi", "tomador", "destinatario", "bairro", "cidade", "uf", "nf_serie",
    "tipo_ca", "tipo_carga", "qtd_nf", "regiao", "mesorregiao", "sub_regiao", "ocorrencias_nfs", "remetente",
    "observacao_r", "ref_cliente", "cidade_dest", "agenda", "ultima", "status", "latitude_destinatario",
    "longitude_destinatario", "prioridade_embarque", "restricao_veiculo", "veiculo_exclusivo", "inicio_entrega",
    "fim_entrega", "prioridade_embarque_num", "agendada", "veiculo_exclusivo_flag", "origem_cidade", "origem_uf",
    "latitude_filial", "longitude_filial", "data_base_roteirizacao", "cidade_chave", "uf_chave",
    "ranking_preliminar", "score_prioridade_preliminar", "subregiao", "data_limite_considerada",
    "tipo_data_limite", "dias_ate_data_alvo", "horas_viagem_estimadas", "transit_time_dias", "folga_dias",
    "status_folga", "distancia_km", "distancia_rodoviaria_est_km", "faixa_km_cd", "quadrante",
    "angulo_origem_destino_graus", "eixo_8_setores", "corredor_30g", "corredor_30g_idx",
    "perfil_veiculo_referencia", "status_geo", "origem_latitude", "origem_longitude", "status_triagem",
    "motivo_triagem", "grupo_saida", "prioridade_label", "ranking_prioridade_operacional", "flag_roteirizavel",
    "flag_agendamento_futuro", "flag_agenda_vencida", "id_linha_pipeline",
    "manifesto_id", "tipo_manifesto", "veiculo_tipo", "veiculo_perfil", "qtd_eixos", "qtd_itens", "qtd_ctes",
    "qtd_paradas", "qtd_cidades", "base_carga_oficial", "peso_total_kg", "vol_total_m3", "km_referencia",
    "ocupacao_oficial_perc", "capacidade_peso_kg_veiculo", "capacidade_vol_m3_veiculo", "max_entregas_veiculo",
    "max_km_distancia_veiculo", "ocupacao_minima_perc_veiculo", "ocupacao_maxima_perc_veiculo",
    "ignorar_ocupacao_minima", "origem_modulo",
    "origem_etapa", "motivo_nao_roteirizavel", "motivo_final_remanescente_m4",
    "peso_total_cidade", "km_referencia_cidade", "qtd_clientes_cidade", "qtd_linhas_cidade",
    "status_perfil_cidade", "motivo_status_perfil_cidade", "regra_aplicada", "cidade_elegivel_m5_1",
    "motivo_status_cidade_m5_1", "ordem_cidade_m5_1",
    "peso_total_subregiao", "qtd_linhas_subregiao", "qtd_clientes_subregiao", "km_referencia_subregiao",
    "qtd_cidades_subregiao", "perfil", "tipo", "capacidade_peso_kg", "capacidade_vol_m3", "max_entregas",
    "max_km_distancia", "ocupacao_minima_perc", "ocupacao_maxima_perc", "ocupacao_calculada_perc",
    "status_perfil_subregiao", "motivo_status_perfil_subregiao", "subregiao_elegivel_m5_3",
    "motivo_status_subregiao_m5_3", "ordem_subregiao_m5_3", "qtd_perfis_elegiveis", "qtd_perfis_descartados",
    "peso_total_mesorregiao", "km_referencia_mesorregiao", "qtd_clientes_mesorregiao",
    "qtd_subregioes_mesorregiao", "qtd_cidades_mesorregiao", "qtd_linhas_mesorregiao",
    "status_perfil_mesorregiao", "motivo_status_perfil_mesorregiao", "mesorregiao_elegivel_m5_4",
    "motivo_status_mesorregiao_m5_4", "ordem_mesorregiao_m5_4",
    "tentativa_idx", "blocos_considerados", "veiculo_tipo_tentado", "veiculo_perfil_tentado", "resultado",
    "motivo", "qtd_itens_candidato", "qtd_paradas_candidato", "peso_total_candidato", "peso_kg_total_candidato",
    "volume_total_candidato", "km_referencia_candidato", "ocupacao_perc_candidato",
    "origem_manifesto_modulo", "origem_manifesto_tipo", "texto_exclusivo_detectado_m6", "peso_base_antes_m6",
    "km_base_antes_m6", "ocupacao_base_antes_m6", "qtd_itens_base_antes_m6", "qtd_ctes_base_antes_m6",
    "qtd_paradas_base_antes_m6", "manifesto_a", "manifesto_b", "mesmo_perfil_flag", "mesma_mesorregiao_flag",
    "score_ocupacao", "score_km", "score_prioridade_m6", "elegivel_otimizacao_m6", "motivo_elegibilidade_m6",
    "perfil_final_m6_2", "ocupacao_entrada_perc", "ocupacao_final_m6_2", "peso_final_m6_2", "km_final_m6_2",
    "qtd_itens_final_m6_2", "qtd_paradas_final_m6_2", "flag_otimizado_m6_2", "espaco_disponivel_peso_kg_m6_2",
    "paradas_disponiveis_m6_2", "mesorregiao_manifesto_m6", "origem_item_m6_2", "tipo_tentativa",
    "nivel_hierarquia", "aceito", "comparativo_ocupacao_antes", "comparativo_ocupacao_depois",
    "comparativo_peso_antes", "comparativo_peso_depois",
    "ordem_entrega_doc_m7", "ordem_carregamento_doc_m7", "ordem_parada_m7",
    "sequencia_entrega", "sequencia_carregamento", "parada", "parada_idx", "cidade_parada",
    "distancia_parcial_km", "distancia_acumulada_km", "km_entre_paradas",
    "metodo_predominante_m7", "metodo_sequenciamento", "status_sequenciamento_m7",
    "criterio_doc", "criterio_parada", "bucket_prioridade_doc_m7", "folga_prioridade_doc_m7",
    "peso_prioridade_doc_m7", "coord_dest_origem_m7", "status_coord_filial_m7", "status_coord_dest_m7",
    "fonte_filial_m7",
    "cidade_filial_m7_1", "possui_cidade_filial_no_manifesto_m7_1", "cidade_filial_foi_primeiro_bloco_m7_1",
    "flag_reordenado_origem_m7_1", "km_total_original_m7", "km_total_reavaliado_m7_1", "diferenca_km_m7_1",
    "status_km_m7_1", "ocupacao_original_m7_1", "peso_total_original_m7_1", "uf_filial_m7_1",
    "fonte_filial_m7_1", "km_total_cidades_reavaliado_m7_1",
    "ordem_entrega_doc_m7_1", "ordem_parada_m7_1", "flag_cidade_filial_m7_1",
    "status_final_m7_2", "km_total_final_m7_2", "ocupacao_original_m7_2", "ocupacao_final_m7_2",
    "cidade_removida_m7_2", "quantidade_cidades_removidas_m7_2", "manifesto_desfeito_m7_2",
    "motivo_ajuste_m7_2", "ordem_entrega_doc_m7_2", "ordem_parada_m7_2", "status_item_m7_2",
    "motivo_retirada_m7_2", "mensagem_erro_m7_2", "cidades_testadas", "km_resultante", "ocupacao_resultante",
]


def _normalizar_dataframe_para_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []

    df2 = df.copy()
    for col in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[col]):
            df2[col] = df2[col].astype(str)
    df2 = df2.where(pd.notnull(df2), None)

    records = df2.to_dict(orient="records")
    records_sanitizados, _ = sanitizar_json_safe(records)
    return records_sanitizados


def _pick_primeiro_valor(row: Dict[str, Any], nomes: List[str]) -> Any:
    for nome in nomes:
        if nome in row and row.get(nome) is not None:
            return row.get(nome)
    return None


def _valor_normalizado_chave(valor: Any) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    if texto.lower() in {"none", "nan", "nat"}:
        return ""
    return texto


def _gerar_chave_linha_dataset(row: Dict[str, Any]) -> str:
    id_linha = _pick_primeiro_valor(row, ["id_linha_pipeline"])
    if id_linha is not None and _valor_normalizado_chave(id_linha):
        return _valor_normalizado_chave(id_linha)

    campos_fallback = {
        "nro_documento": _valor_normalizado_chave(_pick_primeiro_valor(row, ["nro_documento"])),
        "destinatario": _valor_normalizado_chave(_pick_primeiro_valor(row, ["destinatario"])),
        "cidade": _valor_normalizado_chave(_pick_primeiro_valor(row, ["cidade"])),
        "uf": _valor_normalizado_chave(_pick_primeiro_valor(row, ["uf"])),
        "data_nf": _valor_normalizado_chave(_pick_primeiro_valor(row, ["data_nf"])),
        "romaneio": _valor_normalizado_chave(_pick_primeiro_valor(row, ["romaneio"])),
        "serie": _valor_normalizado_chave(_pick_primeiro_valor(row, ["serie"])),
    }
    assinatura = json.dumps(campos_fallback, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(assinatura.encode("utf-8")).hexdigest()


def _normalizar_valor_flat(valor: Any) -> Any:
    if valor is None:
        return None
    if isinstance(valor, (datetime, pd.Timestamp)):
        return pd.to_datetime(valor, errors="coerce").isoformat() if pd.notna(valor) else None
    if isinstance(valor, (dict, list)):
        return json.dumps(valor, ensure_ascii=False)
    return valor


def _normalizar_valor_json(valor: Any) -> Any:
    if valor is None or valor is pd.NA:
        return None
    if isinstance(valor, float) and pd.isna(valor):
        return None
    if isinstance(valor, (np.generic,)):
        return valor.item()
    if isinstance(valor, (pd.Timestamp, datetime)):
        return pd.to_datetime(valor, errors="coerce").isoformat() if pd.notna(valor) else None
    if isinstance(valor, (list, dict)):
        return json.dumps(valor, ensure_ascii=False)
    if hasattr(valor, "to_pydatetime"):
        try:
            return valor.to_pydatetime().isoformat()
        except Exception:
            return str(valor)
    if isinstance(valor, (pd.Series, pd.DataFrame)):
        return json.dumps(sanitizar_json_safe(valor.to_dict())[0], ensure_ascii=False)
    return valor


def _normalizar_row_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    row_normalizada: Dict[str, Any] = {}
    for col, val in row.items():
        row_normalizada[col] = _normalizar_valor_json(val)
    row_sanitizada, _ = sanitizar_json_safe(row_normalizada)
    return row_sanitizada


def _normalizar_nome_coluna_auditoria(nome: str) -> str:
    if nome is None:
        return ""
    texto = str(nome).strip()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(char for char in texto if not unicodedata.combining(char))
    texto = texto.replace(" ", "_")
    texto = re.sub(r"[^a-zA-Z0-9_]", "_", texto)
    texto = re.sub(r"_+", "_", texto)
    texto = texto.strip("_")
    aliases = {
        "qtd_perfis_elegiaveis": "qtd_perfis_elegiveis",
    }
    return aliases.get(texto, texto)


def _normalizar_chaves_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return {}

    row_normalizada: Dict[str, Any] = {}
    colisoes: Dict[str, Dict[str, Any]] = {}
    for chave_original, valor in row.items():
        chave_normalizada = _normalizar_nome_coluna_auditoria(chave_original)
        if not chave_normalizada:
            continue

        if chave_normalizada not in row_normalizada:
            row_normalizada[chave_normalizada] = valor
            colisoes[chave_normalizada] = {
                "chave_original": chave_original,
                "ja_normalizada": chave_original == chave_normalizada,
            }
            continue

        atual = colisoes.get(chave_normalizada, {})
        atual_ja_normalizada = bool(atual.get("ja_normalizada"))
        chave_original_ja_normalizada = chave_original == chave_normalizada
        if not atual_ja_normalizada and chave_original_ja_normalizada:
            row_normalizada[chave_normalizada] = valor
            colisoes[chave_normalizada] = {
                "chave_original": chave_original,
                "ja_normalizada": True,
            }
    return row_normalizada


def _config_supabase() -> tuple[str | None, str | None]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    return url, key


def persistir_snapshot_modulo_auditoria(
    *,
    teste_id: str,
    rodada_id: str | None,
    upload_id: str | None,
    modulo: str,
    ordem_modulo: int,
    df_etapa: pd.DataFrame | None,
    snapshot_nome: str | None = None,
    contexto: Dict[str, Any] | None = None,
    rastreamento: Dict[str, Any] | None = None,
) -> int:
    contexto = contexto or {}
    rastreamento = rastreamento or {}
    rows = _normalizar_dataframe_para_records(df_etapa if isinstance(df_etapa, pd.DataFrame) else pd.DataFrame())
    total_linhas = len(rows)

    if total_linhas == 0:
        print(f"[AUDITORIA FLAT] snapshot={snapshot_nome or modulo} linhas=0")
        return 0

    payload_insert: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        row_sanitizada = _normalizar_row_snapshot(row)
        row_sanitizada = _normalizar_chaves_snapshot(row_sanitizada)
        id_linha_pipeline = _pick_primeiro_valor(row_sanitizada, ["id_linha_pipeline", "id_linha", "linha_id", "pedido_id"])
        chave_linha_dataset = _gerar_chave_linha_dataset(row_sanitizada)
        if idx == 0:
            print(f"[AUDITORIA FLAT] exemplo_chave_linha_dataset={chave_linha_dataset}")

        registro: Dict[str, Any] = {
            "teste_id": teste_id,
            "rodada_id": rodada_id,
            "upload_id": upload_id,
            "modulo": modulo,
            "ordem_modulo": ordem_modulo,
            "snapshot_nome": snapshot_nome or modulo,
            "chave_linha_dataset": chave_linha_dataset,
            "id_linha_pipeline": str(id_linha_pipeline) if id_linha_pipeline is not None else None,
            "manifesto_id": _normalizar_valor_flat(row_sanitizada.get("manifesto_id")),
            "payload_linha_json": row_sanitizada,
            "campos_auditoria_json": {
                "modulo": modulo,
                "ordem_modulo": ordem_modulo,
                "snapshot_nome": str(snapshot_nome or modulo),
                "snapshot_em": datetime.utcnow().isoformat() + "Z",
            },
        }
        payload_insert.append(registro)

    supabase_url, supabase_key = _config_supabase()
    if not supabase_url or not supabase_key:
        print(f"[AUDITORIA] supabase não configurado. modulo={modulo} linhas={total_linhas}")
        print(f"[AUDITORIA FLAT] snapshot={snapshot_nome or modulo} linhas=0")
        return 0

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/auditoria_pipeline_modular"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    try:
        with httpx.Client(timeout=SUPABASE_TIMEOUT_SECONDS) as client:
            for i in range(0, len(payload_insert), SUPABASE_INSERT_CHUNK_SIZE):
                chunk = payload_insert[i : i + SUPABASE_INSERT_CHUNK_SIZE]
                response = client.post(endpoint, headers=headers, json=chunk)
                response.raise_for_status()
    except Exception as exc:
        print(f"[AUDITORIA] falha ao persistir modulo={modulo}: {exc}")
        print("[AUDITORIA ERRO DETALHADO]")
        print("snapshot:", snapshot_nome or modulo)
        if payload_insert:
            print("[AUDITORIA ERRO CHAVES]", sorted(list(payload_insert[0].get("payload_linha_json", {}).keys()))[:200])
            try:
                print("exemplo payload:", json.dumps(payload_insert[0], ensure_ascii=False)[:2000])
            except Exception:
                print("exemplo payload: <erro ao serializar payload para log>")
        return 0

    print(f"[AUDITORIA FLAT] snapshot={snapshot_nome or modulo} linhas={total_linhas}")
    return total_linhas
