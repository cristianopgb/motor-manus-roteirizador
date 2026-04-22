from typing import Dict, Any, List


def _safe_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return value
    return []


def _is_manifesto_fechado(manifesto: Dict[str, Any]) -> bool:
    origem = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
    if "manifesto_fechado" in origem:
        return True

    origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
    if origem_modulo == "M4":
        return True

    return False


def _is_manifesto_composto(manifesto: Dict[str, Any]) -> bool:
    if _is_manifesto_fechado(manifesto):
        return False

    origem = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
    if origem.startswith("pre_manifesto_"):
        return True

    origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
    return origem_modulo in {"M5.2", "M5.3B", "M5.4B"}


def montar_resposta_sucesso(resultado_pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preserva contrato completo retornado pelo pipeline.
    """
    resposta = dict(resultado_pipeline or {})
    resposta.setdefault("manifestos_m7", _safe_list(resposta.get("manifestos_m7")))
    resposta.setdefault("itens_manifestos_sequenciados_m7", _safe_list(resposta.get("itens_manifestos_sequenciados_m7")))
    resposta.setdefault("manifestos_sequenciamento_resumo_m7", _safe_list(resposta.get("manifestos_sequenciamento_resumo_m7")))
    resposta.setdefault("tentativas_sequenciamento_m7", _safe_list(resposta.get("tentativas_sequenciamento_m7")))
    resposta.setdefault("diagnostico_recuperacao_coordenadas_m7", _safe_list(resposta.get("diagnostico_recuperacao_coordenadas_m7")))
    resposta.setdefault("remanescentes", {"nao_roteirizaveis_m3": [], "saldo_final_roteirizacao": []})
    resposta.setdefault("auditoria_serializacao", {})
    resposta.setdefault("auditoria_m7", {})
    resposta.setdefault("logs", [])
    return resposta


def montar_resposta_erro(mensagem: str, tipo_erro: str = "ERRO") -> Dict[str, Any]:
    """
    Monta resposta padronizada de erro.
    Não levanta exceção — retorna erro controlado.
    """

    return {
        "status": "erro",
        "mensagem": mensagem,
        "tipo_erro": tipo_erro,
        "pipeline_real_ate": "ERRO",
        "modo_resposta": "auditoria_m7_sequenciamento_entregas",
        "resposta_truncada": False,
        "resumo_execucao": {},
        "resumo_negocio": {},
        "contexto_rodada": {},
        "manifestos_m7": [],
        "itens_manifestos_sequenciados_m7": [],
        "manifestos_sequenciamento_resumo_m7": [],
        "tentativas_sequenciamento_m7": [],
        "diagnostico_recuperacao_coordenadas_m7": [],
        "remanescentes": {"nao_roteirizaveis_m3": [], "saldo_final_roteirizacao": []},
        "auditoria_serializacao": {},
        "auditoria_m7": {},
        "erro_tecnico": mensagem,
        "logs": [
            {
                "modulo": "erro",
                "status": "falha",
                "mensagem": mensagem,
                "quantidade_entrada": None,
                "quantidade_saida": None,
            }
        ],
    }
