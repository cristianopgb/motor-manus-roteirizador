from __future__ import annotations

from datetime import datetime, time
from typing import Any, Iterable, Optional

from app.schemas import RoteirizacaoRequest


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _is_valid_number(value: Any) -> bool:
    try:
        num = float(value)
        return num == num  # evita NaN
    except Exception:
        return False


def _validar_data_iso(value: str) -> None:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(
            "Campo 'data_base_roteirizacao' inválido. "
            "Use formato ISO datetime, por exemplo: 2026-04-05T12:30:00.000Z"
        ) from exc


def _normalizar_texto(value: Any) -> Optional[str]:
    if value is None:
        return None
    txt = str(value).strip()
    return txt if txt != "" else None


def _normalizar_bool_flex(value: Any) -> Optional[bool]:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    txt = str(value).strip().lower()
    if txt == "":
        return None

    positivos = {"1", "true", "sim", "s", "yes", "y"}
    negativos = {"0", "false", "nao", "não", "n", "no"}

    if txt in positivos:
        return True
    if txt in negativos:
        return False

    return None


def _parse_hora_flex(value: Any) -> Optional[time]:
    if value is None:
        return None

    if isinstance(value, time):
        return value

    txt = str(value).strip()
    if txt == "":
        return None

    formatos = ("%H:%M", "%H:%M:%S")
    for fmt in formatos:
        try:
            return datetime.strptime(txt, fmt).time()
        except Exception:
            continue

    return None


def _validar_lista_nao_vazia(nome: str, valores: Iterable[Any]) -> None:
    if valores is None or len(list(valores)) == 0:
        raise ValueError(f"A lista de {nome} enviada ao motor está vazia")


def _validar_bloco_filial(payload: RoteirizacaoRequest) -> None:
    if payload.filial is None:
        raise ValueError("Bloco obrigatório ausente: filial")

    if _is_blank(payload.filial.id):
        raise ValueError("Bloco filial inválido: id ausente")

    if _is_blank(payload.filial.nome):
        raise ValueError("Bloco filial inválido: nome ausente")

    if _is_blank(payload.filial.cidade):
        raise ValueError("Bloco filial inválido: cidade ausente")

    if _is_blank(payload.filial.uf):
        raise ValueError("Bloco filial inválido: uf ausente")

    if not _is_valid_number(payload.filial.latitude):
        raise ValueError("Bloco filial inválido: latitude ausente ou inválida")

    if not _is_valid_number(payload.filial.longitude):
        raise ValueError("Bloco filial inválido: longitude ausente ou inválida")

    lat = float(payload.filial.latitude)
    lon = float(payload.filial.longitude)

    if not (-35 <= lat <= 5):
        raise ValueError("Bloco filial inválido: latitude fora de faixa plausível")

    if not (-80 <= lon <= -30):
        raise ValueError("Bloco filial inválido: longitude fora de faixa plausível")


def _validar_configuracao_frota(payload: RoteirizacaoRequest) -> None:
    if payload.tipo_roteirizacao != "frota":
        return

    if len(payload.configuracao_frota) == 0:
        raise ValueError(
            "tipo_roteirizacao='frota' exige configuracao_frota com pelo menos um perfil"
        )

    for idx, item in enumerate(payload.configuracao_frota, start=1):
        if _is_blank(item.perfil):
            raise ValueError(
                f"configuracao_frota[{idx}] inválida: campo 'perfil' ausente"
            )

        try:
            qtd = int(item.quantidade)
        except Exception as exc:
            raise ValueError(
                f"configuracao_frota[{idx}] inválida: campo 'quantidade' deve ser inteiro"
            ) from exc

        if qtd <= 0:
            raise ValueError(
                f"configuracao_frota[{idx}] inválida: 'quantidade' deve ser maior que zero"
            )


def _validar_campos_minimos_carteira(payload: RoteirizacaoRequest) -> None:
    """
    Validação leve e estrutural da carteira.
    Não endurece regras de negócio que pertencem ao M1/M3/M4.
    """
    if len(payload.carteira) == 0:
        raise ValueError("A carteira enviada ao motor está vazia")

    itens_validos_minimos = 0

    for idx, item in enumerate(payload.carteira, start=1):
        # documento / rastreabilidade mínima
        nro_doc = getattr(item, "Nro_Doc", None)
        peso = getattr(item, "Peso", None)
        lat = getattr(item, "Latitude", None)
        lon = getattr(item, "Longitude", None)

        # aceita linha mesmo sem documento, desde que tenha ao menos peso ou localização útil.
        # isso evita rejeitar cedo demais registros que o M1 ainda pode tratar.
        possui_conteudo_minimo = (
            not _is_blank(nro_doc)
            or _is_valid_number(peso)
            or _is_valid_number(lat)
            or _is_valid_number(lon)
        )

        if possui_conteudo_minimo:
            itens_validos_minimos += 1

        # Peso Calculo - se vier, precisa ser numérico
        peso_calculo = getattr(item, "Peso_Calculo", None)
        if not _is_blank(peso_calculo) and not _is_valid_number(peso_calculo):
            raise ValueError(
                f"carteira[{idx}] inválido: 'Peso Calculo' deve ser numérico quando informado"
            )

        # Peso Cub. - se vier, precisa ser numérico
        peso_cub = getattr(item, "Peso_Cub", None)
        if not _is_blank(peso_cub) and not _is_valid_number(peso_cub):
            raise ValueError(
                f"carteira[{idx}] inválido: 'Peso Cub.' deve ser numérico quando informado"
            )

        # Latitude / Longitude - se vierem, precisam ser numéricas
        if not _is_blank(lat) and not _is_valid_number(lat):
            raise ValueError(
                f"carteira[{idx}] inválido: 'Latitude' deve ser numérica quando informada"
            )

        if not _is_blank(lon) and not _is_valid_number(lon):
            raise ValueError(
                f"carteira[{idx}] inválido: 'Longitude' deve ser numérica quando informada"
            )

        # Carro Dedicado - validação leve
        carro_dedicado = getattr(item, "Carro_Dedicado", None)
        if not _is_blank(carro_dedicado):
            bool_norm = _normalizar_bool_flex(carro_dedicado)
            if bool_norm is None:
                raise ValueError(
                    f"carteira[{idx}] inválido: 'Carro Dedicado' fora do padrão esperado"
                )

        # Prioridade - enum, mas aqui validamos de forma leve
        prioridade = getattr(item, "Prioridade", None)
        if not _is_blank(prioridade):
            prioridade_txt = _normalizar_texto(prioridade)
            if prioridade_txt is None:
                raise ValueError(
                    f"carteira[{idx}] inválido: 'Prioridade' vazia quando informada"
                )

        # Restrição Veículo - enum, mas aqui validamos só presença sem texto vazio
        restricao_veiculo = getattr(item, "Restricao_Veiculo", None)
        if not _is_blank(restricao_veiculo):
            restricao_txt = _normalizar_texto(restricao_veiculo)
            if restricao_txt is None:
                raise ValueError(
                    f"carteira[{idx}] inválido: 'Restrição Veículo' vazia quando informada"
                )

        # Janela de entrega - validação leve de formato
        inicio_ent = getattr(item, "Inicio_Ent", None)
        fim_en = getattr(item, "Fim_En", None)

        hora_inicio = _parse_hora_flex(inicio_ent)
        hora_fim = _parse_hora_flex(fim_en)

        if not _is_blank(inicio_ent) and hora_inicio is None:
            raise ValueError(
                f"carteira[{idx}] inválido: 'Inicio Ent.' fora do formato esperado (HH:MM ou HH:MM:SS)"
            )

        if not _is_blank(fim_en) and hora_fim is None:
            raise ValueError(
                f"carteira[{idx}] inválido: 'Fim En' fora do formato esperado (HH:MM ou HH:MM:SS)"
            )

        if hora_inicio is not None and hora_fim is not None and hora_inicio >= hora_fim:
            raise ValueError(
                f"carteira[{idx}] inválido: 'Inicio Ent.' deve ser menor que 'Fim En'"
            )

    if itens_validos_minimos == 0:
        raise ValueError(
            "A carteira enviada ao motor não possui itens com conteúdo mínimo utilizável"
        )


def validar_payload(payload: RoteirizacaoRequest) -> None:
    # ============================================================
    # CABEÇALHO DA RODADA
    # ============================================================
    if _is_blank(payload.rodada_id):
        raise ValueError("Campo obrigatório ausente: rodada_id")

    if _is_blank(payload.upload_id):
        raise ValueError("Campo obrigatório ausente: upload_id")

    if _is_blank(payload.usuario_id):
        raise ValueError("Campo obrigatório ausente: usuario_id")

    if _is_blank(payload.filial_id):
        raise ValueError("Campo obrigatório ausente: filial_id")

    if _is_blank(payload.data_base_roteirizacao):
        raise ValueError("Campo obrigatório ausente: data_base_roteirizacao")

    _validar_data_iso(payload.data_base_roteirizacao)

    if payload.tipo_roteirizacao not in {"carteira", "frota"}:
        raise ValueError("tipo_roteirizacao deve ser 'carteira' ou 'frota'")

    # ============================================================
    # BLOCO FILIAL
    # ============================================================
    _validar_bloco_filial(payload)

    # ============================================================
    # BLOCOS OBRIGATÓRIOS
    # ============================================================
    if len(payload.veiculos) == 0:
        raise ValueError("A lista de veículos enviada ao motor está vazia")

    if len(payload.regionalidades) == 0:
        raise ValueError("A lista de regionalidades enviada ao motor está vazia")

    _validar_configuracao_frota(payload)

    # ============================================================
    # CARTEIRA
    # ============================================================
    _validar_campos_minimos_carteira(payload)
