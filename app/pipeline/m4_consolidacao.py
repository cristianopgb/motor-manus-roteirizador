"""
M4 — Consolidação de Manifestos
Responsabilidade: alocar cargas em veículos respeitando capacidade de peso,
volume e número máximo de entregas. Algoritmo: First Fit Decreasing (FFD)
por peso, com verificação de volume. Prioridade de alocação segue a ordem
de prioridade de negócio (URGENTE > ALTA > MEDIA > NORMAL > BAIXA).
"""
from __future__ import annotations
import uuid
import pandas as pd
from typing import List, Dict, Tuple, Any


def _selecionar_veiculo_compativel(
    restricao: str,
    veiculos_disponiveis: List[Dict],
    peso_carga: float,
    vol_carga: float,
) -> Dict | None:
    """
    Retorna o menor veículo compatível com a restrição e capacidade necessária.
    """
    candidatos = []
    for v in veiculos_disponiveis:
        if restricao and restricao not in ("", "QUALQUER"):
            if v["perfil"].upper() != restricao.upper():
                continue
        if v["capacidade_peso_kg"] >= peso_carga and v["capacidade_vol_m3"] >= vol_carga:
            candidatos.append(v)
    if not candidatos:
        return None
    # Menor capacidade suficiente (evita desperdício)
    return min(candidatos, key=lambda v: v["capacidade_peso_kg"])


def _criar_manifesto_vazio(veiculo: Dict, tipo: str) -> Dict:
    return {
        "manifesto_id": str(uuid.uuid4()),
        "tipo": tipo,
        "veiculo_id": veiculo["id"],
        "veiculo_perfil": veiculo["perfil"],
        "veiculo_placa": veiculo.get("placa"),
        "qtd_eixos": veiculo["qtd_eixos"],
        "cap_peso": veiculo["capacidade_peso_kg"],
        "cap_vol": veiculo["capacidade_vol_m3"],
        "max_entregas": veiculo["max_entregas"],
        "max_km": veiculo["max_km_distancia"],
        "ocup_minima_perc": veiculo["ocupacao_minima_perc"],
        "peso_alocado": 0.0,
        "vol_alocado": 0.0,
        "entregas": [],
        "km_max_entrega": 0.0,
    }


def _manifesto_aceita(manifesto: Dict, peso: float, vol: float, km: float) -> bool:
    if manifesto["peso_alocado"] + peso > manifesto["cap_peso"]:
        return False
    if manifesto["vol_alocado"] + vol > manifesto["cap_vol"]:
        return False
    if len(manifesto["entregas"]) >= manifesto["max_entregas"]:
        return False
    if km > manifesto["max_km"]:
        return False
    return True


def _alocar_dedicados(
    df_dedicados: pd.DataFrame,
    veiculos: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Cada destinatário único com carro_dedicado=True recebe um manifesto próprio.
    """
    manifestos = []
    nao_alocados = []

    if df_dedicados.empty:
        return manifestos, nao_alocados

    # Agrupa por destinatário
    grupos = df_dedicados.groupby("destin", sort=False)

    for destin, grupo in grupos:
        peso_total = grupo["peso_calculo"].sum()
        vol_total = grupo["peso_cubico"].sum()
        km_max = grupo["km_da_filial"].max()

        veiculo = _selecionar_veiculo_compativel("", veiculos, peso_total, vol_total)
        if not veiculo:
            for _, row in grupo.iterrows():
                nao_alocados.append({
                    "nro_doc": row.get("nro_doc"),
                    "romane": row.get("romane"),
                    "destin": row.get("destin"),
                    "cidade": row.get("cidade"),
                    "uf": row.get("uf"),
                    "peso": row.get("peso_calculo"),
                    "vlr_merc": row.get("vlr_merc"),
                    "motivo": f"Carro dedicado: nenhum veículo com capacidade suficiente para {peso_total:.0f} kg"
                })
            continue

        manifesto = _criar_manifesto_vazio(veiculo, "fechado")
        for _, row in grupo.iterrows():
            manifesto["entregas"].append(row.to_dict())
            manifesto["peso_alocado"] += row["peso_calculo"]
            manifesto["vol_alocado"] += row.get("peso_cubico", 0.0)
            manifesto["km_max_entrega"] = max(manifesto["km_max_entrega"], row.get("km_da_filial", 0.0))

        manifestos.append(manifesto)

    return manifestos, nao_alocados


def _alocar_ffd(
    df: pd.DataFrame,
    veiculos: List[Dict],
    tipo: str,
    restricao_col: str = "",
) -> Tuple[List[Dict], List[Dict]]:
    """
    First Fit Decreasing: ordena cargas por peso desc, aloca em manifestos abertos.
    """
    manifestos: List[Dict] = []
    nao_alocados: List[Dict] = []

    if df.empty:
        return manifestos, nao_alocados

    # Ordenar por prioridade (asc) depois por peso (desc) para priorizar urgentes e pesados
    df_sorted = df.sort_values(
        ["prioridade_ordem", "peso_calculo"],
        ascending=[True, False]
    ).reset_index(drop=True)

    for _, row in df_sorted.iterrows():
        peso = row["peso_calculo"]
        vol = row.get("peso_cubico", 0.0)
        km = row.get("km_da_filial", 0.0)
        restricao = row.get("restricao_veiculo", "") if restricao_col else ""

        alocado = False

        # Tenta encaixar em manifesto já aberto
        for manifesto in manifestos:
            # Verifica compatibilidade de perfil se há restrição
            if restricao and restricao not in ("", "QUALQUER"):
                if manifesto["veiculo_perfil"].upper() != restricao.upper():
                    continue
            if _manifesto_aceita(manifesto, peso, vol, km):
                manifesto["entregas"].append(row.to_dict())
                manifesto["peso_alocado"] += peso
                manifesto["vol_alocado"] += vol
                manifesto["km_max_entrega"] = max(manifesto["km_max_entrega"], km)
                alocado = True
                break

        if not alocado:
            # Abre novo manifesto
            veiculo = _selecionar_veiculo_compativel(restricao, veiculos, peso, vol)
            if not veiculo:
                nao_alocados.append({
                    "nro_doc": row.get("nro_doc"),
                    "romane": row.get("romane"),
                    "destin": row.get("destin"),
                    "cidade": row.get("cidade"),
                    "uf": row.get("uf"),
                    "peso": peso,
                    "vlr_merc": row.get("vlr_merc"),
                    "motivo": f"Nenhum veículo disponível compatível com restrição '{restricao}' e capacidade {peso:.0f} kg"
                })
                continue

            novo = _criar_manifesto_vazio(veiculo, tipo)
            novo["entregas"].append(row.to_dict())
            novo["peso_alocado"] += peso
            novo["vol_alocado"] += vol
            novo["km_max_entrega"] = max(novo["km_max_entrega"], km)
            manifestos.append(novo)

    # Verificar ocupação mínima: manifestos abaixo do mínimo têm cargas devolvidas
    manifestos_validos = []
    for m in manifestos:
        ocup_perc = (m["peso_alocado"] / m["cap_peso"]) * 100 if m["cap_peso"] > 0 else 0
        if ocup_perc < m["ocup_minima_perc"] and len(manifestos) > 1:
            # Devolve cargas ao pool de não alocados com motivo
            for entrega in m["entregas"]:
                nao_alocados.append({
                    "nro_doc": entrega.get("nro_doc"),
                    "romane": entrega.get("romane"),
                    "destin": entrega.get("destin"),
                    "cidade": entrega.get("cidade"),
                    "uf": entrega.get("uf"),
                    "peso": entrega.get("peso_calculo"),
                    "vlr_merc": entrega.get("vlr_merc"),
                    "motivo": f"Manifesto abaixo da ocupação mínima ({ocup_perc:.1f}% < {m['ocup_minima_perc']}%)"
                })
        else:
            manifestos_validos.append(m)

    return manifestos_validos, nao_alocados


def executar(
    df_dedicados: pd.DataFrame,
    df_restritos: pd.DataFrame,
    df_livres: pd.DataFrame,
    veiculos: List[Dict],
    tipo_roteirizacao: str,
    configuracao_frota: List[Dict],
) -> Tuple[List[Dict], List[Dict], Dict]:
    """
    Consolida todos os grupos em manifestos.
    Retorna (manifestos, nao_alocados_total, log).
    """
    entrada_total = len(df_dedicados) + len(df_restritos) + len(df_livres)
    todos_nao_alocados = []

    # Filtrar frota disponível conforme tipo_roteirizacao
    if tipo_roteirizacao == "frota" and configuracao_frota:
        perfis_frota = {cfg["perfil"].upper(): cfg["quantidade"] for cfg in configuracao_frota}
        veiculos_frota = []
        for v in veiculos:
            perfil = v["perfil"].upper()
            if perfil in perfis_frota and perfis_frota[perfil] > 0:
                for _ in range(perfis_frota[perfil]):
                    veiculos_frota.append(v)
        veiculos_efetivos = veiculos_frota
    else:
        # Modo carteira: frota ilimitada (replica veículos conforme necessário)
        veiculos_efetivos = veiculos

    # 1. Dedicados
    manifestos_dedicados, nao_ded = _alocar_dedicados(df_dedicados, veiculos_efetivos)
    todos_nao_alocados.extend(nao_ded)

    # 2. Restritos (FFD com filtro de perfil)
    manifestos_restritos, nao_rest = _alocar_ffd(df_restritos, veiculos_efetivos, "composto", restricao_col="restricao_veiculo")
    todos_nao_alocados.extend(nao_rest)

    # 3. Livres (FFD puro)
    manifestos_livres, nao_liv = _alocar_ffd(df_livres, veiculos_efetivos, "composto")
    todos_nao_alocados.extend(nao_liv)

    todos_manifestos = manifestos_dedicados + manifestos_restritos + manifestos_livres
    total_roteirizado = sum(len(m["entregas"]) for m in todos_manifestos)

    log = {
        "etapa": "M4 — Consolidação",
        "descricao": "Alocação de cargas em veículos (First Fit Decreasing por peso + prioridade)",
        "qtd_entrada": entrada_total,
        "qtd_saida": total_roteirizado,
        "qtd_descartada": len(todos_nao_alocados),
        "detalhes": (
            f"Manifestos gerados: {len(todos_manifestos)} "
            f"(Fechados: {len(manifestos_dedicados)} | Compostos: {len(manifestos_restritos) + len(manifestos_livres)}) | "
            f"Não alocados: {len(todos_nao_alocados)}"
        )
    }

    return todos_manifestos, todos_nao_alocados, log
