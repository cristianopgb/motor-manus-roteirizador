"""
M5 — Sequenciamento de Entregas
Responsabilidade: ordenar as entregas dentro de cada manifesto para
minimizar a distância total percorrida. Algoritmo: Nearest Neighbor (NN)
com ponto de partida na filial. Entregas URGENTE/ALTA são priorizadas
para as primeiras posições quando possível.
"""
from __future__ import annotations
import math
from typing import List, Dict, Tuple
from app.pipeline.m3_distancias import distancia_rodoviaria_km


def _nearest_neighbor_route(
    entregas: List[Dict],
    origem_lat: float,
    origem_lon: float,
) -> List[Dict]:
    """
    Algoritmo Nearest Neighbor: a partir da origem, sempre vai para a entrega
    mais próxima ainda não visitada. Retorna lista ordenada com km_origem e km_acumulado.
    """
    if not entregas:
        return []

    restantes = list(range(len(entregas)))
    rota: List[int] = []
    atual_lat = origem_lat
    atual_lon = origem_lon
    km_acumulado = 0.0

    while restantes:
        melhor_idx = None
        melhor_dist = float("inf")

        for i in restantes:
            e = entregas[i]
            d = distancia_rodoviaria_km(
                atual_lat, atual_lon,
                e["latitude"], e["longitude"]
            )
            if d < melhor_dist:
                melhor_dist = d
                melhor_idx = i

        rota.append(melhor_idx)
        restantes.remove(melhor_idx)
        e = entregas[melhor_idx]
        e["km_origem"] = round(melhor_dist, 2)
        km_acumulado += melhor_dist
        e["km_acumulado"] = round(km_acumulado, 2)
        atual_lat = e["latitude"]
        atual_lon = e["longitude"]

    return [entregas[i] for i in rota]


def _reordenar_por_prioridade(
    entregas_sequenciadas: List[Dict],
) -> List[Dict]:
    """
    Pós-processamento: move URGENTE e ALTA para o início da rota
    sem quebrar completamente a otimização de distância.
    """
    urgentes = [e for e in entregas_sequenciadas if e.get("prioridade") in ("URGENTE", "ALTA")]
    demais = [e for e in entregas_sequenciadas if e.get("prioridade") not in ("URGENTE", "ALTA")]
    return urgentes + demais


def _calcular_km_total(entregas: List[Dict]) -> float:
    if not entregas:
        return 0.0
    return entregas[-1].get("km_acumulado", 0.0)


def _recalcular_sequencia(entregas: List[Dict]) -> List[Dict]:
    """Renumera a sequência após reordenação por prioridade."""
    for i, e in enumerate(entregas):
        e["sequencia"] = i + 1
    return entregas


def executar(
    manifestos: List[Dict],
    filial_lat: float,
    filial_lon: float,
) -> Tuple[List[Dict], Dict]:
    """
    Sequencia as entregas de cada manifesto e calcula km total.
    Retorna (manifestos_sequenciados, log).
    """
    entrada_total = sum(len(m["entregas"]) for m in manifestos)

    for manifesto in manifestos:
        entregas = manifesto["entregas"]
        if not entregas:
            continue

        # Sequenciamento por Nearest Neighbor
        entregas_seq = _nearest_neighbor_route(entregas, filial_lat, filial_lon)

        # Reordenar urgentes/alta para o início
        entregas_seq = _reordenar_por_prioridade(entregas_seq)

        # Recalcular km acumulado após reordenação
        km_acum = 0.0
        atual_lat, atual_lon = filial_lat, filial_lon
        for e in entregas_seq:
            d = distancia_rodoviaria_km(atual_lat, atual_lon, e["latitude"], e["longitude"])
            e["km_origem"] = round(d, 2)
            km_acum += d
            e["km_acumulado"] = round(km_acum, 2)
            atual_lat = e["latitude"]
            atual_lon = e["longitude"]

        # Numeração final de sequência
        entregas_seq = _recalcular_sequencia(entregas_seq)
        manifesto["entregas"] = entregas_seq
        manifesto["km_total_estimado"] = round(km_acum, 2)

    log = {
        "etapa": "M5 — Sequenciamento",
        "descricao": "Ordenação de entregas por Nearest Neighbor + priorização de URGENTE/ALTA",
        "qtd_entrada": entrada_total,
        "qtd_saida": entrada_total,
        "qtd_descartada": 0,
        "detalhes": (
            f"Manifestos sequenciados: {len(manifestos)} | "
            f"KM total estimado (soma): {sum(m.get('km_total_estimado', 0) for m in manifestos):.1f} km"
        )
    }

    return manifestos, log
