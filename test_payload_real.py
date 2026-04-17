#!/usr/bin/env python3
"""
Teste do Pipeline M1→M7 com o payload real (390 linhas, REC CONTAGEM)
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from app.services.pipeline_service import executar_pipeline_completo


def main():
    # Carregar payload real
    payload_path = os.path.join(os.path.dirname(__file__), "..", "upload", "payload_swagger_corrigido_pretty.json")
    if not os.path.exists(payload_path):
        payload_path = "/home/ubuntu/upload/payload_swagger_corrigido_pretty.json"

    print("=" * 70)
    print("TESTE COM PAYLOAD REAL — REC CONTAGEM (390 linhas)")
    print("=" * 70)

    with open(payload_path, encoding="utf-8") as f:
        payload = json.load(f)

    print(f"Carteira:  {len(payload.get('carteira', []))} linhas")
    print(f"Veículos:  {len(payload.get('veiculos', []))} veículos")
    print(f"Filial:    {payload.get('filial', {}).get('nome')} ({payload.get('filial', {}).get('cidade')}/{payload.get('filial', {}).get('uf')})")
    print(f"Data base: {payload.get('data_base_roteirizacao')}")
    print()

    # Executar pipeline
    resultado = executar_pipeline_completo(payload)

    # ── Status ──────────────────────────────────────────────────
    status = resultado.get("status", "?")
    etapa_falha = resultado.get("etapa_falha", "")
    print(f"STATUS: {status}" + (f" | FALHA EM: {etapa_falha}" if etapa_falha else ""))

    if resultado.get("erro"):
        print(f"ERRO FATAL: {resultado['erro']}")
        tb = resultado.get("traceback", "")
        if tb:
            print(tb[-3000:])
        return

    # ── Resumo ──────────────────────────────────────────────────
    resumo = resultado.get("resumo", {})
    print()
    print("RESUMO GERAL:")
    print(f"  Linhas de entrada:        {resumo.get('total_linhas_entrada', 0)}")
    print(f"  Manifestos gerados:       {resumo.get('total_manifestos', 0)}")
    print(f"  Itens manifestados:       {resumo.get('total_itens_manifestados', 0)}")
    print(f"  Não roteirizados:         {resumo.get('total_nao_roteirizados', 0)}")
    print(f"  KM total da frota:        {resumo.get('km_total_frota', 0):.2f} km")
    print(f"  Ocupação média:           {resumo.get('ocupacao_media_perc', 0):.2f}%")
    print(f"  Tempo de processamento:   {resumo.get('tempo_processamento_ms', 0)} ms")

    # ── Encadeamento ────────────────────────────────────────────
    encadeamento = resultado.get("encadeamento", [])
    print()
    print(f"ENCADEAMENTO ({len(encadeamento)} etapas):")
    for e in encadeamento:
        print(f"  {e['etapa']:<45} entrada={e['entrada']:>4}  saida={e['saida_principal']:>4}  rem={e['remanescente']:>4}")

    # ── Erros parciais ──────────────────────────────────────────
    erros = resultado.get("erros", [])
    if erros:
        print()
        print(f"ERROS PARCIAIS ({len(erros)}):")
        for err in erros:
            print(f"  [{err['etapa']}] {err['erro'][:120]}")

    # ── Manifestos ──────────────────────────────────────────────
    manifestos = resultado.get("manifestos", [])
    print()
    print(f"MANIFESTOS ({len(manifestos)}):")
    for m in manifestos:
        man_id   = m.get("manifesto_id") or m.get("id_manifesto") or "?"
        perfil   = m.get("veiculo_perfil") or m.get("perfil_veiculo") or m.get("veiculo_tipo") or "?"
        tipo_man = m.get("tipo_manifesto") or "-"
        n_itens  = m.get("qtd_itens") or m.get("qtd_entregas") or "?"
        n_paradas= m.get("qtd_paradas") or 0
        ocup     = m.get("ocupacao_oficial_perc") or m.get("ocupacao_perc") or 0
        km       = m.get("km_rota_total") or m.get("km_referencia") or 0
        peso     = m.get("peso_total_kg") or 0
        eixos    = m.get("qtd_eixos") or "-"
        print(f"  {man_id} | {perfil:<8} | {eixos}eixos | itens={n_itens:>3} | paradas={n_paradas:>3} | ocup={float(ocup):.1f}% | km={float(km):.1f} | peso={float(peso):.0f}kg | {tipo_man}")

    # ── Não roteirizados ────────────────────────────────────────
    nao_rot = resultado.get("nao_roteirizados", [])
    print()
    print(f"NÃO ROTEIRIZADOS ({len(nao_rot)}):")
    motivos: dict = {}
    for nr in nao_rot:
        motivo = str(nr.get("motivo_nao_roteirizacao") or nr.get("motivo") or nr.get("status_triagem") or "sem_motivo")
        motivos[motivo] = motivos.get(motivo, 0) + 1
    for motivo, qtd in sorted(motivos.items(), key=lambda x: -x[1]):
        print(f"  [{qtd:>3}x] {motivo}")

    # ── Detalhes dos 5 primeiros não roteirizados ────────────────
    if nao_rot:
        print()
        print("  Detalhes (primeiros 5):")
        for nr in nao_rot[:5]:
            dest   = nr.get("destinatario") or nr.get("destin") or "?"
            cidade = nr.get("cidade") or nr.get("cidad") or "?"
            motivo = nr.get("motivo_nao_roteirizacao") or nr.get("motivo") or nr.get("status_triagem") or "?"
            peso   = nr.get("peso_calculado") or nr.get("peso") or 0
            print(f"    {dest[:30]:<30} | {cidade:<20} | {motivo} | {float(peso):.1f}kg")

    # ── Salvar resultado completo em JSON ────────────────────────
    output_path = os.path.join(os.path.dirname(__file__), "resultado_payload_real.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2, default=str)
    print()
    print(f"Resultado completo salvo em: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
