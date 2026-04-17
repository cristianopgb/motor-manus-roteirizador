"""
Teste do contrato de retorno do Motor para o Sistema 1.
Valida a estrutura completa do JSON de resposta.
"""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import executar_pipeline_completo
from app.services.response_service import montar_resposta_sistema1

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

print("Executando pipeline...")
resultado_bruto = executar_pipeline_completo(payload)
resposta = montar_resposta_sistema1(resultado_bruto)

print()
print("=" * 70)
print("CONTRATO DE RETORNO — SISTEMA 1")
print("=" * 70)

# 1. Status
print(f"\n[STATUS]: {resposta['status']}")
print(f"[ERRO]:   {resposta['erro']}")

# 2. Resumo / KPIs
print()
print("── RESUMO (Dashboard) ──────────────────────────────────────────────")
r = resposta['resumo']
print(f"  Data Base Roteirização:    {r['data_base_roteirizacao']}")
print(f"  Total Cargas Entrada:      {r['total_cargas_entrada']}")
print(f"  Total Manifestos Gerados:  {r['total_manifestos']}")
print(f"  Total Itens Manifestados:  {r['total_itens_manifestados']}")
print(f"  Total Não Roteirizados:    {r['total_nao_roteirizados']}")
print(f"  ├─ Roteirizáveis:          {r['total_roteirizaveis']}")
print(f"  ├─ Agendamento Futuro:     {r['total_agendamento_futuro']}")
print(f"  ├─ Agenda Vencida:         {r['total_agenda_vencida']}")
print(f"  └─ Exceção Triagem:        {r['total_excecao_triagem']}")
print(f"  KM Total da Frota:         {r['km_total_frota']} km")
print(f"  Ocupação Média:            {r['ocupacao_media_percentual']}%")
print(f"  Tempo Processamento:       {r['tempo_processamento_ms']} ms")

# 3. Encadeamento
print()
print("── ENCADEAMENTO (M1→M7) ────────────────────────────────────────────")
for e in resposta['encadeamento']:
    print(f"  {e['etapa']:<40} entrada={e['entrada']:>4}  saida={e['saida_principal']:>4}  rem={e['remanescente']:>4}")

# 4. Manifestos
print()
print("── MANIFESTOS ──────────────────────────────────────────────────────")
for m in resposta['manifestos']:
    print(f"  [{m['numero_manifesto']:>2}] {m['id_manifesto']:<20} | {m['veiculo_tipo']:<8} | "
          f"entregas={m['total_entregas']:>3} | peso={m['total_peso_kg'] or 0:>8.1f}kg | "
          f"km={m['km_estimado'] or 0:>7.1f} | ocup={m['ocupacao_percentual'] or 0:>5.1f}% | "
          f"agend={len(m['agendamentos']):>2}")

# 5. Primeira entrega do primeiro manifesto
print()
print("── AMOSTRA: MANIFESTO 1 — CABEÇALHO ───────────────────────────────")
m1 = resposta['manifestos'][0]
for k, v in m1.items():
    if k not in ('entregas', 'agendamentos'):
        print(f"  {k}: {v}")

print()
print("── AMOSTRA: MANIFESTO 1 — ENTREGA 1 ───────────────────────────────")
if m1['entregas']:
    e1 = m1['entregas'][0]
    for k, v in e1.items():
        print(f"  {k}: {v}")

print()
print("── AMOSTRA: MANIFESTO 1 — AGENDAMENTOS ────────────────────────────")
for ag in m1['agendamentos']:
    print(f"  CTE={ag['nro_documento']} | {ag['destinatario']} | {ag['cidade']}/{ag['uf']} | {ag['data_agenda']} {ag['hora_agenda']}")
if not m1['agendamentos']:
    print("  (nenhum agendamento neste manifesto)")

# 6. Não roteirizados
print()
print("── NÃO ROTEIRIZADOS ────────────────────────────────────────────────")
print(f"  Total:                 {len(resposta['nao_roteirizados'])}")
print(f"  Agendamento Futuro:    {len(resposta['cargas_agendamento_futuro'])}")
print(f"  Agenda Vencida:        {len(resposta['cargas_agenda_vencida'])}")
print(f"  Exceção Triagem:       {len(resposta['cargas_excecao_triagem'])}")
print(f"  Não Alocados:          {len(resposta['cargas_nao_alocadas'])}")

print()
print("── AMOSTRA: NÃO ROTEIRIZADO 1 ─────────────────────────────────────")
if resposta['nao_roteirizados']:
    nr1 = resposta['nao_roteirizados'][0]
    for k, v in nr1.items():
        print(f"  {k}: {v}")

# 7. Validação de campos obrigatórios
print()
print("── VALIDAÇÃO DE CAMPOS OBRIGATÓRIOS ────────────────────────────────")
erros = []

# Resumo
for campo in ['data_base_roteirizacao', 'total_cargas_entrada', 'total_manifestos',
              'total_itens_manifestados', 'km_total_frota', 'ocupacao_media_percentual',
              'tempo_processamento_ms']:
    if resposta['resumo'].get(campo) is None:
        erros.append(f"  FALTA resumo.{campo}")

# Manifesto
for m in resposta['manifestos']:
    for campo in ['id_manifesto', 'numero_manifesto', 'veiculo_tipo', 'total_entregas', 'entregas']:
        if not m.get(campo) and m.get(campo) != 0:
            erros.append(f"  FALTA manifesto[{m['numero_manifesto']}].{campo}")
    # Entrega
    for e in m['entregas']:
        for campo in ['sequencia', 'nro_documento', 'destinatario', 'cidade']:
            if not e.get(campo) and e.get(campo) != 0:
                erros.append(f"  FALTA entrega[seq={e.get('sequencia')}].{campo}")

if erros:
    print("  ERROS ENCONTRADOS:")
    for err in erros[:20]:
        print(err)
else:
    print("  Todos os campos obrigatórios presentes ✓")

# 8. Exportar amostra do JSON
print()
print("── EXPORTANDO JSON DE AMOSTRA (manifesto 1) ───────────────────────")
amostra = {
    "status": resposta["status"],
    "resumo": resposta["resumo"],
    "encadeamento": resposta["encadeamento"],
    "manifestos": [resposta["manifestos"][0]] if resposta["manifestos"] else [],
    "nao_roteirizados": resposta["nao_roteirizados"][:3],
    "cargas_agendamento_futuro": resposta["cargas_agendamento_futuro"][:3],
    "cargas_agenda_vencida": resposta["cargas_agenda_vencida"][:3],
}

with open('/home/ubuntu/motor-manus-roteirizador/sample_response_sistema1.json', 'w', encoding='utf-8') as f:
    json.dump(amostra, f, ensure_ascii=False, indent=2, default=str)

print("  Exportado: sample_response_sistema1.json")
print()
print("=" * 70)
print("CONTRATO VALIDADO COM SUCESSO")
print("=" * 70)
