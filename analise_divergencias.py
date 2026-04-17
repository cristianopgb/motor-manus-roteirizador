"""
Análise comparativa entre pipeline original e motor novo.
Identifica divergências etapa a etapa.
"""
import json, sys
sys.path.insert(0, '.')

# ── Dados do pipeline ORIGINAL ──────────────────────────────────
original = {
    "M3_triagem":                  {"roteirizavel": 272, "agenda_vencida": 66, "agendamento_futuro": 51, "excecao": 1},
    "M3_1_validacao_fronteira":    {"entrada": 272, "saida": 272},
    "M4_manifestos_fechados":      {"entrada": 272, "manifestos": 13, "itens_manifestados": 30, "remanescente": 242},
    "M5_1_triagem_cidades":        {"entrada": 242, "elegivel": 128, "nao_elegivel": 73},
    "M5_2_composicao_cidades":     {"entrada": 128, "itens": 55, "remanescente": 73},
    "M5_3_triagem_subregioes":     {"entrada": 73, "elegivel": 72},
    "M5_3b_composicao_subregioes": {"entrada": 72, "itens": 13, "remanescente": 59},
    "M5_4a_triagem_mesorregioes":  {"entrada": 59, "elegivel": 59},
    "M5_4b_composicao_mesorregioes":{"entrada": 59, "itens": 6, "remanescente": 53},
    "M6_1_consolidacao":           {"entrada": 21, "manifestos": 21},
    "M6_2_complemento_ocupacao":   {"entrada_itens": 104, "itens_complementados": 50, "total_itens_m6_2": 154, "remanescente": 3},
    "M7_sequenciamento":           {"manifestos": 21, "itens": 154},
}

# ── Dados do motor NOVO (último teste) ─────────────────────────
novo = {
    "M3_triagem":                  {"roteirizavel": 156, "agenda_vencida": 233, "agendamento_futuro": 0, "excecao": 1},
    "M3_1_validacao_fronteira":    {"entrada": 156, "saida": 156},
    "M4_manifestos_fechados":      {"entrada": 156, "manifestos": 8, "itens_manifestados": 16, "remanescente": 140},
    "M5_1_triagem_cidades":        {"entrada": 140, "elegivel": 64, "nao_elegivel": 76},
    "M5_2_composicao_cidades":     {"entrada": 64, "itens": 28, "remanescente": 36},
    "M5_3_composicao_subregioes":  {"entrada": 112, "itens": 3, "remanescente": 90},
    "M5_4_composicao_mesorregioes":{"entrada": 90, "itens": 2, "remanescente": 69},
    "M6_1_consolidacao":           {"entrada": 17, "manifestos": 17},
    "M6_2_complemento_ocupacao":   {"entrada_itens": 87, "itens_complementados": 24, "total_itens_m6_2": 111, "remanescente": 45},
    "M7_sequenciamento":           {"manifestos": 17, "itens": 111},
}

print("=" * 80)
print("ANÁLISE COMPARATIVA: PIPELINE ORIGINAL vs MOTOR NOVO")
print("=" * 80)

print("\n[DIVERGÊNCIA CRÍTICA 1 — M3 TRIAGEM]")
print(f"  ORIGINAL: roteirizavel=272, agenda_vencida=66, agendamento_futuro=51, excecao=1")
print(f"  NOVO:     roteirizavel=156, agenda_vencida=233, agendamento_futuro=0,  excecao=1")
print(f"  DELTA:    roteirizavel={156-272} ({272-156} a menos), agenda_vencida={233-66} (+{233-66})")
print(f"  IMPACTO:  116 cargas que deveriam ser 'roteirizavel' estão sendo classificadas como 'agenda_vencida'")
print(f"  CAUSA PROVÁVEL: Regra de folga_dias diferente ou cálculo de data_limite errado")

print("\n[DIVERGÊNCIA 2 — M4 MANIFESTOS FECHADOS]")
print(f"  ORIGINAL: entrada=272, manifestos=13, itens=30, remanescente=242")
print(f"  NOVO:     entrada=156, manifestos=8,  itens=16, remanescente=140")
print(f"  NOTA: Diferença proporcional ao M3 - se M3 for corrigido, M4 deve melhorar")

print("\n[DIVERGÊNCIA 3 — M5.1 TRIAGEM CIDADES]")
print(f"  ORIGINAL: entrada=242, elegivel=128, nao_elegivel=73 (restante=41 sem info)")
print(f"  NOVO:     entrada=140, elegivel=64,  nao_elegivel=76")
print(f"  NOTA: Proporcional ao M3. Mas nao_elegivel no novo é maior (76 vs 73) relativamente")

print("\n[DIVERGÊNCIA 4 — M5.3 TRIAGEM/COMPOSIÇÃO]")
print(f"  ORIGINAL: M5.3 tem etapa de TRIAGEM separada (73→72 elegíveis) antes da composição")
print(f"  NOVO:     M5.3 não tem etapa de triagem - vai direto para composição")
print(f"  CAUSA: Motor novo não implementou m5_3_triagem_subregioes.py separado")

print("\n[DIVERGÊNCIA 5 — M5.4 TRIAGEM/COMPOSIÇÃO]")
print(f"  ORIGINAL: M5.4a triagem (59→59) + M5.4b composição (59→6 itens, rem=53)")
print(f"  NOVO:     M5.4 direto composição (90→2 itens, rem=69)")
print(f"  CAUSA: Motor novo não implementou m5_4a_triagem_mesorregioes.py separado")

print("\n[DIVERGÊNCIA 6 — M6.2 COMPLEMENTO]")
print(f"  ORIGINAL: 104 itens base → +50 complementados = 154 total, rem=3")
print(f"  NOVO:     87 itens base  → +24 complementados = 111 total, rem=45")
print(f"  NOTA: Proporcional ao M3. Se M3 for corrigido, M6.2 deve melhorar")

print("\n" + "=" * 80)
print("CAUSA RAIZ PRINCIPAL: M3 TRIAGEM")
print("=" * 80)
print("""
O pipeline original classifica 272 cargas como 'roteirizavel' vs 156 no motor novo.
A diferença de 116 cargas está sendo classificada como 'agenda_vencida' no motor novo.

Regra do M3 original (confirmada pelos logs):
  - roteirizavel:        0 <= folga_dias < 2  (inclui leadtime SEM agenda)
  - agendamento_futuro:  folga_dias >= 2
  - agenda_vencida:      folga_dias < 0

O motor novo tem a mesma regra, mas o cálculo de folga_dias pode estar errado.
Hipóteses:
  1. data_limite_considerada calculada diferente (leadtime vs data_agenda)
  2. Timezone: datas com +00:00 sendo comparadas errado (off-by-1-day)
  3. Cargas SEM agenda: no original 156 vão por leadtime, no novo podem estar
     sendo tratadas como 'sem data' e caindo em excecao ou agenda_vencida
""")

# Verificar no payload real quantas cargas têm agenda vs leadtime
with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

carteira = payload.get('carteira', [])
com_agenda = sum(1 for c in carteira if c.get('data_agenda') or c.get('agenda'))
sem_agenda = len(carteira) - com_agenda
print(f"Payload real: {len(carteira)} cargas total")
print(f"  Com data_agenda: {com_agenda}")
print(f"  Sem data_agenda: {sem_agenda}")

# Verificar campos de data disponíveis
if carteira:
    amostra = carteira[0]
    campos_data = [k for k in amostra.keys() if 'data' in k.lower() or 'agenda' in k.lower() or 'lead' in k.lower() or 'dle' in k.lower()]
    print(f"\nCampos de data na carteira: {campos_data}")
    
    # Mostrar amostra de valores
    for c in carteira[:3]:
        print(f"\n  doc={c.get('nro_documento','?')} | data_agenda={c.get('data_agenda','N/A')} | data_leadtime={c.get('data_leadtime','N/A')} | data_descarga={c.get('data_descarga','N/A')}")
