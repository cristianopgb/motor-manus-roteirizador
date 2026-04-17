"""
Debug: inspecionar folga_dias calculado pelo M2 para as 390 cargas do payload real.
Comparar com o pipeline original: 272 roteirizavel vs 156 no motor novo.
"""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import _payload_to_dataframes
from app.pipeline.m1_padronizacao import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

df_c, df_g, df_p, df_v = _payload_to_dataframes(payload)
r1 = executar_m1_padronizacao(df_c, df_g, df_p, df_v)
r2 = executar_m2_enriquecimento(r1['df_carteira_tratada'], r1['df_geo_tratado'], r1['param_dict'])

df = r2['df_carteira_enriquecida']

print(f"Total cargas: {len(df)}")
print(f"Data base: {r1['param_dict'].get('data_base')}")
print()

# Verificar colunas de data
print("Colunas de data disponíveis:")
for c in df.columns:
    if any(x in c for x in ['data', 'folga', 'agenda', 'leadtime', 'limite', 'dias', 'transit']):
        nulos = df[c].isna().sum()
        print(f"  {c}: {nulos} nulos de {len(df)}")

print()
print("=== DISTRIBUIÇÃO DE folga_dias ===")
import pandas as pd
folga = pd.to_numeric(df['folga_dias'], errors='coerce')
print(f"  Nulos: {folga.isna().sum()}")
print(f"  < 0 (agenda_vencida): {(folga < 0).sum()}")
print(f"  == 0: {(folga == 0).sum()}")
print(f"  == 1: {(folga == 1).sum()}")
print(f"  >= 2 (futuro): {(folga >= 2).sum()}")
print(f"  0 <= folga < 2 (roteirizavel): {((folga >= 0) & (folga < 2)).sum()}")

print()
print("=== DISTRIBUIÇÃO DE status_triagem (se já calculado) ===")
if 'status_triagem' in df.columns:
    print(df['status_triagem'].value_counts().to_string())
else:
    print("  status_triagem não calculado no M2")

print()
print("=== AMOSTRA: cargas com folga < 0 (agenda_vencida) ===")
vencidas = df[folga < 0][['destin', 'cidade', 'data_agenda', 'data_leadtime', 'data_descarga', 'data_limite_considerada', 'folga_dias', 'agendada']].head(10)
print(vencidas.to_string())

print()
print("=== AMOSTRA: cargas com folga >= 0 e < 2 (roteirizavel) ===")
rot = df[(folga >= 0) & (folga < 2)][['destin', 'cidade', 'data_agenda', 'data_leadtime', 'data_descarga', 'data_limite_considerada', 'folga_dias', 'agendada']].head(10)
print(rot.to_string())

print()
print("=== VERIFICAR: data_limite_considerada ===")
if 'data_limite_considerada' in df.columns:
    print(f"  Nulos: {df['data_limite_considerada'].isna().sum()}")
    print(f"  Amostra de valores:")
    print(df['data_limite_considerada'].dropna().head(5).to_string())
else:
    print("  COLUNA AUSENTE!")

print()
print("=== VERIFICAR: agendada ===")
if 'agendada' in df.columns:
    print(df['agendada'].value_counts().to_string())
else:
    print("  COLUNA AUSENTE!")
