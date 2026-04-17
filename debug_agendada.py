"""Debug: verificar como agendada é definida e comparar com o pipeline original."""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import _payload_to_dataframes
from app.pipeline.m1_padronizacao import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento
import pandas as pd

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

df_c, df_g, df_p, df_v = _payload_to_dataframes(payload)
r1 = executar_m1_padronizacao(df_c, df_g, df_p, df_v)
df_m1 = r1['df_carteira_tratada']

print("=== APÓS M1 — colunas de agenda ===")
cols_agenda = [c for c in df_m1.columns if 'agenda' in c.lower() or 'agendam' in c.lower()]
print(f"Colunas: {cols_agenda}")
print(f"agendada=True: {df_m1['agendada'].sum() if 'agendada' in df_m1.columns else 'N/A'}")
print(f"agendada=False: {(~df_m1['agendada']).sum() if 'agendada' in df_m1.columns else 'N/A'}")

print()
print("=== DISTRIBUIÇÃO DE data_agenda no M1 ===")
if 'data_agenda' in df_m1.columns:
    print(f"  Nulos: {df_m1['data_agenda'].isna().sum()}")
    print(f"  Não nulos: {df_m1['data_agenda'].notna().sum()}")
    print(f"  Amostra não nulos:")
    print(df_m1[df_m1['data_agenda'].notna()][['data_agenda', 'agendada']].head(5).to_string())

print()
print("=== DISTRIBUIÇÃO DE agendam (campo original) no M1 ===")
if 'agendam' in df_m1.columns:
    print(f"  Nulos: {df_m1['agendam'].isna().sum()}")
    print(f"  Não nulos: {df_m1['agendam'].notna().sum()}")
    print(f"  Amostra não nulos:")
    print(df_m1[df_m1['agendam'].notna()][['agendam', 'data_agenda', 'agendada']].head(5).to_string())

print()
print("=== DISTRIBUIÇÃO DE agenda (campo original) no M1 ===")
if 'agenda' in df_m1.columns:
    print(f"  Nulos: {df_m1['agenda'].isna().sum()}")
    print(f"  Não nulos: {df_m1['agenda'].notna().sum()}")
    print(f"  Valores únicos: {df_m1['agenda'].dropna().unique()[:10]}")

print()
r2 = executar_m2_enriquecimento(df_m1, r1['df_geo_tratado'], r1['param_dict'])
df_m2 = r2['df_carteira_enriquecida']
import pandas as pd
folga = pd.to_numeric(df_m2['folga_dias'], errors='coerce')

print("=== APÓS M2 — agendada vs data_agenda ===")
print(f"  agendada=True, data_agenda notna: {((df_m2['agendada']==True) & df_m2['data_agenda'].notna()).sum()}")
print(f"  agendada=True, data_agenda nula:  {((df_m2['agendada']==True) & df_m2['data_agenda'].isna()).sum()}")
print(f"  agendada=False, data_agenda notna:{((df_m2['agendada']==False) & df_m2['data_agenda'].notna()).sum()}")
print(f"  agendada=False, data_agenda nula: {((df_m2['agendada']==False) & df_m2['data_agenda'].isna()).sum()}")

print()
print("=== SIMULAÇÃO DO M3 COM REGRA ORIGINAL ===")
# Regra original: sem data_agenda + tem data_leadtime = roteirizavel
sem_agenda_com_dle = ((df_m2['data_agenda'].isna()) & (df_m2['data_leadtime'].notna())).sum()
sem_agenda_sem_dle = ((df_m2['data_agenda'].isna()) & (df_m2['data_leadtime'].isna())).sum()
com_agenda_rot = ((df_m2['data_agenda'].notna()) & (folga >= 0) & (folga < 2)).sum()
com_agenda_fut = ((df_m2['data_agenda'].notna()) & (folga >= 2)).sum()
com_agenda_ven = ((df_m2['data_agenda'].notna()) & (folga < 0)).sum()
print(f"  Sem agenda + com DLE (roteirizavel): {sem_agenda_com_dle}")
print(f"  Sem agenda + sem DLE (excecao):      {sem_agenda_sem_dle}")
print(f"  Com agenda + 0<=folga<2 (roteiriz.): {com_agenda_rot}")
print(f"  Com agenda + folga>=2 (futuro):      {com_agenda_fut}")
print(f"  Com agenda + folga<0 (vencida):      {com_agenda_ven}")
print(f"  TOTAL roteirizavel esperado:         {sem_agenda_com_dle + com_agenda_rot}")
print(f"  ORIGINAL tinha:                      272")
