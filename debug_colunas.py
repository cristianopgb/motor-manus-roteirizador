import unicodedata, re, sys
sys.path.insert(0, '.')
from app.pipeline.m1_padronizacao import padronizar_nome_coluna

cols_reais = ['Filial R', 'Romane', 'Filial D', 'Série', 'Nro Doc.', 'Data Des', 'Data NF',
              'D.L.E.', 'Agendam.', 'Palet', 'Conf', 'Peso', 'Vlr.Merc.', 'Qtd.', 'Peso Cub.',
              'Classif', 'Tomad', 'Destin', 'Bairro', 'Cidad', 'UF', 'NF / Serie', 'Tipo Ca',
              'Qtd.NF', 'Mesoregião', 'Sub-Região', 'Ocorrências NF', 'Remetente', 'Observação',
              'Ref Cliente', 'Cidade Dest.', 'Agenda', 'Tipo Carga', 'Última Ocorrência',
              'Status R', 'Latitude', 'Longitude', 'Peso Calculo', 'Prioridade',
              'Restrição Veículo', 'Carro Dedicado', 'Inicio Ent.', 'Fim En']

print('Mapeamento de colunas reais -> normalizadas:')
for c in cols_reais:
    norm = padronizar_nome_coluna(c)
    print(f'  {c:30s} -> {norm}')

print()
print('Colunas relevantes para data/agenda:')
relevantes = ['D.L.E.', 'Agendam.', 'Data Des', 'Data NF', 'Agenda']
for c in relevantes:
    norm = padronizar_nome_coluna(c)
    print(f'  {c:20s} -> {norm}')

print()
print('Coalesce esperado no M1:')
print('  data_des  <- ["data_des", "data", "data_d", "data_descarga"]')
print('  dle       <- ["dle", "data_leadtime"]')
print('  agendam   <- ["agendam", "data_agenda", "agenda"]')
print()
print('Resultado:')
print(f'  "D.L.E."  -> "{padronizar_nome_coluna("D.L.E.")}" -> coalesce para "dle"? -> data_leadtime')
print(f'  "Agendam."-> "{padronizar_nome_coluna("Agendam.")}" -> coalesce para "agendam"? -> data_agenda')
print(f'  "Data Des"-> "{padronizar_nome_coluna("Data Des")}" -> coalesce para "data_des"? -> data_descarga')
