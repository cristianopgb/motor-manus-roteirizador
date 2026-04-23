-- Auditoria FLAT M4: colunas físicas reais para snapshots m4_manifestos, m4_itens e m4_remanescente

alter table public.auditoria_pipeline_modular add column if not exists manifesto_id text;
alter table public.auditoria_pipeline_modular add column if not exists tipo_manifesto text;
alter table public.auditoria_pipeline_modular add column if not exists veiculo_tipo text;
alter table public.auditoria_pipeline_modular add column if not exists veiculo_perfil text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_eixos text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_itens text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_ctes text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_paradas text;
alter table public.auditoria_pipeline_modular add column if not exists base_carga_oficial text;
alter table public.auditoria_pipeline_modular add column if not exists peso_total_kg text;
alter table public.auditoria_pipeline_modular add column if not exists vol_total_m3 text;
alter table public.auditoria_pipeline_modular add column if not exists km_referencia text;
alter table public.auditoria_pipeline_modular add column if not exists ocupacao_oficial_perc text;
alter table public.auditoria_pipeline_modular add column if not exists capacidade_peso_kg_veiculo text;
alter table public.auditoria_pipeline_modular add column if not exists capacidade_vol_m3_veiculo text;
alter table public.auditoria_pipeline_modular add column if not exists max_entregas_veiculo text;
alter table public.auditoria_pipeline_modular add column if not exists max_km_distancia_veiculo text;
alter table public.auditoria_pipeline_modular add column if not exists ocupacao_minima_perc_veiculo text;
alter table public.auditoria_pipeline_modular add column if not exists ignorar_ocupacao_minima text;
alter table public.auditoria_pipeline_modular add column if not exists origem_modulo text;
alter table public.auditoria_pipeline_modular add column if not exists origem_etapa text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_nao_roteirizavel text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_final_remanescente_m4 text;

create index if not exists idx_auditoria_pipeline_modular_snapshot_m4 on public.auditoria_pipeline_modular (snapshot_nome, modulo);
