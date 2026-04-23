-- M5.2 auditoria flat: colunas de pré-manifestos e tentativas operacionais
alter table public.auditoria_pipeline_modular add column if not exists ocupacao_maxima_perc_veiculo text;

alter table public.auditoria_pipeline_modular add column if not exists tentativa_idx text;
alter table public.auditoria_pipeline_modular add column if not exists blocos_considerados text;
alter table public.auditoria_pipeline_modular add column if not exists veiculo_tipo_tentado text;
alter table public.auditoria_pipeline_modular add column if not exists veiculo_perfil_tentado text;
alter table public.auditoria_pipeline_modular add column if not exists resultado text;
alter table public.auditoria_pipeline_modular add column if not exists motivo text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_itens_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_paradas_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists peso_total_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists peso_kg_total_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists volume_total_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists km_referencia_candidato text;
alter table public.auditoria_pipeline_modular add column if not exists ocupacao_perc_candidato text;

create index if not exists idx_auditoria_pipeline_modular_snapshot_m5_2
    on public.auditoria_pipeline_modular (snapshot_nome, modulo);
