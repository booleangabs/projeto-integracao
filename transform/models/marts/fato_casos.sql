{{ config(
    materialized='table',
    unique_key='caso_key',
) }}

SELECT
    stg.surrogate_key_caso AS caso_key,

    -- Foreign Keys para Dimensões
    CAST(TO_CHAR(stg.data_notificacao, 'YYYYMMDD') AS INT) AS data_key,
    {{ dbt_utils.generate_surrogate_key(['stg.id_municipio', 'stg.uf_notificacao']) }} AS localidade_key,
    {{ dbt_utils.generate_surrogate_key(['stg.id_municipio', 'stg.sexo', 'stg.idade', 'stg.raca']) }} AS paciente_key,
    {{ dbt_utils.generate_surrogate_key(['stg.surrogate_key_caso']) }} AS quadro_clinico_key,
    {{ dbt_utils.generate_surrogate_key(['stg.surrogate_key_caso']) }} AS exame_key,
    {{ dbt_utils.generate_surrogate_key(['stg.id_unidade_saude', 'stg.id_municipio', 'stg.uf_notificacao']) }} AS unidade_saude_key,
    {{ dbt_utils.generate_surrogate_key(['stg.surrogate_key_caso']) }} AS notificacao_key,

    -- Métricas
    1 AS contagem_casos,

    -- Atributos Degenerados (mantidos na fato para performance)
    stg.classificacao_final,
    stg.evolucao,
    stg.hospitalizacao,
    stg.criterio_confirmacao,

    -- Métricas Calculadas
    (stg.data_encerramento - stg.data_notificacao) AS dias_para_encerramento,
    (stg.data_investigacao - stg.data_notificacao) AS dias_para_investigacao,
    (stg.data_sintomas - stg.data_notificacao) AS dias_sintomas_ate_notificacao,

    -- Metadados
    stg.ano_fonte

FROM {{ ref('stg_dengue') }} stg

WHERE stg.data_notificacao IS NOT NULL
  AND stg.id_municipio IS NOT NULL
  AND stg.classificacao_final IS NOT NULL
