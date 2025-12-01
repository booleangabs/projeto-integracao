{{ config(
    materialized='table',
    unique_key='caso_key',
) }}

SELECT
    stg.surrogate_key_caso AS caso_key,

    CAST(TO_CHAR(stg.data_notificacao, 'YYYYMMDD') AS INT) AS data_key,
    {{ dbt_utils.generate_surrogate_key(['stg.id_municipio', 'stg.uf_notificacao']) }} AS localidade_key,

    1 AS contagem_casos, 
    
    stg.idade,
    stg.sexo,
    stg.classificacao_final,
    
    (stg.data_encerramento - stg.data_notificacao) AS dias_para_encerramento,
    
    stg.ano_fonte 
    
FROM {{ ref('stg_dengue') }} stg

WHERE stg.data_notificacao IS NOT NULL
  AND stg.id_municipio IS NOT NULL
  AND stg.classificacao_final IS NOT NULL
