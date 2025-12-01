{{ config(
    materialized='table',
    unique_key='data_key',
) }}

WITH datas_notificacao AS (
    SELECT DISTINCT
        data_notificacao
    FROM {{ ref('stg_dengue') }}
    WHERE data_notificacao IS NOT NULL
),

dimensao_tempo AS (
    SELECT
        CAST(TO_CHAR(d.data_notificacao, 'YYYYMMDD') AS INT) AS data_key,

        d.data_notificacao AS data_completa,
        EXTRACT(YEAR FROM d.data_notificacao) AS ano,
        EXTRACT(MONTH FROM d.data_notificacao) AS mes,
        TO_CHAR(d.data_notificacao, 'Month') AS nome_mes,
        EXTRACT(DAY FROM d.data_notificacao) AS dia_do_mes,
        EXTRACT(DOW FROM d.data_notificacao) AS dia_da_semana_num,
        TO_CHAR(d.data_notificacao, 'Day') AS nome_dia_semana,

        EXTRACT(WEEK FROM d.data_notificacao) AS semana_do_ano,
        EXTRACT(QUARTER FROM d.data_notificacao) AS trimestre,

        CASE WHEN EXTRACT(DOW FROM d.data_notificacao) IN (0, 6) THEN TRUE ELSE FALSE END AS is_fim_de_semana,
        CASE WHEN EXTRACT(MONTH FROM d.data_notificacao) IN (1, 2, 3) THEN 'Verão'
             WHEN EXTRACT(MONTH FROM d.data_notificacao) IN (4, 5, 6) THEN 'Outono'
             WHEN EXTRACT(MONTH FROM d.data_notificacao) IN (7, 8, 9) THEN 'Inverno'
             ELSE 'Primavera' END AS estacao_do_ano
    FROM datas_notificacao d
)

SELECT * FROM dimensao_tempo
ORDER BY data_key