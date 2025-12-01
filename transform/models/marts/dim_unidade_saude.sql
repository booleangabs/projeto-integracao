{{ config(
    materialized='table',
    unique_key='unidade_saude_key',
) }}

WITH unidades_base AS (
    SELECT DISTINCT
        id_unidade_saude,
        id_municipio,
        uf_notificacao,
        id_regional_saude

    FROM {{ ref('stg_dengue') }}
    WHERE id_unidade_saude IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['id_unidade_saude', 'id_municipio', 'uf_notificacao']) }} AS unidade_saude_key,

    id_unidade_saude AS codigo_unidade,
    id_municipio AS municipio_unidade,
    uf_notificacao AS uf_unidade,
    id_regional_saude AS regional_saude,

    -- Mapeamento de UF
    CASE
        WHEN uf_notificacao = '11' THEN 'Rondônia'
        WHEN uf_notificacao = '12' THEN 'Acre'
        WHEN uf_notificacao = '13' THEN 'Amazonas'
        WHEN uf_notificacao = '14' THEN 'Roraima'
        WHEN uf_notificacao = '15' THEN 'Pará'
        WHEN uf_notificacao = '16' THEN 'Amapá'
        WHEN uf_notificacao = '17' THEN 'Tocantins'
        WHEN uf_notificacao = '21' THEN 'Maranhão'
        WHEN uf_notificacao = '22' THEN 'Piauí'
        WHEN uf_notificacao = '23' THEN 'Ceará'
        WHEN uf_notificacao = '24' THEN 'Rio Grande do Norte'
        WHEN uf_notificacao = '25' THEN 'Paraíba'
        WHEN uf_notificacao = '26' THEN 'Pernambuco'
        WHEN uf_notificacao = '27' THEN 'Alagoas'
        WHEN uf_notificacao = '28' THEN 'Sergipe'
        WHEN uf_notificacao = '29' THEN 'Bahia'
        WHEN uf_notificacao = '31' THEN 'Minas Gerais'
        WHEN uf_notificacao = '32' THEN 'Espírito Santo'
        WHEN uf_notificacao = '33' THEN 'Rio de Janeiro'
        WHEN uf_notificacao = '35' THEN 'São Paulo'
        WHEN uf_notificacao = '41' THEN 'Paraná'
        WHEN uf_notificacao = '42' THEN 'Santa Catarina'
        WHEN uf_notificacao = '43' THEN 'Rio Grande do Sul'
        WHEN uf_notificacao = '50' THEN 'Mato Grosso do Sul'
        WHEN uf_notificacao = '51' THEN 'Mato Grosso'
        WHEN uf_notificacao = '52' THEN 'Goiás'
        WHEN uf_notificacao = '53' THEN 'Distrito Federal'
        ELSE 'UF Desconhecida'
    END AS nome_estado,

    -- Região
    CASE
        WHEN uf_notificacao IN ('11', '12', '13', '14', '15', '16', '17') THEN 'Norte'
        WHEN uf_notificacao IN ('21', '22', '23', '24', '25', '26', '27', '28', '29') THEN 'Nordeste'
        WHEN uf_notificacao IN ('31', '32', '33', '35') THEN 'Sudeste'
        WHEN uf_notificacao IN ('41', '42', '43') THEN 'Sul'
        WHEN uf_notificacao IN ('50', '51', '52', '53') THEN 'Centro-Oeste'
        ELSE 'Região Desconhecida'
    END AS regiao

FROM unidades_base
ORDER BY unidade_saude_key
