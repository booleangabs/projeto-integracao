{{ config(
    materialized='table',
    unique_key='localidade_key',
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['id_municipio', 'uf_notificacao']) }} AS localidade_key,
    
    id_municipio,
    uf_notificacao,
    
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
    END AS nome_estado
    
FROM (
    SELECT DISTINCT
        id_municipio,
        uf_notificacao
    FROM {{ ref('stg_dengue') }}
    WHERE id_municipio IS NOT NULL AND uf_notificacao IS NOT NULL
) AS unique_locations
