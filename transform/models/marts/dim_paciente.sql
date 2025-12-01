{{ config(
    materialized='table',
    unique_key='paciente_key',
) }}

WITH paciente_base AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['id_municipio', 'sexo', 'idade', 'raca']) }} AS paciente_key,

        sexo,
        idade,
        raca,
        gestante,

        id_municipio AS municipio_residencia,
        uf_notificacao AS uf_residencia

    FROM {{ ref('stg_dengue') }}
    WHERE sexo IS NOT NULL
)

SELECT
    paciente_key,
    sexo,
    CASE
        WHEN sexo = 'M' THEN 'Masculino'
        WHEN sexo = 'F' THEN 'Feminino'
        WHEN sexo = 'I' THEN 'Ignorado'
        ELSE 'Não Informado'
    END AS sexo_descricao,

    idade,
    CASE
        WHEN idade < 1 THEN 'Menor de 1 ano'
        WHEN idade BETWEEN 1 AND 12 THEN 'Criança (1-12)'
        WHEN idade BETWEEN 13 AND 17 THEN 'Adolescente (13-17)'
        WHEN idade BETWEEN 18 AND 59 THEN 'Adulto (18-59)'
        WHEN idade >= 60 THEN 'Idoso (60+)'
        ELSE 'Não Informado'
    END AS faixa_etaria,

    raca,
    CASE
        WHEN raca = 1 THEN 'Branca'
        WHEN raca = 2 THEN 'Preta'
        WHEN raca = 3 THEN 'Amarela'
        WHEN raca = 4 THEN 'Parda'
        WHEN raca = 5 THEN 'Indígena'
        WHEN raca = 9 THEN 'Ignorado'
        ELSE 'Não Informado'
    END AS raca_descricao,

    gestante,
    CASE
        WHEN gestante = 1 THEN '1º Trimestre'
        WHEN gestante = 2 THEN '2º Trimestre'
        WHEN gestante = 3 THEN '3º Trimestre'
        WHEN gestante = 4 THEN 'Idade gestacional ignorada'
        WHEN gestante = 5 THEN 'Não'
        WHEN gestante = 6 THEN 'Não se aplica'
        WHEN gestante = 9 THEN 'Ignorado'
        ELSE 'Não Informado'
    END AS gestante_descricao,

    municipio_residencia,
    uf_residencia

FROM paciente_base
ORDER BY paciente_key
