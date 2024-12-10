SELECT TYPEID, TYPENAME
FROM {{ source('market_data', 'invtypes') }}