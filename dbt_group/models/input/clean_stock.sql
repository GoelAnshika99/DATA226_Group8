SELECT
    SYMBOL,
    DATE,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    VOLUME
FROM {{ source('raw', 'NVDA_STOCK') }}
WHERE close > 0
AND volume > 0
