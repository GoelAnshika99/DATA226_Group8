WITH base AS (
    SELECT *
    FROM {{ ref('clean_stock') }}
),

metrics AS (
    SELECT
        symbol,
        date,
        close,

        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,

        -- Short moving averages for MACD
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ema_fast,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS ema_slow,

        -- Bollinger mid (SMA10) and stddev
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_10,
        STDDEV(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS stddev_10
    FROM base
),

gains_losses AS (
    SELECT
        *,
        close - prev_close AS momentum,
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0 END AS gain,
        CASE WHEN close < prev_close THEN ABS(close - prev_close) ELSE 0 END AS loss
    FROM metrics
),

rsi_calc AS (
    SELECT
        *,
        -- RSI-5 calculation
        100 - (100 / (1 + (
            AVG(gain) OVER (
                PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                AVG(loss) OVER (
                    PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ), 0)
        ))) AS rsi_5
    FROM gains_losses
),

signals AS (
    SELECT
        *,
        -- RSI classification
        CASE 
            WHEN rsi_5 > 70 THEN 'Overbought'
            WHEN rsi_5 < 30 THEN 'Oversold'
            ELSE 'Neutral'
        END AS rsi_class,

        -- MACD value
        ema_fast - ema_slow AS macd,

        -- Bollinger Bands
        sma_10 + 2 * stddev_10 AS bollinger_upper,
        sma_10 - 2 * stddev_10 AS bollinger_lower,

        -- Individual indicator-based signals
        CASE 
            WHEN rsi_5 < 30 THEN 'Buy'
            WHEN rsi_5 > 70 THEN 'Sell'
            ELSE 'Hold'
        END AS rsi_signal,

        CASE 
            WHEN ema_fast > ema_slow AND prev_close < sma_10 THEN 'Buy'
            WHEN ema_fast < ema_slow AND prev_close > sma_10 THEN 'Sell'
            ELSE 'Hold'
        END AS macd_signal,

        CASE
            WHEN close > sma_10 + 2 * stddev_10 THEN 'Sell'
            WHEN close < sma_10 - 2 * stddev_10 THEN 'Buy'
            ELSE 'Hold'
        END AS bollinger_signal
    FROM rsi_calc
),

final AS (
    SELECT
        *,
        -- Final signal based on alignment of multiple indicators
        CASE
            WHEN rsi_signal = 'Buy' AND macd_signal = 'Buy' AND bollinger_signal = 'Buy' THEN 'Strong Buy'
            WHEN rsi_signal = 'Sell' AND macd_signal = 'Sell' AND bollinger_signal = 'Sell' THEN 'Strong Sell'
            WHEN rsi_signal = 'Buy' OR macd_signal = 'Buy' OR bollinger_signal = 'Buy' THEN 'Buy'
            WHEN rsi_signal = 'Sell' OR macd_signal = 'Sell' OR bollinger_signal = 'Sell' THEN 'Sell'
            ELSE 'Hold'
        END AS overall_signal
    FROM signals
)

SELECT
    symbol,
    date,
    close,
    momentum,
    
    -- Indicators
    rsi_5,
    rsi_class,
    macd,
    sma_10 AS bollinger_mid,
    bollinger_upper,
    bollinger_lower,

    -- Individual signals
    rsi_signal,
    macd_signal,
    bollinger_signal,

    -- Final combined signal
    overall_signal
FROM final
ORDER BY symbol, date