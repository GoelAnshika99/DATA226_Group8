{% snapshot snapshot_stock_analytics %}

{{
  config(
    target_schema='snapshots',
    unique_key='symbol || date',
    strategy='check',
    check_cols=['overall_signal', 'macd_signal', 'rsi_signal', 'bollinger_signal']
  )
}}

SELECT * FROM {{ ref('stock_analytics') }}

{% endsnapshot %}