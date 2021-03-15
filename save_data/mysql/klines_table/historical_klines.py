def create_historical_klines_table(symble, time_interval):

    return f"""
    CREATE TABLE IF NOT EXISTS binance_dev.{symble}_{time_interval}(
    open_time VARCHAR(40) NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    close_time VARCHAR(40) NOT NULL,
    quote_asset DOUBLE NOT NULL COMMENT 'Quote asset volume',
    trades INT NOT NULL COMMENT 'Number of trades',
    taker_buy_base_asset_volume DOUBLE NOT NULL COMMENT 'Taker buy base asset volume',
    taker_quote DOUBLE NOT NULL COMMENT 'Taker buy quote asset volume',
    can_be_ignored DOUBLE NOT NULL COMMENT 'Can be ignored ',
    PRIMARY KEY (open_time)
    ); 
    """
