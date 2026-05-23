-- Bảng lưu trữ thông tin người dùng
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id VARCHAR(50) UNIQUE NOT NULL,
    username VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu cấu hình cấu hình cảnh báo của từng user
CREATE TABLE IF NOT EXISTS configs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    threshold_usd NUMERIC(15, 2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu lịch sử các cảnh báo đã gửi thành công
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(15, 2),
    volume NUMERIC(15, 2),
    total_usd NUMERIC(15, 2),
    is_buyer_maker BOOLEAN,
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TABLE OHLCV
CREATE TABLE IF NOT EXISTS ohlcv_candles(
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- 5m, 10m
    timestamp TIMESTAMP NOT NULL,  -- time for candles
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(18, 8) NOT NULL,
    cnt INTEGER NOT NULL,
    buyer BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE ohlcv_candles 
ADD CONSTRAINT unique_candle UNIQUE (symbol, timeframe, timestamp); -- update candles easy