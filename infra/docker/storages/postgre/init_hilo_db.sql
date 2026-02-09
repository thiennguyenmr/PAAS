CREATE TABLE IF NOT EXISTS hilo_game_data (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    game_info JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert dummy data for manual testing
INSERT INTO hilo_game_data (user_id, game_info) VALUES
('user_1', '{"game_id": "g1", "score": 100, "status": "win", "moves": ["up", "down", "up"]}'),
('user_2', '{"game_id": "g2", "score": 50, "status": "loss", "moves": ["down", "down"]}');
