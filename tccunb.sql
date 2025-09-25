CREATE TABLE user_actions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    respondida_em DATETIME NOT NULL,
    tipo_acao VARCHAR(50) NOT NULL,
    item_id VARCHAR(50) NOT NULL,
    fonte VARCHAR(50) NOT NULL,
    resposta_usuario VARCHAR(255),
    plataforma VARCHAR(50) NOT NULL,
    user_id INT NOT NULL
);
