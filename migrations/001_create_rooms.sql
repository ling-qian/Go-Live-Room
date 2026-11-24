CREATE TABLE IF NOT EXISTS rooms (
    id            VARCHAR(36)  NOT NULL PRIMARY KEY,
    title         VARCHAR(160) NOT NULL,
    host_id       VARCHAR(128) NOT NULL,
    status        VARCHAR(16)  NOT NULL,
    max_members   INT          NOT NULL,
    created_at    DATETIME(3)  NOT NULL,
    INDEX idx_rooms_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS room_members (
    room_id       VARCHAR(36)  NOT NULL,
    user_id       VARCHAR(128) NOT NULL,
    role          VARCHAR(16)  NOT NULL,
    joined_at     DATETIME(3)  NOT NULL,
    PRIMARY KEY (room_id, user_id),
    INDEX idx_room_members_user (user_id),
    CONSTRAINT fk_room_members_room FOREIGN KEY (room_id)
        REFERENCES rooms(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
