package com.example.couponsystem.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class LockRepository2 {

    private final DataSource dataSource;

    public void executeWithLocK(String key, int timeout, Runnable action) {
        try (Connection conn = dataSource.getConnection()) {
            try {
                getLock(conn, key, timeout);
                action.run();
            } finally {
                releaseLock(conn, key);
            }
        } catch (SQLException e) {
            throw new RuntimeException("락 처리 중 오류", e);
        }
    }

    private void getLock(Connection conn, String key, int timeout) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT GET_LOCK(?, ?)")) {
            ps.setString(1, key);
            ps.setInt(2, timeout);
            ps.executeQuery();
        }
    }

    private void releaseLock(Connection conn, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT RELEASE_LOCK(?)")) {
            ps.setString(1, key);
            ps.executeQuery();
        }
    }
}
