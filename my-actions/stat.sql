-- SQLite
SELECT
    priority,
    COUNT(*) AS activations_cnt,
    SUM(started_at - created_at) / COUNT(*) AS average_delay_in_ms
FROM activations
GROUP BY priority
ORDER BY average_delay_in_ms;
