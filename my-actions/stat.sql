-- SQLite
SELECT priority, COUNT(*), SUM(started_at - created_at * 1000) / COUNT(*) FROM activations GROUP BY priority;
