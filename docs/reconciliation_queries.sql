-- Severity distribution
SELECT level, COUNT(*) AS event_count
FROM log_events GROUP BY level ORDER BY level;

-- Error rate by service (ERROR and FATAL)
SELECT service,
       COUNT(*) AS total_events,
       SUM(CASE WHEN level IN ('ERROR','FATAL') THEN 1 ELSE 0 END) AS error_events,
       ROUND(100.0 * SUM(CASE WHEN level IN ('ERROR','FATAL') THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct
FROM log_events GROUP BY service;

-- Latency statistics by service (SQLite AVG/MIN/MAX; P50/P95 are in log_summary.csv)
SELECT service, AVG(latency_ms), MIN(latency_ms), MAX(latency_ms)
FROM log_events WHERE latency_ms IS NOT NULL GROUP BY service;

-- Run-level operational history
SELECT * FROM pipeline_runs ORDER BY start_time DESC;


-- Referential-integrity check: should return zero rows.
SELECT e.record_id, e.service_id
FROM log_events AS e
LEFT JOIN service_catalog AS s ON s.service_id = e.service_id
WHERE s.service_id IS NULL;

-- Registered services and accepted event counts (including zero-event services).
SELECT s.service_id, s.service_name, COUNT(e.record_id) AS event_count
FROM service_catalog AS s
LEFT JOIN log_events AS e ON e.service_id = s.service_id
GROUP BY s.service_id, s.service_name
ORDER BY s.service_name;
