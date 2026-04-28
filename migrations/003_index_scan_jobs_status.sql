-- Migration 003: Replace composite index with lightweight status index
-- Anterior: idx_scan_jobs_status_jobtype (causava INSERT lento)
-- Novo: idx_scan_jobs_status (status(20)) — leve, evita full table scan

DROP INDEX IF EXISTS idx_scan_jobs_status_jobtype ON scan_jobs;
CREATE INDEX idx_scan_jobs_status ON scan_jobs(status(20));
