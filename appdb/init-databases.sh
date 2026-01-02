#!/bin/bash
set -e

echo "🔧 Initializing Data Lakehouse Architecture..."

# Default user
ADMIN_USER="${POSTGRES_USER:-postgres}"

# Function to create database if not exists
create_db() {
    local dbname=$1
    echo "Checking database $dbname..."
    psql -v ON_ERROR_STOP=0 --username "$ADMIN_USER" --dbname "postgres" <<-EOSQL
        SELECT 'CREATE DATABASE $dbname'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$dbname')\gexec
EOSQL
}

# 1. Ensure 'vicarius_user' exists (if not created by Docker default)
# We do this because the app uses 'vicarius_user' but sometimes Docker init uses 'postgres'
echo "Ensuring user 'vicarius_user' exists..."
psql -v ON_ERROR_STOP=0 --username "$ADMIN_USER" --dbname "postgres" <<-EOSQL
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'vicarius_user') THEN
            CREATE ROLE vicarius_user WITH LOGIN PASSWORD 'VicariusT3N48l3' SUPERUSER;
        ELSE
            ALTER ROLE vicarius_user WITH PASSWORD 'VicariusT3N48l3' SUPERUSER;
        END IF;
    END
    \$\$;
EOSQL

# 1b. Create Databases
create_db "tenable_source_db"
create_db "vicarius_source_db"
create_db "integration_db"
create_db "metabase" # For Metabase internal DB

# 2. Configure Tenable Source DB
echo "Configuring tenable_source_db..."
psql -v ON_ERROR_STOP=1 --username "$ADMIN_USER" --dbname "tenable_source_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS "Tenable_Assets_Raw" (
        asset_uuid TEXT PRIMARY KEY,
        hostname TEXT,
        fqdn TEXT,
        os TEXT,
        tags TEXT,
        last_seen TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "Tenable_Vulns_Raw" (
        plugin_id TEXT,
        cve TEXT,
        risk TEXT,
        status TEXT,
        asset_uuid_fk TEXT REFERENCES "Tenable_Assets_Raw"(asset_uuid),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    -- Index for faster joins
    CREATE INDEX IF NOT EXISTS idx_tenable_vulns_asset ON "Tenable_Vulns_Raw"(asset_uuid_fk);
EOSQL

# 3. Configure Vicarius Source DB
echo "Configuring vicarius_source_db..."
psql -v ON_ERROR_STOP=1 --username "$ADMIN_USER" --dbname "vicarius_source_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS "Vicarius_Endpoints_Raw" (
        asset_id TEXT PRIMARY KEY,
        hostname TEXT,
        group_name TEXT,
        os TEXT,
        agent_version TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "Vicarius_Incidents_Raw" (
        incident_id TEXT PRIMARY KEY,
        cve TEXT,
        severity TEXT,
        status TEXT,
        asset_id_fk TEXT REFERENCES "Vicarius_Endpoints_Raw"(asset_id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "Vicarius_Patch_Stats" (
        stat_id SERIAL PRIMARY KEY,
        total_vulns INTEGER,
        total_patches INTEGER,
        snapshot_date DATE DEFAULT CURRENT_DATE
    );

    CREATE TABLE IF NOT EXISTS "Vicarius_Event_Task" (
        task_id SERIAL PRIMARY KEY,
        event_tag TEXT, -- e.g., 'Pendiente Reinicio'
        asset_id_fk TEXT,
        request_date TIMESTAMP
    );
EOSQL

# 4. Configure Integration DB (Data Warehouse)
echo "Configuring integration_db..."
psql -v ON_ERROR_STOP=1 --username "$ADMIN_USER" --dbname "integration_db" <<-EOSQL
    CREATE TABLE IF NOT EXISTS "Integracion_Dim_Assets" (
        unified_asset_id SERIAL PRIMARY KEY,
        hostname_normalized TEXT UNIQUE,
        original_tenable_uuid TEXT,
        original_vicarius_id TEXT,
        group_assignment TEXT, -- UNICON, UNACEM, etc.
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "Integracion_Fact_Unified_Vulns" (
        fact_id SERIAL PRIMARY KEY,
        hostname TEXT,
        cve_id TEXT,
        severity TEXT,
        source_detection TEXT, -- AMBAS, TENABLE_ONLY, VICARIUS_ONLY
        status TEXT,
        detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "Integracion_Log_Mitigation_History" (
        log_id SERIAL PRIMARY KEY,
        snapshot_date DATE,
        group_name TEXT,
        total_detected INTEGER,
        total_resolved INTEGER,
        mitigation_percentage NUMERIC(5,2)
    );

    CREATE TABLE IF NOT EXISTS "Integracion_Daily_Reboot_Audit" (
        audit_id SERIAL PRIMARY KEY,
        hostname TEXT,
        pending_reboot_since TIMESTAMP,
        days_pending INTEGER
    );
    
    -- Views for Metabase (Pre-creating Questions)

    -- Dashboard 1: Security Gap Analysis
    CREATE OR REPLACE VIEW "View_Security_Gap_Analysis" AS
    SELECT hostname, cve_id, severity, source_detection 
    FROM "Integracion_Fact_Unified_Vulns";

    -- Dashboard 2: Mitigation Evolution (Time Series)
    CREATE OR REPLACE VIEW "View_Mitigation_Evolution" AS
    SELECT snapshot_date, group_name, mitigation_percentage 
    FROM "Integracion_Log_Mitigation_History"
    ORDER BY snapshot_date ASC;

    -- Dashboard 3: No-CVE Configuration
    -- Note: Data will need to be populated from Vicarius non-CVE incidents 
    -- We assume a table or filter for this. For now creating a placeholder view.
    CREATE OR REPLACE VIEW "View_No_CVE_Incidents" AS
    SELECT severity, COUNT(*) as count 
    FROM "Integracion_Fact_Unified_Vulns" 
    WHERE cve_id IS NULL OR cve_id = 'N/A'
    GROUP BY severity;

    -- Dashboard 4: Daily Operations (Reboots)
    CREATE OR REPLACE VIEW "View_Daily_Reboots" AS
    SELECT hostname, pending_reboot_since, days_pending
    FROM "Integracion_Daily_Reboot_Audit";

EOSQL

echo "✅ Data Lakehouse Architecture initialized successfully."
