
import os
import time
import requests
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DB_HOST = os.getenv('POSTGRES_HOST', 'appdb')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'password')

# Connection Strings
CONN_STR_TENABLE = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/tenable_source_db"
CONN_STR_VICARIUS = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/vicarius_source_db"
CONN_STR_INTEGRATION = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/integration_db"

def get_engine(conn_str: str) -> Engine:
    return sa.create_engine(conn_str)

def normalize_hostname(hostname):
    if not hostname or pd.isna(hostname):
        return "UNKNOWN"
    return str(hostname).upper().split('.')[0]

def determine_group(hostname):
    hostname = str(hostname).upper()
    if 'UNICON' in hostname: return 'UNICON'
    if 'UNACEM' in hostname: return 'UNACEM'
    if 'CONCREMAX' in hostname: return 'CONCREMAX'
    if 'ARPL' in hostname: return 'ARPL'
    return 'OTROS'

class TenableIngestor:
    def __init__(self):
        self.api_key = os.getenv('TENABLE_API_KEY')
        self.secret_key = os.getenv('TENABLE_SECRET_KEY')
        self.base_url = "https://cloud.tenable.com"
        self.engine = get_engine(CONN_STR_TENABLE)
        self.headers = {
            "X-ApiKeys": f"accessKey={self.api_key};secretKey={self.secret_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def fetch_and_load(self):
        if not self.api_key or not self.secret_key:
            logger.warning("Skipping Tenable Ingestion: Missing API Keys")
            return

        logger.info("Fetching Assets from Tenable...")
        assets = self._get_assets()
        if assets:
            df_assets = pd.DataFrame(assets)
            # Rename match DB schema
            df_assets = df_assets.rename(columns={'id': 'asset_uuid', 'ipv4': 'ip_address'})
            # Ensure columns exist
            for col in ['fqdn', 'hostname', 'operating_system', 'last_seen']:
                if col not in df_assets.columns: df_assets[col] = None
            
            # Load Assets
            # FIX: Drop dependent child table first to allow parent 'replace'
            with self.engine.connect() as conn:
                try:
                    conn.execute(sa.text('DROP TABLE IF EXISTS "Tenable_Vulns_Raw" CASCADE'))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Could not drop child table: {e}")

            df_assets[['asset_uuid', 'hostname', 'fqdn', 'operating_system', 'last_seen']].to_sql(
                'Tenable_Assets_Raw', self.engine, if_exists='replace', index=False
            )
            
            logger.info(f"Loaded {len(df_assets)} Tenable Assets.")
            
            # Fetch Vulns (MVP: Loop assets)
            logger.info(f"Fetching Vulnerabilities for {len(df_assets)} assets...")
            all_vulns = []
            total_assets = len(df_assets)
            for i, row in df_assets.iterrows():
                if (i + 1) % 50 == 0:
                    logger.info(f"  > Progress: Processed {i + 1}/{total_assets} assets...")
                vulns = self._get_asset_vulns(row['asset_uuid'])
                all_vulns.extend(vulns)
            
            if all_vulns:
                df_vulns = pd.DataFrame(all_vulns)
                df_vulns.to_sql('Tenable_Vulns_Raw', self.engine, if_exists='replace', index=False)
                logger.info(f"Loaded {len(df_vulns)} Tenable Vulnerabilities.")

    def _get_assets(self):
        url = f"{self.base_url}/workbenches/assets"
        params = {"date_range": 30}
        try:
            resp = requests.get(url, headers=self.headers, params=params)
            if resp.status_code == 200:
                assets = resp.json().get('assets', [])
                # Normalize parsed structure
                parsed = []
                for a in assets:
                    parsed.append({
                        'id': a.get('id'),
                        'hostname': a.get('hostname', [None])[0] if a.get('hostname') else None,
                        'fqdn': a.get('fqdn', [None])[0] if a.get('fqdn') else None,
                        'operating_system': a.get('operating_system', [None])[0] if a.get('operating_system') else None,
                        'last_seen': a.get('last_seen')
                    })
                return parsed
            return []
        except Exception as e:
            logger.error(f"Tenable Asset Fetch Error: {e}")
            return []

    def _get_asset_vulns(self, asset_uuid):
        url = f"{self.base_url}/workbenches/assets/{asset_uuid}/vulnerabilities"
        try:
            time.sleep(0.1) # Optimized Rate limit
            resp = requests.get(url, headers=self.headers)
            if resp.status_code == 200:
                vulns = resp.json().get('vulnerabilities', [])
                parsed = []
                for v in vulns:
                    parsed.append({
                        'plugin_id': str(v.get('plugin_id')),
                        'cve': v.get('cve', 'N/A'), # Raw CVE string/list
                        'risk': str(v.get('severity_default_id')),
                        'status': v.get('vulnerability_state'),
                        'asset_uuid_fk': asset_uuid
                    })
                return parsed
            return []
        except:
            return []

class VicariusIngestor:
    def __init__(self):
        self.api_key = os.getenv('VICARIUS_API_KEY')
        self.dashboard_id = os.getenv('VICARIUS_DASHBOARD_ID')
        self.base_url = f"https://{self.dashboard_id}.vicarius.cloud/vicarius-external-data-api" if self.dashboard_id else None
        self.engine = get_engine(CONN_STR_VICARIUS)
        self.headers = {
            'Accept': 'application/json',
            'Vicarius-Token': self.api_key,
        }

    def fetch_and_load(self):
        if not self.api_key or not self.base_url:
            logger.warning("Skipping Vicarius Ingestion: Missing API Keys")
            return

        logger.info("Fetching Endpoints from Vicarius...")
        endpoints = self._get_endpoints()
        if endpoints:
            df_eps = pd.DataFrame(endpoints)
            # Rename/Map columns to `Vicarius_Endpoints_Raw`
            # asset_id, hostname, group_name, os, agent_version
            df_eps = df_eps.rename(columns={
                'endpointId': 'asset_id',
                'endpointName': 'hostname',
                'operatingSystemName': 'os',
                'agentVersion': 'agent_version'
            })
            df_eps['group_name'] = 'All Assets' # MVP, implement group fetching later
            
            df_eps[['asset_id', 'hostname', 'group_name', 'os', 'agent_version']].to_sql(
                'Vicarius_Endpoints_Raw', self.engine, if_exists='replace', index=False
            )
            logger.info(f"Loaded {len(df_eps)} Vicarius Endpoints.")

        logger.info("Fetching Incidents from Vicarius (Detected/Mitigated)...")
        incidents = self._get_incidents()
        if incidents:
            df_inc = pd.DataFrame(incidents)
            # Map: incident_id (generated?), cve, severity, status, asset_id_fk
            # We construct a unique ID or use generated one 
            df_inc['incident_id'] = df_inc.index.astype(str) # MVP Simple ID
            df_inc = df_inc.rename(columns={
                'assetId': 'asset_id_fk',
                'cvss': 'severity',
                'eventType': 'status' 
            })
            df_inc[['incident_id', 'cve', 'severity', 'status', 'asset_id_fk']].to_sql(
                'Vicarius_Incidents_Raw', self.engine, if_exists='replace', index=False
            )
            
            logger.info(f"Loaded {len(df_inc)} Vicarius Incidents.")

    def _get_endpoints(self):
        # Simplified Search
        endpoint_list = []
        try:
            params = {'from': 0, 'size': 1000}
            resp = requests.get(f"{self.base_url}/endpoint/search", headers=self.headers, params=params)
            if resp.status_code == 200:
                data = resp.json().get('serverResponseObject', [])
                for i in data:
                    endpoint_list.append({
                        'endpointId': str(i.get('endpointId')),
                        'endpointName': i.get('endpointName'),
                        'operatingSystemName': i.get('endpointOperatingSystem', {}).get('operatingSystemName'),
                        'agentVersion': i.get('endpointVersion', {}).get('versionName')
                    })
        except Exception as e:
            logger.error(f"Vicarius Endpoint Fetch Error: {e}")
        return endpoint_list

    def _get_incidents(self):
        incident_list = []
        try:
            # Fetch last 30 days or so
            # q: incidentEventIncidentEventType=in=(MitigatedVulnerability,DetectedVulnerability)
            params = {
                'from': 0, 
                'size': 1000,
                'q': 'incidentEventIncidentEventType=in=(MitigatedVulnerability,DetectedVulnerability)'
            }
            resp = requests.get(f"{self.base_url}/incidentEvent/filter", headers=self.headers, params=params)
            if resp.status_code == 200:
                data = resp.json().get('serverResponseObject', [])
                for i in data:
                    incident_list.append({
                        'assetId': str(i.get('incidentEventEndpoint', {}).get('endpointId')),
                        'cve': i.get('incidentEventVulnerability', {}).get('vulnerabilityExternalReference', {}).get('externalReferenceExternalId'),
                        'cvss': i.get('incidentEventVulnerability', {}).get('vulnerabilitySensitivityLevel', {}).get('sensitivityLevelName'),
                        'eventType': i.get('incidentEventIncidentEventType')
                    })
        except Exception as e:
            logger.error(f"Vicarius Incident Fetch Error: {e}")
        return incident_list

class DataLakehouseETL:
    def __init__(self):
        self.engine_tenable = get_engine(CONN_STR_TENABLE)
        self.engine_vicarius = get_engine(CONN_STR_VICARIUS)
        self.engine_integration = get_engine(CONN_STR_INTEGRATION)

    def run_full_etl(self):
        logger.info("Starting Full ETL Process...")
        
        # 1. Ingest
        TenableIngestor().fetch_and_load()
        VicariusIngestor().fetch_and_load()
        
        # 2. Integrate
        self.process_integration_layer()
        logger.info("ETL Process Completed.")

    def process_integration_layer(self):
        logger.info("Processing Integration Layer...")

        # --- Step 1: Extract from Sources ---
        try:
            df_tenable = pd.read_sql("SELECT * FROM \"Tenable_Vulns_Raw\" v JOIN \"Tenable_Assets_Raw\" a ON v.asset_uuid_fk = a.asset_uuid", self.engine_tenable)
        except:
            df_tenable = pd.DataFrame()
        
        try:
            df_vicarius = pd.read_sql("SELECT * FROM \"Vicarius_Incidents_Raw\" i JOIN \"Vicarius_Endpoints_Raw\" e ON i.asset_id_fk = e.asset_id", self.engine_vicarius)
        except:
            df_vicarius = pd.DataFrame()

        # --- Step 2: Transform & Unify ---
        
        if not df_tenable.empty and 'cve' in df_tenable.columns: 
             # Tenable often provides CVEs as list/string? If string "CVE-1, CVE-2", split.
             # Assuming simple string for MVP, or robust split
             df_tenable['cve'] = df_tenable['cve'].astype(str)
             # Basic handling for comma separated
             df_tenable = df_tenable.assign(cve=df_tenable['cve'].str.split(',')).explode('cve')
             df_tenable['cve'] = df_tenable['cve'].str.strip()
        
        # Normalize
        if not df_tenable.empty:
            df_tenable['hostname_norm'] = df_tenable['hostname'].apply(normalize_hostname)
        else:
            df_tenable = pd.DataFrame(columns=['hostname_norm', 'cve', 'risk', 'status'])
        
        if not df_vicarius.empty:
            df_vicarius['hostname_norm'] = df_vicarius['hostname'].apply(normalize_hostname)
        else:
            df_vicarius = pd.DataFrame(columns=['hostname_norm', 'cve', 'severity', 'status'])

        # Select key columns
        t_cols = df_tenable[['hostname_norm', 'cve', 'risk', 'status']]
        v_cols = df_vicarius[['hostname_norm', 'cve', 'severity', 'status']]

        t_cols = t_cols.rename(columns={'risk': 'severity_tenable', 'status': 'status_tenable'})
        v_cols = v_cols.rename(columns={'severity': 'severity_vicarius', 'status': 'status_vicarius'})

        # FULL OUTER JOIN
        merged = pd.merge(
            t_cols, 
            v_cols, 
            on=['hostname_norm', 'cve'], 
            how='outer', 
            indicator=True
        )

        def get_source_detection(row):
            if row['_merge'] == 'both': return 'AMBAS'
            if row['_merge'] == 'left_only': return 'TENABLE_ONLY'
            if row['_merge'] == 'right_only': return 'VICARIUS_ONLY'
            return 'UNKNOWN'

        merged['source_detection'] = merged.apply(get_source_detection, axis=1)

        final_df = merged.copy()
        final_df['severity'] = final_df['severity_tenable'].combine_first(final_df['severity_vicarius'])
        final_df['status'] = final_df['status_tenable'].combine_first(final_df['status_vicarius'])
        final_df = final_df.rename(columns={'hostname_norm': 'hostname', 'cve': 'cve_id'})
        final_df['group_name'] = final_df['hostname'].apply(determine_group)

        final_df = final_df[['hostname', 'cve_id', 'severity', 'source_detection', 'status', 'group_name']]
        
        logger.info(f"Loading {len(final_df)} rows to Integration_Fact_Unified_Vulns...")
        final_df.to_sql('Integracion_Fact_Unified_Vulns', self.engine_integration, if_exists='replace', index=False)
        
        dim_assets = final_df[['hostname', 'group_name']].drop_duplicates()
        dim_assets.to_sql('Integracion_Dim_Assets', self.engine_integration, if_exists='replace', index=False)

        # Snapshot Log
        def count_resolved(series):
            # Safe count of resolved items
            return len([s for s in series if str(s).lower() in ['mitigated', 'fixed', 'patched', 'resolved']])

        stats = final_df.groupby('group_name').apply(
            lambda x: pd.Series({
                'total_detected': len(x),
                'total_resolved': count_resolved(x['status']),
            })
        ).reset_index()
        
        if 'level_1' in stats.columns: stats = stats.drop(columns=['level_1'])
        
        stats['mitigation_percentage'] = (stats['total_resolved'] / stats['total_detected']) * 100
        stats['snapshot_date'] = datetime.now().date()
        stats.to_sql('Integracion_Log_Mitigation_History', self.engine_integration, if_exists='append', index=False)
        
        logger.info("Integration Layer Update Complete.")

if __name__ == "__main__":
    etl = DataLakehouseETL()
    etl.run_full_etl()
