
import requests
import json
import os
import time
import sys
import traceback

# Configuration
MB_URL = "http://metabase:3010"
MB_User_File = "/usr/src/app/scripts/mbuser.json"

# Database Config for Metabase to connect to
DB_HOST = os.getenv('POSTGRES_HOST', 'appdb')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'vicarius_user')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'VicariusT3N48l3')
DB_NAME = "integration_db"

def log(msg):
    print(f"[Metabase Setup] {msg}", flush=True)

def get_mb_creds():
    try:
        with open(MB_User_File, 'r') as f:
            data = json.load(f)
            email = data.get('username') or data.get('email')
            if '@' not in email:
                email = f"{email}@vanalyzer.local"
            return email, data.get('password')
    except Exception as e:
        log(f"Error reading mbuser.json: {e}")
        return "admin@vanalyzer.local", "Admin123456"

EMAIL, PASSWORD = get_mb_creds()

def wait_for_metabase():
    log("Waiting for Metabase to be healthy...")
    for i in range(60): # Increased wait time
        try:
            r = requests.get(f"{MB_URL}/api/health")
            if r.status_code == 200:
                log("Metabase is ready.")
                return True
        except:
            pass
        time.sleep(2)
    return False

def try_login(email, password):
    payload = {"username": email, "password": password}
    try:
        r = requests.post(f"{MB_URL}/api/session", json=payload)
        if r.status_code == 200:
            return r.json()['id']
    except:
        pass
    return None

def get_session():
    # 1. Try Login with Configured Creds
    log(f"Attempting login as {EMAIL}...")
    sid = try_login(EMAIL, PASSWORD)
    if sid:
        log("Login successful.")
        return sid
    
    # 2. Try Login with Fallback Defaults (in case volume persists with old entry)
    fallback_user = "admin@vanalyzer.local"
    fallback_pass = "Admin123456"
    if EMAIL != fallback_user:
        log(f"Login failed. Trying fallback user {fallback_user}...")
        sid = try_login(fallback_user, fallback_pass)
        if sid:
            log("Fallback login successful.")
            return sid

    # 3. Try Setup (only if no user exists at all)
    try:
        r_token = requests.get(f"{MB_URL}/api/session/properties")
        setup_token = r_token.json().get('setup_token')
        
        if setup_token:
            log("Performing fresh Metabase Setup...")
            setup_payload = {
                "token": setup_token,
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": EMAIL,
                    "password": PASSWORD
                },
                "prefs": {
                    "site_name": "vAnalyzer Dashboard",
                    "allow_tracking": False
                }
            }
            r_setup = requests.post(f"{MB_URL}/api/setup", json=setup_payload)
            if r_setup.status_code == 200:
                log("Setup complete. Logging in...")
                return r_setup.json()['id']
            else:
                log(f"Setup failed: {r_setup.text}")
    except Exception as e:
        log(f"Setup Check Error: {e}")

    return None

def add_database(session_id):
    headers = {"X-Metabase-Session": session_id}
    
    # DB Configuration Payload
    db_payload = {
        "name": "V-Analyzer Integration",
        "engine": "postgres",
        "details": {
            "host": DB_HOST,
            "port": 5432,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS,
            "ssl": False # Force disable SSL
        }
    }

    # Check if exists
    try:
        r = requests.get(f"{MB_URL}/api/database", headers=headers)
        if r.status_code == 200:
            dbs = r.json()
            # Handle potential pagination or wrapper (e.g., {'data': [...]})
            if isinstance(dbs, dict) and 'data' in dbs:
                dbs = dbs['data']
            
            if not isinstance(dbs, list):
                log(f"Unexpected DB response format (type {type(dbs)}): {str(dbs)[:200]}")
                # We return None here so we don't assume safe to add. 
                # But we might want to fail-open if we are sure it doesn't exist? 
                # Better to be safe to avoid duplicates.
                return None

            for db in dbs:
                if isinstance(db, dict) and db.get('name') == "V-Analyzer Integration":
                    log("Database 'V-Analyzer Integration' exists. Updating configuration...")
                    # Update existing DB to ensure SSL settings are correct
                    r_upd = requests.put(f"{MB_URL}/api/database/{db['id']}", headers=headers, json=db_payload)
                    if r_upd.status_code == 200:
                        log("Database configuration updated.")
                    else:
                        log(f"Failed to update database: {r_upd.text}")
                    return db['id']
    except Exception as e:
        log(f"Error checking databases: {e}")
        traceback.print_exc()
        return None

    # Add DB
    log("Adding 'V-Analyzer Integration' Database...")
    try:
        r = requests.post(f"{MB_URL}/api/database", json=db_payload, headers=headers)
        if r.status_code != 200:
            log(f"Failed to add Database: {r.text}")
            return None
        return r.json()['id']
    except Exception as e:
        log(f"Error adding database: {e}")
        return None

def create_dashboard_and_cards(session_id, db_id):
    headers = {"X-Metabase-Session": session_id}
    
    # 1. Check/Create Collection
    col_id = None
    try:
        r_cols = requests.get(f"{MB_URL}/api/collection", headers=headers)
        if r_cols.status_code == 200:
            for col in r_cols.json():
                if col['name'] == "vAnalyzer Reports":
                    col_id = col['id']
                    log("Collection 'vAnalyzer Reports' already exists.")
                    break
    except Exception as e:
        log(f"Error fetching collections: {e}")
    
    if not col_id:
        r = requests.post(f"{MB_URL}/api/collection", headers=headers, json={
            "name": "vAnalyzer Reports",
            "color": "#509EE3"
        })
        if r.status_code == 200:
            col_id = r.json()['id']
            log("Created Collection 'vAnalyzer Reports'.")
        else:
            log(f"Failed to create collection: {r.text}")
            return

    # 2. Check/Create Dashboard
    dash_id = None
    try:
        r_dash = requests.get(f"{MB_URL}/api/dashboard", headers=headers)
        if r_dash.status_code == 200:
            for dash in r_dash.json():
                if dash['name'] == "Main Vulnerability Dashboard" and dash['collection_id'] == col_id:
                    dash_id = dash['id']
                    log("Dashboard already exists. Skipping creation.")
                    return
    except Exception as e:
        log(f"Error fetching dashboards: {e}")

    if not dash_id:
        r = requests.post(f"{MB_URL}/api/dashboard", headers=headers, json={
            "name": "Main Vulnerability Dashboard",
            "collection_id": col_id,
            "description": "Unified view of Tenable and Vicarius data"
        })
        if r.status_code == 200:
            dash_id = r.json()['id']
            log("Created Dashboard 'Main Vulnerability Dashboard'.")
        else:
            log(f"Failed to create dashboard: {r.text}")
            return

    # Helper to create SQL Card
    def create_card(name, sql, viz_type="table"):
        card = {
            "name": name,
            "collection_id": col_id,
            "display": viz_type,
            "visualization_settings": {},  # New Requirement for Metabase v0.50+
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": sql}
            }
        }
        r = requests.post(f"{MB_URL}/api/card", headers=headers, json=card)
        if r.status_code == 200:
            return r.json()['id']
        else:
            log(f"Failed to create card '{name}': {r.text}")
            return None

    def add_to_dash(card_id, row, col, width=12, height=6):
        requests.post(f"{MB_URL}/api/dashboard/{dash_id}/cards", headers=headers, json={
            "cardId": card_id,
            "row": row, "col": col, "sizeX": width, "sizeY": height
        })

    log("Creating Questions and adding to Dashboard...")
    
    c1 = create_card("Security Gap Analysis", "SELECT * FROM \"View_Security_Gap_Analysis\"", "table")
    if c1: add_to_dash(c1, 0, 0, 12, 8)

    c2 = create_card("Mitigation Evolution", "SELECT * FROM \"View_Mitigation_Evolution\"", "line")
    if c2: add_to_dash(c2, 8, 0, 12, 6)

    c3 = create_card("Pending Reboots", "SELECT * FROM \"View_Daily_Reboots\"", "table")
    if c3: add_to_dash(c3, 14, 0, 6, 6)
    
    c4 = create_card("Non-CVE Incidents", "SELECT * FROM \"View_No_CVE_Incidents\"", "bar")
    if c4: add_to_dash(c4, 14, 6, 6, 6)

    log(f"Dashboard created successfully! Link: {MB_URL}/dashboard/{dash_id}")

def run():
    try:
        if not wait_for_metabase():
            log("Metabase not available.")
            return

        session_id = get_session()
        if not session_id:
            log("Could not login or setup Metabase. Please check logs and credentials.")
            return

        db_id = add_database(session_id)
        if db_id:
            create_dashboard_and_cards(session_id, db_id)
        else:
            log("Skipping dashboard creation because DB addition failed.")
            
    except Exception as e:
        log("CRITICAL ERROR IN SETUP SCRIPT:")
        traceback.print_exc()

if __name__ == "__main__":
    run()
