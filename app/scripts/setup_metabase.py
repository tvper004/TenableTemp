
import requests
import json
import os
import time

# Configuration
MB_URL = "http://metabase:3000"
MB_User_File = "/usr/src/app/scripts/mbuser.json"

# Database Config for Metabase to connect to
DB_HOST = os.getenv('POSTGRES_HOST', 'appdb')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'VicariusT3N48l3')
DB_NAME = "integration_db"

def get_mb_creds():
    try:
        with open(MB_User_File, 'r') as f:
            data = json.load(f)
            # Map json keys to what we need
            email = data.get('username') or data.get('email')
            # If username doesn't look like email, append fake domain for setup requirements if needed, 
            # but usually setup requires email. 'mbbackup' might not be email.
            # Let's assume we use a standard admin email for setup if not email.
            if '@' not in email:
                email = f"{email}@vanalyzer.local"
            
            return email, data.get('password')
    except Exception as e:
        print(f"Error reading mbuser.json: {e}")
        return "admin@vanalyzer.local", "Admin123456" # Fallback

EMAIL, PASSWORD = get_mb_creds()

def wait_for_metabase():
    print("Waiting for Metabase to be healthy...")
    for i in range(30):
        try:
            r = requests.get(f"{MB_URL}/api/health")
            if r.status_code == 200:
                print("Metabase is ready.")
                return True
        except:
            pass
        time.sleep(2)
    return False

def get_session():
    # 1. Try Login
    payload = {"username": EMAIL, "password": PASSWORD}
    r = requests.post(f"{MB_URL}/api/session", json=payload)
    if r.status_code == 200:
        return r.json()['id']
    
    # 2. If login fails, maybe fresh install? Try Setup
    # Metabase Setup Token
    try:
        r_token = requests.get(f"{MB_URL}/api/session/properties")
        setup_token = r_token.json().get('setup_token')
        
        if setup_token:
            print("Performing fresh Metabase Setup...")
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
                print("Setup complete. Logging in...")
                return r_setup.json()['id'] # Typically returns session
            else:
                print(f"Setup failed: {r_setup.text}")
    except Exception as e:
        print(f"Setup Check Error: {e}")

    return None

def add_database(session_id):
    headers = {"X-Metabase-Session": session_id}
    
    # Check if exists
    r = requests.get(f"{MB_URL}/api/database", headers=headers)
    dbs = r.json()
    for db in dbs:
        if db['name'] == "V-Analyzer Integration":
            print("Database 'V-Analyzer Integration' already exists.")
            return db['id']

    # Add DB
    print("Adding 'V-Analyzer Integration' Database...")
    payload = {
        "name": "V-Analyzer Integration",
        "engine": "postgres",
        "details": {
            "host": DB_HOST,
            "port": 5432,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS,
            "ssl": False
        }
    }
    r = requests.post(f"{MB_URL}/api/database", json=payload, headers=headers)
    if r.status_code != 200:
        print(f"Failed to add Database: {r.text}")
        return None
    return r.json()['id']

def create_dashboard_and_cards(session_id, db_id):
    headers = {"X-Metabase-Session": session_id}
    
    # 1. Check/Create Collection
    col_id = None
    r_cols = requests.get(f"{MB_URL}/api/collection", headers=headers)
    if r_cols.status_code == 200:
        for col in r_cols.json():
            if col['name'] == "vAnalyzer Reports":
                col_id = col['id']
                print("Collection 'vAnalyzer Reports' already exists.")
                break
    
    if not col_id:
        r = requests.post(f"{MB_URL}/api/collection", headers=headers, json={
            "name": "vAnalyzer Reports",
            "color": "#509EE3"
        })
        if r.status_code == 200:
            col_id = r.json()['id']
            print("Created Collection 'vAnalyzer Reports'.")
        else:
            print(f"Failed to create collection: {r.text}")
            return # Cannot proceed

    # 2. Check/Create Dashboard
    dash_id = None
    r_dash = requests.get(f"{MB_URL}/api/dashboard", headers=headers)
    if r_dash.status_code == 200:
        for dash in r_dash.json():
            if dash['name'] == "Main Vulnerability Dashboard" and dash['collection_id'] == col_id:
                dash_id = dash['id']
                print("Dashboard 'Main Vulnerability Dashboard' already exists. Skipping creation.")
                return # Prevent duplicates

    if not dash_id:
        r = requests.post(f"{MB_URL}/api/dashboard", headers=headers, json={
            "name": "Main Vulnerability Dashboard",
            "collection_id": col_id,
            "description": "Unified view of Tenable and Vicarius data"
        })
        if r.status_code == 200:
            dash_id = r.json()['id']
            print("Created Dashboard 'Main Vulnerability Dashboard'.")
        else:
            print(f"Failed to create dashboard: {r.text}")
            return

    # Helper to create SQL Card
    def create_card(name, sql, viz_type="table"):
        # Check if card exists in collection to avoid duplicates (Simple check by name)
        # Note: Ideally we check deeper, but this is MVP idempotency
        # For simplicity, we just create them for the new dashboard.
        card = {
            "name": name,
            "collection_id": col_id,
            "display": viz_type,
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": sql}
            }
        }
        r = requests.post(f"{MB_URL}/api/card", headers=headers, json=card)
        return r.json()['id'] if r.status_code == 200 else None

    # Helper to add card to dashboard
    def add_to_dash(card_id, row, col, width=12, height=6):
        requests.post(f"{MB_URL}/api/dashboard/{dash_id}/cards", headers=headers, json={
            "cardId": card_id,
            "row": row, "col": col, "sizeX": width, "sizeY": height
        })

    # Create Questions based on Views
    print("Creating Questions and adding to Dashboard...")
    
    # Q1: Security Gap
    c1 = create_card("Security Gap Analysis", "SELECT * FROM \"View_Security_Gap_Analysis\"", "table")
    if c1: add_to_dash(c1, 0, 0, 12, 8)

    # Q2: Mitigation Evolution
    c2 = create_card("Mitigation Evolution", "SELECT * FROM \"View_Mitigation_Evolution\"", "line")
    if c2: add_to_dash(c2, 8, 0, 12, 6)

    # Q3: Daily Reboots
    c3 = create_card("Pending Reboots", "SELECT * FROM \"View_Daily_Reboots\"", "table")
    if c3: add_to_dash(c3, 14, 0, 6, 6)
    
    # Q4: No CVEs
    c4 = create_card("Non-CVE Incidents", "SELECT * FROM \"View_No_CVE_Incidents\"", "bar")
    if c4: add_to_dash(c4, 14, 6, 6, 6)

    print(f"Dashboard created successfully! Link: {MB_URL}/dashboard/{dash_id}")

def run():
    if not wait_for_metabase():
        print("Metabase not available.")
        return

    session_id = get_session()
    if not session_id:
        print("Could not login or setup Metabase.")
        return

    print("Logged in successfully.")
    
    db_id = add_database(session_id)
    if db_id:
        create_dashboard_and_cards(session_id, db_id)

if __name__ == "__main__":
    run()
