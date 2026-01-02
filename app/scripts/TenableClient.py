import requests
import json
import time
from datetime import datetime

class TenableClient:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = "https://cloud.tenable.com"
        self.headers = {
            "X-ApiKeys": f"accessKey={self.api_key};secretKey={self.secret_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def get_assets(self):
        """
        Fetches assets from the workbench.
        """
        url = f"{self.base_url}/workbenches/assets"
        params = {
            "date_range": 30  # Last 30 days
        }
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            assets = response.json().get('assets', [])
            return self._parse_assets(assets)
        except Exception as e:
            print(f"Error fetching Tenable assets: {e}")
            return []

    def _parse_assets(self, assets_data):
        parsed_assets = []
        for asset in assets_data:
            # Extract basic info
            hostname = None
            if asset.get('hostname'):
                hostname = asset['hostname'][0]
            elif asset.get('fqdn'):
                hostname = asset['fqdn'][0]
            else:
                hostname = "Unknown Host"

            ip_addr = asset.get('ipv4', [None])[0]
            os_name = asset.get('operating_system', [None])[0]
            
            # Formatting last_seen
            last_seen_str = asset.get('last_seen')
            
            parsed_assets.append({
                "asset_uuid": asset.get('id'),
                "hostname": hostname,
                "ip_address": ip_addr,
                "operating_system": os_name,
                "last_seen": last_seen_str
            })
        return parsed_assets

    def export_vulns(self):
        """
        Simulate a vuln export or use a simpler per-asset fetch for MVP.
        For MVP, we will iterate the assets we found and fetch their vulns.
        (Not efficient for large scale, but works for PoC).
        """
        assets = self.get_assets()
        all_vulns = []
        print(f"Fetching vulnerabilities for {len(assets)} assets...")
        
        for asset in assets:
            asset_id = asset['asset_uuid']
            url = f"{self.base_url}/workbenches/assets/{asset_id}/vulnerabilities"
            try:
                # We need to be careful with rate limits here.
                time.sleep(0.5) 
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    vulns = response.json().get('vulnerabilities', [])
                    for v in vulns:
                        all_vulns.append({
                            "asset_uuid": asset_id,
                            "plugin_id": str(v.get('plugin_id')),
                            "cve": v.get('cve', 'N/A'),
                            "cvss": v.get('v3_base_score') or v.get('v2_base_score'),
                            "severity": str(v.get('severity_default_id')), 
                            "vulnerability_name": v.get('plugin_name'),
                            "first_found": v.get('first_found'),
                            "last_found": v.get('last_found'),
                            "state": v.get('vulnerability_state')
                        })
            except Exception as e:
                print(f"Failed to get vulns for {asset_id}: {e}")
                
        return all_vulns
