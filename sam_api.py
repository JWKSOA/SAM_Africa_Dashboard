import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from config import (
    SAM_API_KEY,
    SAM_BASE_URL,
    AFRICAN_COUNTRIES,
    AFRICA_KEYWORDS,
    AFRICAN_COUNTRY_NAMES,
)


class SAMAfricaAPI:
    """Production-ready class for interacting with SAM.gov API."""
    
    def __init__(self) -> None:
        self.api_key = SAM_API_KEY
        self.base_url = SAM_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "SAM-Africa-Dashboard/1.0"
        })
        # Ensure data directory exists
        self._ensure_data_directory()
    
    def _ensure_data_directory(self) -> None:
        """Create data directory if it doesn't exist."""
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
            print(f"[INFO] Created {data_dir} directory")
    
    def _validate_api_key(self) -> bool:
        """Validate API key by making a test request."""
        if not self.api_key:
            print("[ERROR] SAM_API_KEY environment variable not set")
            return False
        
        test_params = {
            "api_key": self.api_key,
            "limit": 1,
            "postedFrom": datetime.now().strftime("%m/%d/%Y"),
            "postedTo": datetime.now().strftime("%m/%d/%Y")
        }
        
        try:
            response = self.session.get(self.base_url, params=test_params, timeout=10)
            if response.status_code == 401:
                print(f"[ERROR] Invalid API key: {self.api_key[:20]}...")
                print("[INFO] Please verify your SAM.gov API key at https://sam.gov")
                return False
            elif response.status_code == 200:
                print("[INFO] API key validated successfully")
                return True
            else:
                print(f"[WARN] API validation returned status {response.status_code}")
                return True  # Continue anyway
        except Exception as e:
            print(f"[WARN] Could not validate API key: {e}")
            return True  # Continue anyway
    
    def fetch_opportunities(
        self,
        posted_from: Optional[str] = None,
        posted_to: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """Fetch opportunities from SAM.gov API with error handling."""
        # Validate API key first
        if not self._validate_api_key():
            print("[ERROR] API key validation failed - returning empty results")
            return []
        
        if not posted_from:
            posted_from = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
        if not posted_to:
            posted_to = datetime.now().strftime("%m/%d/%Y")
        
        params = {
            "api_key": self.api_key,
            "postedFrom": posted_from,
            "postedTo": posted_to,
            "limit": limit,
            "offset": 0,
        }
        
        all_opportunities: List[Dict] = []
        max_requests = 10  # Prevent infinite loops
        
        try:
            for request_count in range(max_requests):
                print(f"[INFO] API request {request_count + 1}/{max_requests}, offset: {params['offset']}")
                response = self.session.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 401:
                    print(f"[ERROR] Unauthorized access. Check API key: {self.api_key[:20]}...")
                    break
                elif response.status_code != 200:
                    print(f"[ERROR] API returned status {response.status_code}: {response.text[:200]}")
                    break
                
                data = response.json()
                opportunities = data.get("opportunitiesData", [])
                
                if not opportunities:
                    print("[INFO] No more opportunities found")
                    break
                
                all_opportunities.extend(opportunities)
                print(f"[INFO] Retrieved {len(opportunities)} opportunities (total: {len(all_opportunities)})")
                
                # Stop if fewer than limit results (last page)
                if len(opportunities) < limit:
                    break
                
                params["offset"] += limit
                
        except requests.exceptions.Timeout:
            print("[ERROR] Request timeout - using partial results")
        except requests.exceptions.RequestException as exc:
            print(f"[ERROR] API request failed: {exc}")
        except Exception as exc:
            print(f"[ERROR] Unexpected error: {exc}")
        
        return all_opportunities
    
    def filter_africa_opportunities(self, opportunities: List[Dict]) -> List[Dict]:
        """Filter opportunities relevant to Africa."""
        africa_opps: List[Dict] = []
        
        for opp in opportunities:
            is_africa = False
            
            # Check place of performance
            pop = opp.get("placeOfPerformance", {})
            country_code = pop.get("country", {}).get("code", "")
            
            if country_code in AFRICAN_COUNTRIES:
                is_africa = True
            
            # Check keywords in title and description
            title = str(opp.get("title", "")).lower()
            desc = str(opp.get("description", "")).lower()
            
            if any(keyword in title or keyword in desc for keyword in AFRICA_KEYWORDS):
                is_africa = True
            
            if is_africa:
                africa_opps.append(self.process_opportunity(opp))
        
        return africa_opps
    
    def process_opportunity(self, opp: Dict) -> Dict:
        """Extract and normalize opportunity fields."""
        processed: Dict = {
            "notice_id": str(opp.get("noticeId", "")),
            "title": str(opp.get("title", "")),
            "description": str(opp.get("description", ""))[:1000],  # Limit description length
            "department": str(opp.get("department", "")),
            "sub_tier": str(opp.get("subTier", "")),
            "office": str(opp.get("office", "")),
            "posted_date": str(opp.get("postedDate", "")),
            "response_date": str(opp.get("responseDeadLine", "")),
            "notice_type": str(opp.get("type", "")),
            "base_type": str(opp.get("baseType", "")),
            "archive_date": str(opp.get("archiveDate", "")),
            "archive_type": str(opp.get("archiveType", "")),
            "award_date": str(opp.get("awardDate", "")),
            "award_number": str(opp.get("awardNumber", "")),
            "award_amount": str(opp.get("awardAmount", "")),
            "awardee": str(opp.get("awardee", "")),
        }
        
        # Extract place of performance
        pop = opp.get("placeOfPerformance", {})
        processed["pop_country_code"] = str(pop.get("country", {}).get("code", ""))
        processed["pop_country_name"] = str(pop.get("country", {}).get("name", ""))
        processed["pop_state"] = str(pop.get("state", {}).get("name", ""))
        processed["pop_city"] = str(pop.get("city", {}).get("name", ""))
        
        # Map to readable country name
        processed["african_country"] = AFRICAN_COUNTRY_NAMES.get(
            processed["pop_country_code"], "Other/Multiple"
        )
        
        return processed
    
    def save_to_csv(
        self, opportunities: List[Dict], filename: str = "data/opportunities.csv"
    ) -> pd.DataFrame:
        """Save opportunities to CSV with directory creation."""
        self._ensure_data_directory()
        df = pd.DataFrame(opportunities)
        
        try:
            df.to_csv(filename, index=False)
            print(f"[INFO] Saved {len(df)} opportunities to {filename}")
        except Exception as e:
            print(f"[ERROR] Failed to save CSV: {e}")
        
        return df
    
    def load_from_csv(self, filename: str = "data/opportunities.csv") -> pd.DataFrame:
        """Load opportunities from CSV with comprehensive error handling."""
        try:
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                df = pd.read_csv(filename)
                print(f"[INFO] Loaded {len(df)} opportunities from {filename}")
                return df
            else:
                print(f"[INFO] {filename} not found or empty")
                return pd.DataFrame()
        except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            print(f"[WARN] Could not load CSV: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Unexpected error loading CSV: {e}")
            return pd.DataFrame()


def update_africa_data() -> pd.DataFrame:
    """Fetch, filter, and save Africa opportunities with fallback data."""
    api = SAMAfricaAPI()
    print("[INFO] Starting data update process...")
    
    all_opps = api.fetch_opportunities()
    if not all_opps:
        print("[WARN] No opportunities retrieved from API")
        # Return sample data for demo purposes
        return create_sample_data()
    
    print(f"[INFO] Processing {len(all_opps):,} total opportunities")
    africa_opps = api.filter_africa_opportunities(all_opps)
    
    if not africa_opps:
        print("[WARN] No Africa-related opportunities found")
        return create_sample_data()
    
    print(f"[INFO] Found {len(africa_opps):,} Africa-related opportunities")
    df = api.save_to_csv(africa_opps)
    return df


def create_sample_data() -> pd.DataFrame:
    """Create sample data when API fails."""
    print("[INFO] Creating sample data for demonstration")
    
    sample_data = [
        {
            "notice_id": "SAMPLE001",
            "title": "Infrastructure Development in West Africa",
            "description": "Sample opportunity for demonstration purposes",
            "department": "DEPARTMENT OF STATE",
            "sub_tier": "Bureau of African Affairs",
            "office": "Office of West African Affairs",
            "posted_date": datetime.now().strftime("%Y-%m-%d"),
            "response_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
            "notice_type": "Presolicitation",
            "base_type": "Contract Opportunity",
            "archive_date": "",
            "archive_type": "",
            "award_date": "",
            "award_number": "",
            "award_amount": "",
            "awardee": "",
            "pop_country_code": "GHA",
            "pop_country_name": "Ghana",
            "pop_state": "",
            "pop_city": "Accra",
            "african_country": "Ghana"
        },
        {
            "notice_id": "SAMPLE002",
            "title": "Healthcare Systems Support in East Africa",
            "description": "Sample healthcare opportunity for demonstration",
            "department": "DEPARTMENT OF HEALTH AND HUMAN SERVICES",
            "sub_tier": "Centers for Disease Control and Prevention",
            "office": "Global Health Center",
            "posted_date": datetime.now().strftime("%Y-%m-%d"),
            "response_date": (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d"),
            "notice_type": "Sources Sought",
            "base_type": "Contract Opportunity",
            "archive_date": "",
            "archive_type": "",
            "award_date": "",
            "award_number": "",
            "award_amount": "",
            "awardee": "",
            "pop_country_code": "KEN",
            "pop_country_name": "Kenya",
            "pop_state": "",
            "pop_city": "Nairobi",
            "african_country": "Kenya"
        },
        {
            "notice_id": "SAMPLE003",
            "title": "Agricultural Development Initiative - Southern Africa",
            "description": "Sample agricultural development opportunity",
            "department": "DEPARTMENT OF AGRICULTURE",
            "sub_tier": "Foreign Agricultural Service",
            "office": "International Development",
            "posted_date": datetime.now().strftime("%Y-%m-%d"),
            "response_date": (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d"),
            "notice_type": "Combined Synopsis/Solicitation",
            "base_type": "Contract Opportunity",
            "archive_date": "",
            "archive_type": "",
            "award_date": "",
            "award_number": "",
            "award_amount": "",
            "awardee": "",
            "pop_country_code": "ZAF",
            "pop_country_name": "South Africa",
            "pop_state": "",
            "pop_city": "Cape Town",
            "african_country": "South Africa"
        }
    ]
    
    api = SAMAfricaAPI()
    df = api.save_to_csv(sample_data)
    return df


if __name__ == "__main__":
    update_africa_data()
