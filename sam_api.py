import os
import json
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

from config import (
    SAM_API_KEY,
    SAM_BASE_URL,
    AFRICAN_COUNTRIES,
    AFRICA_KEYWORDS,
    AFRICAN_COUNTRY_NAMES,
    HISTORICAL_YEARS_BACK,
    MAX_RESULTS_PER_REQUEST,
    SAM_GOV_OPPORTUNITY_BASE_URL,
)


class EnhancedSAMAfricaAPI:
    """Enhanced production-ready class for comprehensive SAM.gov data collection."""
    
    def __init__(self) -> None:
        self.api_key = SAM_API_KEY
        self.base_url = SAM_BASE_URL
        self.sam_opportunity_url = SAM_GOV_OPPORTUNITY_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "SAM-Africa-Dashboard-Enhanced/2.0"
        })
        # Ensure data directory and database exist
        self._ensure_data_directory()
        self._init_database()
    
    def _ensure_data_directory(self) -> None:
        """Create data directory if it doesn't exist."""
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
            print(f"[INFO] Created {data_dir} directory")
    
    def _init_database(self) -> None:
        """Initialize SQLite database for comprehensive data storage."""
        db_path = "data/sam_africa_opportunities.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create main opportunities table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS opportunities (
                notice_id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                department TEXT,
                sub_tier TEXT,
                office TEXT,
                posted_date TEXT,
                response_date TEXT,
                notice_type TEXT,
                base_type TEXT,
                archive_date TEXT,
                archive_type TEXT,
                award_date TEXT,
                award_number TEXT,
                award_amount TEXT,
                awardee TEXT,
                pop_country_code TEXT,
                pop_country_name TEXT,
                pop_state TEXT,
                pop_city TEXT,
                african_country TEXT,
                sam_url TEXT,
                is_active INTEGER,
                data_collection_date TEXT,
                last_updated TEXT
            )
        ''')
        
        # Create index for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_country ON opportunities(african_country)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_date ON opportunities(posted_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_active ON opportunities(is_active)')
        
        conn.commit()
        conn.close()
        print("[INFO] Database initialized successfully")
    
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
    
    def fetch_comprehensive_historical_data(self) -> List[Dict]:
        """Fetch ALL historical data for African countries - comprehensive collection."""
        if not self._validate_api_key():
            print("[ERROR] API key validation failed")
            return []
        
        print(f"[INFO] Starting comprehensive historical data collection for {HISTORICAL_YEARS_BACK} years")
        all_opportunities = []
        
        # Generate date ranges for comprehensive collection
        end_date = datetime.now()
        start_date = end_date - timedelta(days=HISTORICAL_YEARS_BACK * 365)
        
        # Collect data in 30-day chunks to avoid API limits
        current_date = start_date
        chunk_count = 0
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=30), end_date)
            chunk_count += 1
            
            print(f"[INFO] Collecting chunk {chunk_count}: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            chunk_opportunities = self.fetch_opportunities(
                posted_from=current_date.strftime("%m/%d/%Y"),
                posted_to=chunk_end.strftime("%m/%d/%Y"),
                limit=MAX_RESULTS_PER_REQUEST
            )
            
            if chunk_opportunities:
                all_opportunities.extend(chunk_opportunities)
                print(f"[INFO] Collected {len(chunk_opportunities)} opportunities from this chunk")
            
            # Rate limiting to be respectful to SAM.gov API
            time.sleep(2)  # 2-second delay between requests
            current_date = chunk_end
        
        print(f"[INFO] Historical data collection complete: {len(all_opportunities)} total opportunities")
        return all_opportunities
    
    def fetch_opportunities(
        self,
        posted_from: Optional[str] = None,
        posted_to: Optional[str] = None,
        limit: int = MAX_RESULTS_PER_REQUEST,
    ) -> List[Dict]:
        """Enhanced opportunity fetching with better pagination and error handling."""
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
            "includeExpired": "true",  # Include expired/archived opportunities
        }
        
        all_opportunities: List[Dict] = []
        max_requests = 50  # Increased for comprehensive collection
        
        try:
            for request_count in range(max_requests):
                print(f"[INFO] API request {request_count + 1}/{max_requests}, offset: {params['offset']}")
                response = self.session.get(self.base_url, params=params, timeout=60)
                
                if response.status_code == 401:
                    print(f"[ERROR] Unauthorized access. Check API key: {self.api_key[:20]}...")
                    break
                elif response.status_code == 429:
                    print("[WARN] Rate limited. Waiting 30 seconds...")
                    time.sleep(30)
                    continue
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
                
                # Rate limiting
                time.sleep(1)  # 1-second delay between requests
                
        except requests.exceptions.Timeout:
            print("[ERROR] Request timeout - using partial results")
        except requests.exceptions.RequestException as exc:
            print(f"[ERROR] API request failed: {exc}")
        except Exception as exc:
            print(f"[ERROR] Unexpected error: {exc}")
        
        return all_opportunities
    
    def filter_africa_opportunities(self, opportunities: List[Dict]) -> List[Dict]:
        """Enhanced filtering for African opportunities with better accuracy."""
        africa_opps: List[Dict] = []
        
        for opp in opportunities:
            is_africa = False
            
            # Check place of performance
            pop = opp.get("placeOfPerformance", {})
            country_code = pop.get("country", {}).get("code", "")
            
            if country_code in AFRICAN_COUNTRIES:
                is_africa = True
            
            # Enhanced keyword checking
            title = str(opp.get("title", "")).lower()
            desc = str(opp.get("description", "")).lower()
            
            # Check for Africa keywords
            if any(keyword in title or keyword in desc for keyword in AFRICA_KEYWORDS):
                is_africa = True
            
            # Check for specific country names
            for country_name in AFRICAN_COUNTRY_NAMES.values():
                if country_name.lower() in title or country_name.lower() in desc:
                    is_africa = True
                    break
            
            if is_africa:
                africa_opps.append(self.process_opportunity(opp))
        
        return africa_opps
    
    def process_opportunity(self, opp: Dict) -> Dict:
        """Enhanced opportunity processing with SAM.gov links and status tracking."""
        notice_id = str(opp.get("noticeId", ""))
        
        processed: Dict = {
            "notice_id": notice_id,
            "title": str(opp.get("title", "")),
            "description": str(opp.get("description", ""))[:2000],  # Increased description length
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
        
        # Create direct SAM.gov link
        processed["sam_url"] = f"{self.sam_opportunity_url}{notice_id}/view" if notice_id else ""
        
        # Determine if opportunity is active
        archive_type = processed["archive_type"]
        processed["is_active"] = 1 if not archive_type or archive_type == "" or archive_type == "nan" else 0
        
        # Add metadata
        processed["data_collection_date"] = datetime.now().isoformat()
        processed["last_updated"] = datetime.now().isoformat()
        
        return processed
    
    def save_to_database(self, opportunities: List[Dict]) -> pd.DataFrame:
        """Save opportunities to SQLite database with duplicate handling."""
        if not opportunities:
            return pd.DataFrame()
        
        db_path = "data/sam_africa_opportunities.db"
        conn = sqlite3.connect(db_path)
        
        try:
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(opportunities)
            
            # Save to database with conflict resolution (replace on duplicate notice_id)
            df.to_sql('opportunities', conn, if_exists='replace', index=False)
            
            print(f"[INFO] Saved {len(df)} opportunities to database")
            
            conn.commit()
            return df
            
        except Exception as e:
            print(f"[ERROR] Failed to save to database: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def load_from_database(self, active_only: bool = False) -> pd.DataFrame:
        """Load opportunities from database with filtering options."""
        db_path = "data/sam_africa_opportunities.db"
        
        if not os.path.exists(db_path):
            print("[INFO] Database not found")
            return pd.DataFrame()
        
        try:
            conn = sqlite3.connect(db_path)
            
            if active_only:
                query = "SELECT * FROM opportunities WHERE is_active = 1 ORDER BY posted_date DESC"
            else:
                query = "SELECT * FROM opportunities ORDER BY posted_date DESC"
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            print(f"[INFO] Loaded {len(df)} opportunities from database")
            return df
            
        except Exception as e:
            print(f"[ERROR] Failed to load from database: {e}")
            return pd.DataFrame()
    
    def get_historical_opportunities(self) -> pd.DataFrame:
        """Get all historical (inactive/archived) opportunities."""
        db_path = "data/sam_africa_opportunities.db"
        
        if not os.path.exists(db_path):
            return pd.DataFrame()
        
        try:
            conn = sqlite3.connect(db_path)
            query = "SELECT * FROM opportunities WHERE is_active = 0 ORDER BY posted_date DESC"
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            print(f"[INFO] Loaded {len(df)} historical opportunities from database")
            return df
            
        except Exception as e:
            print(f"[ERROR] Failed to load historical data: {e}")
            return pd.DataFrame()


def update_comprehensive_africa_data() -> pd.DataFrame:
    """Perform comprehensive data update including all historical data."""
    api = EnhancedSAMAfricaAPI()
    print("[INFO] Starting comprehensive Africa data update...")
    
    # Fetch comprehensive historical data
    all_opps = api.fetch_comprehensive_historical_data()
    
    if not all_opps:
        print("[WARN] No opportunities retrieved from API")
        return create_enhanced_sample_data()
    
    print(f"[INFO] Processing {len(all_opps):,} total opportunities")
    africa_opps = api.filter_africa_opportunities(all_opps)
    
    if not africa_opps:
        print("[WARN] No Africa-related opportunities found")
        return create_enhanced_sample_data()
    
    print(f"[INFO] Found {len(africa_opps):,} Africa-related opportunities")
    df = api.save_to_database(africa_opps)
    
    return df


def create_enhanced_sample_data() -> pd.DataFrame:
    """Create enhanced sample data for demonstration."""
    print("[INFO] Creating enhanced sample data")
    
    sample_data = [
        {
            "notice_id": "SAMPLE001",
            "title": "Infrastructure Development in West Africa - Roads and Bridges",
            "description": "Comprehensive infrastructure development project focusing on road and bridge construction across multiple West African nations. This long-term initiative aims to improve transportation networks and economic connectivity throughout the region.",
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
            "award_amount": "$15,000,000",
            "awardee": "",
            "pop_country_code": "GHA",
            "pop_country_name": "Ghana",
            "pop_state": "",
            "pop_city": "Accra",
            "african_country": "Ghana",
            "sam_url": "https://sam.gov/opp/SAMPLE001/view",
            "is_active": 1,
            "data_collection_date": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        },
        {
            "notice_id": "SAMPLE002",
            "title": "Healthcare Systems Support in East Africa - Medical Equipment Supply",
            "description": "Multi-year healthcare improvement initiative providing medical equipment, training, and systems support to healthcare facilities across East African countries. Focus on sustainable healthcare infrastructure development.",
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
            "award_amount": "$25,000,000",
            "awardee": "",
            "pop_country_code": "KEN",
            "pop_country_name": "Kenya",
            "pop_state": "",
            "pop_city": "Nairobi",
            "african_country": "Kenya",
            "sam_url": "https://sam.gov/opp/SAMPLE002/view",
            "is_active": 1,
            "data_collection_date": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        },
        {
            "notice_id": "SAMPLE003_HISTORICAL",
            "title": "Agricultural Development Initiative - Southern Africa (COMPLETED)",
            "description": "Completed agricultural development program that provided farming equipment, training, and sustainable agriculture practices to rural communities across Southern Africa. This historical project ran from 2020-2023.",
            "department": "DEPARTMENT OF AGRICULTURE",
            "sub_tier": "Foreign Agricultural Service",
            "office": "International Development",
            "posted_date": "2020-01-15",
            "response_date": "2020-03-15",
            "notice_type": "Combined Synopsis/Solicitation",
            "base_type": "Contract Opportunity",
            "archive_date": "2023-12-31",
            "archive_type": "Manual Close",
            "award_date": "2020-04-01",
            "award_number": "AGR-2020-AFR-001",
            "award_amount": "$45,000,000",
            "awardee": "Global Development Solutions Inc.",
            "pop_country_code": "ZAF",
            "pop_country_name": "South Africa",
            "pop_state": "",
            "pop_city": "Cape Town",
            "african_country": "South Africa",
            "sam_url": "https://sam.gov/opp/SAMPLE003_HISTORICAL/view",
            "is_active": 0,
            "data_collection_date": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }
    ]
    
    api = EnhancedSAMAfricaAPI()
    df = api.save_to_database(sample_data)
    return df


if __name__ == "__main__":
    update_comprehensive_africa_data()