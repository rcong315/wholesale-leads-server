import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Column mapping for easy schema modifications
CSV_COLUMNS = [
    "property_address", "city", "state", "zip", "phone_numbers",
    "owner_first_name", "owner_last_name", "list_count", "tag_count",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip_code",
    "emails", "pics", "apn", "est_value", "county", "date_added", "date_updated",
    "last_sale_date", "last_sale_amount", "mailing_county", "salesforce_lead_id",
    "vacancy", "mailing_vacancy", "opt_out", "property_type", "owner_occupied",
    "bedrooms", "bathrooms", "property_sqft", "lot_size", "year_build",
    "assessed_value", "total_loan_balance", "est_equity", "est_ltv", "mls_status",
    "data_provider_ranking", "probate", "liens", "pre_foreclosure", "taxes",
    "vacant", "zoning", "loan_type", "loan_interest_rate", "owner_2_first_name",
    "owner_2_last_name", "self_managed", "pushed_to_batchdialer", "lead_score",
    "arv", "spread", "pct_arv"
]

class Database:
    def __init__(self, db_path: str = "leads.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Initialize database and create tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_address TEXT,
                    city TEXT,
                    state TEXT,
                    zip TEXT,
                    phone_numbers TEXT,
                    owner_first_name TEXT,
                    owner_last_name TEXT,
                    list_count INTEGER,
                    tag_count INTEGER,
                    mailing_address TEXT,
                    mailing_city TEXT,
                    mailing_state TEXT,
                    mailing_zip_code TEXT,
                    emails TEXT,
                    pics TEXT,
                    apn TEXT,
                    est_value TEXT,
                    county TEXT,
                    date_added TEXT,
                    date_updated TEXT,
                    last_sale_date TEXT,
                    last_sale_amount TEXT,
                    mailing_county TEXT,
                    salesforce_lead_id TEXT,
                    vacancy TEXT,
                    mailing_vacancy TEXT,
                    opt_out TEXT,
                    property_type TEXT,
                    owner_occupied TEXT,
                    bedrooms INTEGER,
                    bathrooms INTEGER,
                    property_sqft TEXT,
                    lot_size TEXT,
                    year_build INTEGER,
                    assessed_value TEXT,
                    total_loan_balance TEXT,
                    est_equity TEXT,
                    est_ltv TEXT,
                    mls_status TEXT,
                    data_provider_ranking TEXT,
                    probate TEXT,
                    liens TEXT,
                    pre_foreclosure TEXT,
                    taxes TEXT,
                    vacant TEXT,
                    zoning TEXT,
                    loan_type TEXT,
                    loan_interest_rate TEXT,
                    owner_2_first_name TEXT,
                    owner_2_last_name TEXT,
                    self_managed TEXT,
                    pushed_to_batchdialer TEXT,
                    lead_score INTEGER,
                    arv TEXT,
                    spread TEXT,
                    pct_arv TEXT,
                    location TEXT,
                    scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """)

                # Create indexes for common queries
                conn.execute("CREATE INDEX IF NOT EXISTS idx_location ON leads(location)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_scraped_at ON leads(scraped_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_property_address ON leads(property_address)")

                conn.commit()
                logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def save_leads(self, location: str, leads: List[Dict]) -> int:
        """Save leads to database, return number of leads saved"""
        if not leads:
            return 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Clear existing leads for this location
                conn.execute("DELETE FROM leads WHERE location = ?", (location,))

                # Prepare insert data
                insert_data = []
                for lead in leads:
                    # Convert CSV headers to database columns (replace spaces with underscores, lowercase)
                    db_lead = {}
                    for key, value in lead.items():
                        db_key = key.lower().replace(' ', '_').replace('.', '').replace('?', '').replace('%', 'pct')
                        if db_key in CSV_COLUMNS:
                            # Handle numeric fields
                            if db_key in ['list_count', 'tag_count', 'bedrooms', 'bathrooms', 'year_build', 'lead_score']:
                                try:
                                    db_lead[db_key] = int(value) if value and value != '-' else None
                                except ValueError:
                                    db_lead[db_key] = None
                            else:
                                db_lead[db_key] = value if value != '-' else None

                    db_lead['location'] = location
                    insert_data.append(db_lead)

                # Build dynamic insert query
                if insert_data:
                    sample_lead = insert_data[0]
                    columns = list(sample_lead.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_query = f"INSERT INTO leads ({', '.join(columns)}) VALUES ({placeholders})"

                    # Execute batch insert
                    values_list = [[lead.get(col) for col in columns] for lead in insert_data]
                    conn.executemany(insert_query, values_list)
                    conn.commit()

                    logger.info(f"Saved {len(insert_data)} leads for location {location}")
                    return len(insert_data)

        except Exception as e:
            logger.error(f"Failed to save leads for location {location}: {e}")
            raise

        return 0

    def get_leads(self, location: str) -> Optional[Dict]:
        """Get leads for a location, return dict with leads data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM leads WHERE location = ? ORDER BY scraped_at DESC",
                    (location,)
                )
                rows = cursor.fetchall()

                if not rows:
                    return None

                # Convert to list of dictionaries
                leads = [dict(row) for row in rows]

                # Get scrape info from first row
                first_row = rows[0]
                scraped_at = datetime.fromisoformat(first_row['scraped_at'])
                cache_age_days = (datetime.now() - scraped_at).days

                return {
                    "location": location,
                    "total_leads": len(leads),
                    "leads": leads,
                    "cached": True,
                    "cache_age_days": cache_age_days,
                    "scraped_at": first_row['scraped_at']
                }

        except Exception as e:
            logger.error(f"Failed to get leads for location {location}: {e}")
            return None

    def location_exists(self, location: str) -> bool:
        """Check if location has cached data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE location = ?",
                    (location,)
                )
                count = cursor.fetchone()[0]
                return count > 0

        except Exception as e:
            logger.error(f"Failed to check if location exists {location}: {e}")
            return False

    def get_locations(self) -> List[str]:
        """Get all cached locations"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT DISTINCT location FROM leads ORDER BY location"
                )
                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            return []

    def delete_location(self, location: str) -> bool:
        """Delete all leads for a location"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("DELETE FROM leads WHERE location = ?", (location,))
                conn.commit()
                logger.info(f"Deleted {cursor.rowcount} leads for location {location}")
                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"Failed to delete location {location}: {e}")
            return False