import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory monitoring disabled. Install with: pip install psutil")

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

# Configuration for memory management
DEFAULT_CHUNK_SIZE = 500  # Process leads in chunks of 500
MEMORY_WARNING_THRESHOLD_MB = 1000  # Warn when process uses > 1GB
MEMORY_CRITICAL_THRESHOLD_MB = 2000  # Critical warning at 2GB

class Database:
    def __init__(self, db_path: str = "leads.db", chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.db_path = db_path
        self.chunk_size = chunk_size
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
                    scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """)

                # Create indexes for common queries
                conn.execute("CREATE INDEX IF NOT EXISTS idx_location ON leads(location)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_scraped_at ON leads(scraped_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON leads(created_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON leads(city)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_zip ON leads(zip)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_property_address ON leads(property_address)")

                # Check if we need to add created_at column to existing tables
                cursor = conn.execute("PRAGMA table_info(leads)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'created_at' not in columns:
                    conn.execute("ALTER TABLE leads ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                    logger.info("Added created_at column to existing leads table")

                conn.commit()
                logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def _get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB"""
        if not PSUTIL_AVAILABLE:
            return 0.0
        try:
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0

    def _log_memory_usage(self, context: str = ""):
        """Log current memory usage with warnings if needed"""
        if not PSUTIL_AVAILABLE:
            logger.debug(f"Memory monitoring disabled (psutil not available) {context}")
            return 0.0

        memory_mb = self._get_memory_usage_mb()
        if memory_mb > MEMORY_CRITICAL_THRESHOLD_MB:
            logger.warning(f"CRITICAL: High memory usage {memory_mb:.1f}MB {context}")
        elif memory_mb > MEMORY_WARNING_THRESHOLD_MB:
            logger.warning(f"WARNING: High memory usage {memory_mb:.1f}MB {context}")
        else:
            logger.debug(f"Memory usage: {memory_mb:.1f}MB {context}")
        return memory_mb

    def save_leads(self, location: str, leads: List[Dict]) -> int:
        """Save leads to database using chunked processing, return number of leads saved"""
        if not leads:
            return 0

        self._log_memory_usage(f"before saving {len(leads)} leads for {location}")

        try:
            total_saved = 0

            with sqlite3.connect(self.db_path) as conn:
                # Clear existing leads for this location
                conn.execute("DELETE FROM leads WHERE location = ?", (location,))
                logger.info(f"Cleared existing leads for location {location}")

                # Process leads in chunks to manage memory
                for i in range(0, len(leads), self.chunk_size):
                    chunk = leads[i:i + self.chunk_size]
                    chunk_num = (i // self.chunk_size) + 1
                    total_chunks = (len(leads) + self.chunk_size - 1) // self.chunk_size

                    logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} leads) for location {location}")

                    # Prepare insert data for this chunk
                    insert_data = []
                    current_time = datetime.now().isoformat()

                    for lead in chunk:
                        # Convert CSV headers to database columns
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

                        # Ensure location and timestamps are properly set
                        db_lead['location'] = location
                        db_lead['created_at'] = current_time
                        insert_data.append(db_lead)

                    # Build dynamic insert query for this chunk
                    if insert_data:
                        sample_lead = insert_data[0]
                        columns = list(sample_lead.keys())
                        placeholders = ', '.join(['?' for _ in columns])
                        insert_query = f"INSERT INTO leads ({', '.join(columns)}) VALUES ({placeholders})"

                        # Execute batch insert for this chunk
                        values_list = [[lead.get(col) for col in columns] for lead in insert_data]
                        conn.executemany(insert_query, values_list)
                        total_saved += len(insert_data)

                        logger.debug(f"Saved chunk {chunk_num}: {len(insert_data)} leads")

                        # Log memory usage after each chunk
                        self._log_memory_usage(f"after chunk {chunk_num}/{total_chunks}")

                # Commit all changes
                conn.commit()
                logger.info(f"Successfully saved {total_saved} leads for location {location} in {total_chunks} chunks")

                self._log_memory_usage(f"after saving all leads for {location}")
                return total_saved

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

    def get_leads_paginated(self, offset: int = 0, limit: int = 100, filters: Optional[Dict] = None, sort_by: str = "id", sort_order: str = "asc") -> Dict:
        """Get paginated leads with optional filters and sorting"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Build WHERE clause from filters
                where_conditions = []
                params = []
                if filters:
                    for key, value in filters.items():
                        if value is not None:
                            where_conditions.append(f"{key} = ?")
                            params.append(value)

                where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

                # Validate sort order
                sort_order = sort_order.lower()
                if sort_order not in ["asc", "desc"]:
                    sort_order = "asc"

                # Get total count
                count_query = f"SELECT COUNT(*) FROM leads {where_clause}"
                total = conn.execute(count_query, params).fetchone()[0]

                # Get paginated results
                query = f"SELECT * FROM leads {where_clause} ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?"
                cursor = conn.execute(query, params + [limit, offset])
                rows = cursor.fetchall()

                leads = [dict(row) for row in rows]

                return {
                    "leads": leads,
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "count": len(leads)
                }

        except Exception as e:
            logger.error(f"Failed to get paginated leads: {e}")
            raise