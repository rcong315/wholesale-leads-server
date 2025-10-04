import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional

from .schema import CSV_COLUMNS, NUMERIC_COLUMNS, CREATE_TABLE_SQL, INDEXES

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning(
        "psutil not available - memory monitoring disabled. Install with: pip install psutil"
    )

logger = logging.getLogger(__name__)

# Configuration for memory management
DEFAULT_CHUNK_SIZE = 500  # Process leads in chunks of 500
MEMORY_WARNING_THRESHOLD_MB = 1000  # Warn when process uses > 1GB
MEMORY_CRITICAL_THRESHOLD_MB = 2000  # Critical warning at 2GB


class Database:
    def __init__(self, db_path: str = None, chunk_size: int = DEFAULT_CHUNK_SIZE):
        if db_path is None:
            # Use /var/data in Docker, current directory otherwise
            data_dir = "/var/data" if os.path.exists("/var/data") else "."
            db_path = os.path.join(data_dir, "leads.db")
        self.db_path = db_path
        self.chunk_size = chunk_size
        self.init_db()

    def init_db(self):
        """Initialize database and create tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Create table using schema definition
                conn.execute(CREATE_TABLE_SQL)

                # Create indexes from schema definition
                for index in INDEXES:
                    index_name = index["name"]
                    columns = ", ".join(index["columns"])
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} ON leads({columns})"
                    )

                # Check if we need to add created_at column to existing tables
                cursor = conn.execute("PRAGMA table_info(leads)")
                columns = [column[1] for column in cursor.fetchall()]
                if "created_at" not in columns:
                    conn.execute(
                        "ALTER TABLE leads ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
                    )
                    logger.info("Added created_at column to existing leads table")

                # Check if we need to add is_favorite column to existing tables
                cursor = conn.execute("PRAGMA table_info(leads)")
                columns = [column[1] for column in cursor.fetchall()]
                if "is_favorite" not in columns:
                    conn.execute(
                        "ALTER TABLE leads ADD COLUMN is_favorite BOOLEAN DEFAULT 0"
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_is_favorite ON leads(is_favorite)"
                    )
                    logger.info("Added is_favorite column to existing leads table")

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
                    chunk = leads[i : i + self.chunk_size]
                    chunk_num = (i // self.chunk_size) + 1
                    total_chunks = (len(leads) + self.chunk_size - 1) // self.chunk_size

                    logger.info(
                        f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} leads) for location {location}"
                    )

                    # Prepare insert data for this chunk
                    insert_data = []
                    current_time = datetime.now().isoformat()

                    for lead in chunk:
                        # Convert CSV headers to database columns
                        db_lead = {}
                        for key, value in lead.items():
                            db_key = (
                                key.lower()
                                .replace(" ", "_")
                                .replace(".", "")
                                .replace("?", "")
                                .replace("%", "pct")
                            )
                            if db_key in CSV_COLUMNS:
                                # Handle numeric fields
                                if db_key in NUMERIC_COLUMNS:
                                    try:
                                        db_lead[db_key] = (
                                            int(value)
                                            if value and value != "-"
                                            else None
                                        )
                                    except ValueError:
                                        db_lead[db_key] = None
                                else:
                                    db_lead[db_key] = value if value != "-" else None

                        # Ensure location and timestamps are properly set
                        db_lead["location"] = location
                        db_lead["created_at"] = current_time
                        insert_data.append(db_lead)

                    # Build dynamic insert query for this chunk
                    if insert_data:
                        sample_lead = insert_data[0]
                        columns = list(sample_lead.keys())
                        placeholders = ", ".join(["?" for _ in columns])
                        insert_query = f"INSERT INTO leads ({', '.join(columns)}) VALUES ({placeholders})"

                        # Execute batch insert for this chunk
                        values_list = [
                            [lead.get(col) for col in columns] for lead in insert_data
                        ]
                        conn.executemany(insert_query, values_list)
                        total_saved += len(insert_data)

                        logger.debug(
                            f"Saved chunk {chunk_num}: {len(insert_data)} leads"
                        )

                        # Log memory usage after each chunk
                        self._log_memory_usage(
                            f"after chunk {chunk_num}/{total_chunks}"
                        )

                # Commit all changes
                conn.commit()
                logger.info(
                    f"Successfully saved {total_saved} leads for location {location} in {total_chunks} chunks"
                )

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
                    (location,),
                )
                rows = cursor.fetchall()

                if not rows:
                    return None

                # Convert to list of dictionaries
                leads = [dict(row) for row in rows]

                # Get scrape info from first row
                first_row = rows[0]
                scraped_at = datetime.fromisoformat(first_row["scraped_at"])
                cache_age_days = (datetime.now() - scraped_at).days

                return {
                    "location": location,
                    "total_leads": len(leads),
                    "leads": leads,
                    "cached": True,
                    "cache_age_days": cache_age_days,
                    "scraped_at": first_row["scraped_at"],
                }

        except Exception as e:
            logger.error(f"Failed to get leads for location {location}: {e}")
            return None

    def location_exists(self, location: str) -> bool:
        """Check if location has cached data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE location = ?", (location,)
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

    def get_leads_paginated(
        self,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict] = None,
        sort_by: str = "id",
        sort_order: str = "asc",
    ) -> Dict:
        """Get paginated leads with optional filters and sorting"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Build WHERE clause from filters
                where_conditions = []
                params = []
                if filters:
                    range_filters = [
                        "minValue",
                        "maxValue",
                        "minSaleAmount",
                        "maxSaleAmount",
                        "minLoanBalance",
                        "maxLoanBalance",
                        "minInterestRate",
                        "maxInterestRate",
                    ]
                    special_filters = ["isFavorite"]

                    for key, value in filters.items():
                        if value is not None and key not in range_filters and key not in special_filters:
                            if key == "city":
                                where_conditions.append(f"{key} LIKE ?")
                                params.append(f"{value}%")
                            elif key == "mlsStatus":
                                where_conditions.append("mls_status LIKE ?")
                                params.append(f"%{value}%")
                            elif key == "probate":
                                where_conditions.append("probate = ?")
                                params.append(value)
                            elif key == "liens":
                                where_conditions.append("liens = ?")
                                params.append(value)
                            elif key == "preForeclosure":
                                where_conditions.append("pre_foreclosure = ?")
                                params.append(value)
                            elif key == "taxes":
                                where_conditions.append("taxes LIKE ?")
                                params.append(f"%{value}%")
                            else:
                                where_conditions.append(f"{key} = ?")
                                params.append(value)

                    # Handle favorites filter
                    if "isFavorite" in filters and filters["isFavorite"]:
                        where_conditions.append("is_favorite = 1")

                    # Handle value range filters (remove $ and commas before casting)
                    if "minValue" in filters and filters["minValue"] is not None:
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(est_value, '$', ''), ',', '') AS REAL) >= ?"
                        )
                        params.append(float(filters["minValue"]))
                    if "maxValue" in filters and filters["maxValue"] is not None:
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(est_value, '$', ''), ',', '') AS REAL) <= ?"
                        )
                        params.append(float(filters["maxValue"]))

                    # Handle last sale amount filters
                    if (
                        "minSaleAmount" in filters
                        and filters["minSaleAmount"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(last_sale_amount, '$', ''), ',', '') AS REAL) >= ?"
                        )
                        params.append(float(filters["minSaleAmount"]))
                    if (
                        "maxSaleAmount" in filters
                        and filters["maxSaleAmount"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(last_sale_amount, '$', ''), ',', '') AS REAL) <= ?"
                        )
                        params.append(float(filters["maxSaleAmount"]))

                    # Handle loan balance filters
                    if (
                        "minLoanBalance" in filters
                        and filters["minLoanBalance"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(total_loan_balance, '$', ''), ',', '') AS REAL) >= ?"
                        )
                        params.append(float(filters["minLoanBalance"]))
                    if (
                        "maxLoanBalance" in filters
                        and filters["maxLoanBalance"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(REPLACE(total_loan_balance, '$', ''), ',', '') AS REAL) <= ?"
                        )
                        params.append(float(filters["maxLoanBalance"]))

                    # Handle interest rate filters
                    if (
                        "minInterestRate" in filters
                        and filters["minInterestRate"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(loan_interest_rate, '%', '') AS REAL) >= ?"
                        )
                        params.append(float(filters["minInterestRate"]))
                    if (
                        "maxInterestRate" in filters
                        and filters["maxInterestRate"] is not None
                    ):
                        where_conditions.append(
                            "CAST(REPLACE(loan_interest_rate, '%', '') AS REAL) <= ?"
                        )
                        params.append(float(filters["maxInterestRate"]))

                where_clause = (
                    f"WHERE {' AND '.join(where_conditions)}"
                    if where_conditions
                    else ""
                )

                # Validate sort order
                sort_order = sort_order.lower()
                if sort_order not in ["asc", "desc"]:
                    sort_order = "asc"

                # Get total count
                count_query = f"SELECT COUNT(*) FROM leads {where_clause}"
                total = conn.execute(count_query, params).fetchone()[0]

                # Get paginated results - favorited leads first, then by specified sort
                query = f"SELECT * FROM leads {where_clause} ORDER BY is_favorite DESC, {sort_by} {sort_order} LIMIT ? OFFSET ?"
                cursor = conn.execute(query, params + [limit, offset])
                rows = cursor.fetchall()

                leads = [dict(row) for row in rows]

                return {
                    "leads": leads,
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "count": len(leads),
                }

        except Exception as e:
            logger.error(f"Failed to get paginated leads: {e}")
            raise

    def get_filter_options(self) -> Dict:
        """Get distinct values for filter dropdowns"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                options = {}

                # Get distinct cities
                cursor = conn.execute(
                    "SELECT DISTINCT city FROM leads WHERE city IS NOT NULL AND city != '' AND city != '-' ORDER BY city"
                )
                options["cities"] = [row[0] for row in cursor.fetchall()]

                # Get distinct MLS statuses
                cursor = conn.execute(
                    "SELECT DISTINCT mls_status FROM leads WHERE mls_status IS NOT NULL AND mls_status != '' AND mls_status != '-' ORDER BY mls_status"
                )
                options["mlsStatuses"] = [row[0] for row in cursor.fetchall()]

                # Get distinct probate values
                cursor = conn.execute(
                    "SELECT DISTINCT probate FROM leads WHERE probate IS NOT NULL AND probate != '' AND probate != '-' ORDER BY probate"
                )
                options["probateValues"] = [row[0] for row in cursor.fetchall()]

                # Get distinct liens values
                cursor = conn.execute(
                    "SELECT DISTINCT liens FROM leads WHERE liens IS NOT NULL AND liens != '' AND liens != '-' ORDER BY liens"
                )
                options["liensValues"] = [row[0] for row in cursor.fetchall()]

                # Get distinct pre-foreclosure values
                cursor = conn.execute(
                    "SELECT DISTINCT pre_foreclosure FROM leads WHERE pre_foreclosure IS NOT NULL AND pre_foreclosure != '' AND pre_foreclosure != '-' ORDER BY pre_foreclosure"
                )
                options["preForeclosureValues"] = [row[0] for row in cursor.fetchall()]

                # Get distinct taxes values
                cursor = conn.execute(
                    "SELECT DISTINCT taxes FROM leads WHERE taxes IS NOT NULL AND taxes != '' AND taxes != '-' ORDER BY taxes"
                )
                options["taxesValues"] = [row[0] for row in cursor.fetchall()]

                return options

        except Exception as e:
            logger.error(f"Failed to get filter options: {e}")
            raise

    def update_lead(self, lead_id: int, updates: Dict) -> bool:
        """Update a lead by ID and automatically mark as favorite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Build SET clause
                set_clauses = []
                params = []

                for key, value in updates.items():
                    set_clauses.append(f"{key} = ?")
                    params.append(value)

                # Always mark as favorite when updating
                set_clauses.append("is_favorite = ?")
                params.append(1)

                # Add lead_id to params for WHERE clause
                params.append(lead_id)

                update_query = f"UPDATE leads SET {', '.join(set_clauses)} WHERE id = ?"
                cursor = conn.execute(update_query, params)
                conn.commit()

                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"Failed to update lead {lead_id}: {e}")
            raise

    def get_lead_by_id(self, lead_id: int) -> Optional[Dict]:
        """Get a single lead by ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
                row = cursor.fetchone()

                if row:
                    return dict(row)
                return None

        except Exception as e:
            logger.error(f"Failed to get lead {lead_id}: {e}")
            raise

    def toggle_favorite(self, lead_id: int, is_favorite: bool) -> bool:
        """Toggle favorite status for a lead"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                update_query = "UPDATE leads SET is_favorite = ? WHERE id = ?"
                cursor = conn.execute(update_query, (1 if is_favorite else 0, lead_id))
                conn.commit()

                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"Failed to toggle favorite for lead {lead_id}: {e}")
            raise
