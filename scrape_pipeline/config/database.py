"""
Database configuration and connection management for the Valorant data pipeline.
Handles PostgreSQL connections, schema management, and database utilities.
"""

import os
import logging
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass
from contextlib import contextmanager, asynccontextmanager
from collections.abc import Generator
from sqlalchemy.orm import Session
import asyncio
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import RealDictCursor, execute_values
import asyncpg
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine

from .settings import settings

# Set up logging
logger = logging.getLogger(__name__)


@dataclass
class DatabaseMetrics:
    """Database performance and health metrics"""
    active_connections: int = 0
    idle_connections: int = 0
    total_connections: int = 0
    query_count: int = 0
    error_count: int = 0
    avg_query_time: float = 0.0
    last_health_check: Optional[datetime] = None
    database_size: Optional[str] = None


class DatabaseConfig:
    """Database configuration and connection management"""
    
    def __init__(self):
        self.config = settings.database
        self.metrics = DatabaseMetrics()
        
        # Connection pool settings
        self.pool_settings = {
            'minconn': int(os.getenv('DB_MIN_CONNECTIONS', 5)),
            'maxconn': int(os.getenv('DB_MAX_CONNECTIONS', 20)),
            'pool_size': int(os.getenv('DB_POOL_SIZE', 10)),
            'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', 20)),
            'pool_timeout': int(os.getenv('DB_POOL_TIMEOUT', 30)),
            'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', 3600)),  # 1 hour
        }
        
        # SQLAlchemy engine
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        
        # Connection pools
        self._sync_pool: Optional[pool.ThreadedConnectionPool] = None
        self._async_pool: Optional[asyncpg.Pool] = None
        
        # Schema information
        self.schema_version = "1.0.0"
        self.required_tables = [
            'matches', 'teams', 'players', 'events', 'patches',
            'match_maps', 'player_stats', 'team_stats', 
            'roster_changes', 'agent_picks', 'map_vetoes'
        ]
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine"""
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_string,
                poolclass=QueuePool,
                pool_size=self.pool_settings['pool_size'],
                max_overflow=self.pool_settings['max_overflow'],
                pool_timeout=self.pool_settings['pool_timeout'],
                pool_recycle=self.pool_settings['pool_recycle'],
                echo=settings.DEBUG,
                echo_pool=settings.DEBUG,
            )
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get or create SQLAlchemy session factory"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    async def get_async_pool(self) -> asyncpg.Pool:
        """Get or create async connection pool"""
        if self._async_pool is None:
            self._async_pool = await asyncpg.create_pool(
                self.config.connection_string,
                min_size=self.pool_settings['minconn'],
                max_size=self.pool_settings['maxconn'],
                command_timeout=self.pool_settings['pool_timeout'],
                server_settings={
                    'application_name': 'valorant_data_pipeline',
                    'timezone': 'UTC',
                }
            )
        return self._async_pool
    
    def get_sync_pool(self) -> pool.ThreadedConnectionPool:
        """Get or create sync connection pool"""
        if self._sync_pool is None:
            self._sync_pool = pool.ThreadedConnectionPool(
                self.pool_settings['minconn'],
                self.pool_settings['maxconn'],
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                cursor_factory=RealDictCursor,
                application_name='valorant_data_pipeline'
            )
        return self._sync_pool
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool (context manager)"""
        connection = None
        try:
            sync_pool = self.get_sync_pool()
            connection = sync_pool.getconn()
            yield connection
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                sync_pool.putconn(connection)
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a SQLAlchemy session (context manager)"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.error(f"Database session error: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    @asynccontextmanager
    async def get_async_connection(self):
        """Get an async database connection"""
        pool = await self.get_async_pool()
        connection = None
        try:
            connection = await pool.acquire()
            yield connection
        except Exception as e:
            logger.error(f"Async database connection error: {e}")
            raise
        finally:
            if connection:
                await pool.release(connection)
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    async def test_async_connection(self) -> bool:
        """Test async database connectivity"""
        try:
            async with self.get_async_connection() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Async database connection test failed: {e}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Database version and size
                    cursor.execute("SELECT version()")
                    db_version = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_database_size(current_database()))
                    """)
                    db_size = cursor.fetchone()[0]
                    
                    # Connection stats
                    cursor.execute("""
                        SELECT 
                            count(*) as total_connections,
                            count(*) FILTER (WHERE state = 'active') as active_connections,
                            count(*) FILTER (WHERE state = 'idle') as idle_connections
                        FROM pg_stat_activity 
                        WHERE datname = current_database()
                    """)
                    conn_stats = cursor.fetchone()
                    
                    # Table information
                    cursor.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            n_tup_ins as inserts,
                            n_tup_upd as updates,
                            n_tup_del as deletes,
                            n_live_tup as live_tuples,
                            n_dead_tup as dead_tuples,
                            last_vacuum,
                            last_autovacuum,
                            last_analyze,
                            last_autoanalyze
                        FROM pg_stat_user_tables
                        ORDER BY n_live_tup DESC
                    """)
                    table_stats = cursor.fetchall()
                    
                    return {
                        'version': db_version,
                        'size': db_size,
                        'connections': {
                            'total': conn_stats['total_connections'],
                            'active': conn_stats['active_connections'],
                            'idle': conn_stats['idle_connections']
                        },
                        'tables': [dict(row) for row in table_stats]
                    }
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {}
    
    def check_schema(self) -> Dict[str, bool]:
        """Check if required tables exist"""
        table_status = {}
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tablename 
                        FROM pg_tables 
                        WHERE schemaname = 'public'
                    """)
                    existing_tables = {row[0] for row in cursor.fetchall()}
                    
                    for table in self.required_tables:
                        table_status[table] = table in existing_tables
                        
        except Exception as e:
            logger.error(f"Schema check failed: {e}")
            for table in self.required_tables:
                table_status[table] = False
        
        return table_status
    
    def create_database_if_not_exists(self) -> bool:
        """Create database if it doesn't exist"""
        try:
            # Connect to postgres database to create our database
            temp_config = self.config
            temp_config.database = 'postgres'
            
            conn = psycopg2.connect(
                host=temp_config.host,
                port=temp_config.port,
                database='postgres',
                user=temp_config.username,
                password=temp_config.password
            )
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # Check if database exists
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.config.database,)
                )
                
                if not cursor.fetchone():
                    # Create database
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(self.config.database)
                        )
                    )
                    logger.info(f"Created database: {self.config.database}")
                    return True
                else:
                    logger.info(f"Database already exists: {self.config.database}")
                    return True
                    
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def execute_sql_file(self, file_path: str) -> bool:
        """Execute SQL commands from a file"""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_content)
                    conn.commit()
            
            logger.info(f"Successfully executed SQL file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to execute SQL file {file_path}: {e}")
            return False
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000, on_conflict: str = "IGNORE") -> int:
        """
        Perform bulk insert with conflict resolution
        
        Args:
            table_name: Target table name
            data: List of dictionaries with column->value mappings
            batch_size: Number of records per batch
            on_conflict: How to handle conflicts ("IGNORE", "UPDATE", "ERROR")
        
        Returns:
            Number of records inserted
        """
        if not data:
            return 0
        
        inserted_count = 0
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get column names from first record
                    columns = list(data[0].keys())
                    
                    # Prepare SQL statement
                    if on_conflict == "IGNORE":
                        conflict_clause = "ON CONFLICT DO NOTHING"
                    elif on_conflict == "UPDATE":
                        # Create UPDATE clause for all columns except primary key
                        update_columns = [col for col in columns if not col.endswith('_id')]
                        if update_columns:
                            update_clause = ", ".join([
                                f"{col} = EXCLUDED.{col}" for col in update_columns
                            ])
                            conflict_clause = f"ON CONFLICT DO UPDATE SET {update_clause}"
                        else:
                            conflict_clause = "ON CONFLICT DO NOTHING"
                    else:
                        conflict_clause = ""
                    
                    # Process in batches
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        
                        # Extract values for this batch
                        values = [[record.get(col) for col in columns] for record in batch]
                        
                        # Build and execute query
                        query = f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES %s
                            {conflict_clause}
                        """
                        
                        execute_values(cursor, query, values, page_size=batch_size)
                        inserted_count += len(batch)
                    
                    conn.commit()
                    logger.info(f"Bulk inserted {inserted_count} records into {table_name}")
                    
        except Exception as e:
            logger.error(f"Bulk insert failed for table {table_name}: {e}")
            raise
        
        return inserted_count
    
    def cleanup_old_data(self, retention_days: Dict[str, int]) -> Dict[str, int]:
        """
        Clean up old data based on retention policies
        
        Args:
            retention_days: Dictionary mapping table names to retention days
            
        Returns:
            Dictionary with deletion counts per table
        """
        deletion_counts = {}
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for table_name, days in retention_days.items():
                        cutoff_date = datetime.now() - timedelta(days=days)
                        
                        # Determine date column based on table
                        date_column = 'created_at'
                        if table_name == 'matches':
                            date_column = 'match_date'
                        elif table_name == 'events':
                            date_column = 'start_date'
                        
                        # Delete old records
                        cursor.execute(f"""
                            DELETE FROM {table_name} 
                            WHERE {date_column} < %s
                        """, (cutoff_date,))
                        
                        deleted_count = cursor.rowcount
                        deletion_counts[table_name] = deleted_count
                        
                        if deleted_count > 0:
                            logger.info(f"Deleted {deleted_count} old records from {table_name}")
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Data cleanup failed: {e}")
            raise
        
        return deletion_counts
    
    def vacuum_analyze(self, table_names: Optional[List[str]] = None) -> bool:
        """
        Run VACUUM ANALYZE on specified tables or all tables
        
        Args:
            table_names: List of table names to vacuum, or None for all tables
            
        Returns:
            True if successful, False otherwise
        """
        try:
            tables_to_vacuum = table_names or self.required_tables
            
            with self.get_connection() as conn:
                conn.autocommit = True  # VACUUM requires autocommit
                
                with conn.cursor() as cursor:
                    for table_name in tables_to_vacuum:
                        cursor.execute(f"VACUUM ANALYZE {table_name}")
                        logger.info(f"Vacuumed and analyzed table: {table_name}")
                
                conn.autocommit = False
                return True
                
        except Exception as e:
            logger.error(f"VACUUM ANALYZE failed: {e}")
            return False
    
    def get_table_sizes(self) -> Dict[str, str]:
        """Get sizes of all tables in human-readable format"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables 
                        WHERE schemaname = 'public'
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    """)
                    
                    return {row[0]: row[1] for row in cursor.fetchall()}
                    
        except Exception as e:
            logger.error(f"Failed to get table sizes: {e}")
            return {}
    
    def close_all_connections(self):
        """Close all connection pools"""
        if self._sync_pool:
            self._sync_pool.closeall()
            self._sync_pool = None
        
        if self._async_pool:
            asyncio.create_task(self._async_pool.close())
            self._async_pool = None
        
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None


# Global database instance
db = DatabaseConfig()


# Convenience functions
def get_db_session():
    """Get a database session context manager"""
    return db.get_session()


def get_db_connection():
    """Get a database connection context manager"""
    return db.get_connection()


async def get_async_db_connection():
    """Get an async database connection context manager"""
    return db.get_async_connection()


def test_database_connection() -> bool:
    """Test database connectivity"""
    return db.test_connection()


def get_database_info() -> Dict[str, Any]:
    """Get database information and statistics"""
    return db.get_database_info()


def check_database_schema() -> Dict[str, bool]:
    """Check if required tables exist"""
    return db.check_schema()


# Health check function for monitoring
def database_health_check() -> Dict[str, Any]:
    """Comprehensive database health check"""
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now(),
        'issues': []
    }
    
    try:
        # Test connectivity
        if not db.test_connection():
            health_status['status'] = 'unhealthy'
            health_status['issues'].append('Database connection failed')
            return health_status
        
        # Check schema
        schema_status = db.check_schema()
        missing_tables = [table for table, exists in schema_status.items() if not exists]
        if missing_tables:
            health_status['status'] = 'degraded'
            health_status['issues'].append(f'Missing tables: {missing_tables}')
        
        # Get database info
        db_info = db.get_database_info()
        health_status['info'] = db_info
        
        # Check connection limits
        if db_info.get('connections', {}).get('active', 0) > 15:  # 75% of default max
            health_status['status'] = 'degraded'
            health_status['issues'].append('High connection usage')
        
    except Exception as e:
        health_status['status'] = 'unhealthy'
        health_status['issues'].append(f'Health check failed: {str(e)}')
    
    return health_status