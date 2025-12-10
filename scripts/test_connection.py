#!/usr/bin/env python3
"""
Test database connection and diagnose authentication issues

Usage:
    python3 scripts/test_connection.py
"""
import psycopg2
import os
from dotenv import load_dotenv

# Load .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
print(f"Looking for .env file at: {env_path}")
print(f".env file exists: {os.path.exists(env_path)}")
print()

load_dotenv(env_path)

# Read environment variables
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'pg_analyser'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

print("="*70)
print("DATABASE CONNECTION TEST")
print("="*70)
print("\nConfiguration loaded:")
print(f"  Host:     {db_config['host']}")
print(f"  Port:     {db_config['port']}")
print(f"  Database: {db_config['database']}")
print(f"  User:     {db_config['user']}")
print(f"  Password: {'*' * len(db_config['password']) if db_config['password'] else 'NOT SET'}")
print()

if not db_config['password']:
    print("[ERROR] DB_PASSWORD is not set!")
    print("\nPlease ensure:")
    print("  1. You have created a .env file: cp .env.example .env")
    print("  2. You have set DB_PASSWORD in .env")
    print()
    exit(1)

print("Attempting to connect...")
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Test query
    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0]

    print("[SUCCESS] Connection established!")
    print()
    print(f"PostgreSQL Version:")
    print(f"  {version}")
    print()

    # Check pg_stat_statements
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
        )
    """)
    has_pg_stat = cursor.fetchone()[0]

    if has_pg_stat:
        print("[OK] pg_stat_statements extension is installed")
    else:
        print("[WARNING] pg_stat_statements extension is NOT installed")
        print("          Run: CREATE EXTENSION pg_stat_statements;")

    print()

    # Check database size
    cursor.execute(f"""
        SELECT pg_size_pretty(pg_database_size('{db_config['database']}'))
    """)
    size = cursor.fetchone()[0]
    print(f"Database size: {size}")

    # Check if test tables exist
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name IN ('users', 'products', 'orders')
    """)
    test_tables = cursor.fetchone()[0]

    if test_tables > 0:
        print(f"[INFO] Found {test_tables}/3 test tables (users, products, orders)")
    else:
        print("[INFO] No test tables found - ready for setup")

    cursor.close()
    conn.close()

    print()
    print("="*70)
    print("Connection test PASSED")
    print("="*70)
    print()
    print("You can now run: python3 scripts/setup_test_db.py")
    print()

except psycopg2.OperationalError as e:
    print(f"[ERROR] Connection failed!")
    print()
    print(f"Error details: {e}")
    print()
    print("Common issues:")
    print()
    print("1. Wrong password:")
    print("   - Check .env file has correct DB_PASSWORD")
    print("   - For Docker: password must match docker-compose.yml")
    print()
    print("2. Database doesn't exist:")
    print("   - Create it: docker-compose exec postgres createdb -U postgres pg_analyser")
    print("   - Or: psql -U postgres -c 'CREATE DATABASE pg_analyser'")
    print()
    print("3. PostgreSQL not running:")
    print("   - Docker: docker-compose ps")
    print("   - Local: sudo systemctl status postgresql")
    print()
    print("4. Wrong host/port:")
    print("   - Docker containers: host should be 'localhost' or '127.0.0.1'")
    print("   - Check port is 5432 (default)")
    print()
    print("5. pg_hba.conf authentication:")
    print("   - Docker: should work by default")
    print("   - Local: may need to edit pg_hba.conf for md5 or trust auth")
    print()
    exit(1)

except Exception as e:
    print(f"[ERROR] Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
