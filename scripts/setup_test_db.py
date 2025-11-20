"""
Setup test database with sample data for testing the performance analyzer.
Creates tables with realistic data volumes (500K+ rows).
"""
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

load_dotenv()


def create_connection():
    """Create database connection."""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )


def setup_tables(conn):
    """Create test tables."""
    cursor = conn.cursor()

    print("Creating tables...")

    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS order_items CASCADE")
    cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
    cursor.execute("DROP TABLE IF EXISTS products CASCADE")
    cursor.execute("DROP TABLE IF EXISTS users CASCADE")

    # Create users table (500K rows)
    cursor.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            age INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            active BOOLEAN DEFAULT TRUE
        )
    """)

    # Create products table (10K rows)
    cursor.execute("""
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(100),
            price DECIMAL(10, 2),
            stock INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Create orders table (1M rows)
    cursor.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            total DECIMAL(10, 2),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Create order_items table (2M rows)
    cursor.execute("""
        CREATE TABLE order_items (
            id SERIAL PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10, 2)
        )
    """)

    conn.commit()
    print("Tables created successfully!")


def populate_users(conn, num_users=500000):
    """Populate users table with test data."""
    cursor = conn.cursor()

    print(f"Populating users table with {num_users} records...")
    print("This may take a few minutes...")

    # Use batch inserts for better performance
    batch_size = 10000
    base_date = datetime.now() - timedelta(days=365*2)

    for batch_start in range(0, num_users, batch_size):
        batch_end = min(batch_start + batch_size, num_users)
        values = []

        for i in range(batch_start, batch_end):
            email = f"user{i}@example.com"
            name = f"User {i}"
            age = random.randint(18, 80)
            days_offset = random.randint(0, 730)
            created_at = base_date + timedelta(days=days_offset)
            active = random.choice([True, True, True, False])  # 75% active

            values.append(f"('{email}', '{name}', {age}, '{created_at}', {active})")

        query = f"INSERT INTO users (email, name, age, created_at, active) VALUES {','.join(values)}"
        cursor.execute(query)

        if (batch_end % 50000) == 0:
            print(f"  Inserted {batch_end}/{num_users} users...")
            conn.commit()

    conn.commit()
    print(f"[OK] Users table populated with {num_users} records")


def populate_products(conn, num_products=10000):
    """Populate products table with test data."""
    cursor = conn.cursor()

    print(f"Populating products table with {num_products} records...")

    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Food']
    batch_size = 1000

    for batch_start in range(0, num_products, batch_size):
        batch_end = min(batch_start + batch_size, num_products)
        values = []

        for i in range(batch_start, batch_end):
            name = f"Product {i}"
            category = random.choice(categories)
            price = round(random.uniform(5.99, 999.99), 2)
            stock = random.randint(0, 1000)

            values.append(f"('{name}', '{category}', {price}, {stock})")

        query = f"INSERT INTO products (name, category, price, stock) VALUES {','.join(values)}"
        cursor.execute(query)

    conn.commit()
    print(f"[OK] Products table populated with {num_products} records")


def populate_orders(conn, num_orders=100000):
    """Populate orders table with test data."""
    cursor = conn.cursor()

    print(f"Populating orders table with {num_orders} records...")

    # Get max user_id
    cursor.execute("SELECT MAX(id) FROM users")
    max_user_id = cursor.fetchone()[0]

    statuses = ['pending', 'processing', 'completed', 'cancelled']
    batch_size = 5000
    base_date = datetime.now() - timedelta(days=365)

    for batch_start in range(0, num_orders, batch_size):
        batch_end = min(batch_start + batch_size, num_orders)
        values = []

        for i in range(batch_start, batch_end):
            user_id = random.randint(1, max_user_id)
            total = round(random.uniform(10.0, 1000.0), 2)
            status = random.choice(statuses)
            days_offset = random.randint(0, 365)
            created_at = base_date + timedelta(days=days_offset)

            values.append(f"({user_id}, {total}, '{status}', '{created_at}')")

        query = f"INSERT INTO orders (user_id, total, status, created_at) VALUES {','.join(values)}"
        cursor.execute(query)

        if (batch_end % 25000) == 0:
            print(f"  Inserted {batch_end}/{num_orders} orders...")
            conn.commit()

    conn.commit()
    print(f"[OK] Orders table populated with {num_orders} records")


def create_some_indexes(conn):
    """Create some indexes (but not all optimal ones) for testing."""
    cursor = conn.cursor()

    print("Creating some initial indexes...")

    # Only create primary key indexes (which are automatic)
    # And one index on users.id for foreign key performance
    cursor.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")

    conn.commit()
    print("[OK] Initial indexes created")


def print_table_stats(conn):
    """Print statistics about created tables."""
    cursor = conn.cursor()

    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)

    tables = ['users', 'products', 'orders']

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table.upper():15} {count:>10,} rows")

    print("\nIndexes:")
    cursor.execute("""
        SELECT
            tablename,
            indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
    """)

    for row in cursor.fetchall():
        print(f"  {row[0]}.{row[1]}")

    print("="*50 + "\n")


def main():
    """Main setup function."""
    print("\n" + "="*50)
    print("PostgreSQL Performance Analyzer - Test DB Setup")
    print("="*50 + "\n")

    conn = create_connection()

    try:
        # Setup tables
        setup_tables(conn)

        # Populate with data
        populate_users(conn, num_users=500000)
        populate_products(conn, num_products=10000)
        populate_orders(conn, num_orders=100000)

        # Create minimal indexes
        create_some_indexes(conn)

        # Analyze tables for accurate statistics
        print("\nAnalyzing tables for query planner...")
        cursor = conn.cursor()
        cursor.execute("ANALYZE users")
        cursor.execute("ANALYZE products")
        cursor.execute("ANALYZE orders")
        conn.commit()
        print("[OK] Tables analyzed")

        # Print stats
        print_table_stats(conn)

        print("[SUCCESS] Database setup complete!")
        print("\nYou can now run queries like:")
        print("  SELECT * FROM users WHERE email = 'user50000@example.com'")
        print("  SELECT * FROM users WHERE email LIKE 'user1%'")
        print("  SELECT * FROM users ORDER BY created_at DESC LIMIT 10")
        print("  SELECT * FROM orders WHERE status = 'pending'")
        print("\nThese queries will demonstrate sequential scans that need indexes.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
