"""
Setup comprehensive test database for PostgreSQL performance analyzer

This script is self-contained and handles:
1. Extension setup (pg_stat_statements)
2. Optimizer metadata schema (functions, views, permissions)
3. Test database schema creation
4. Test data population

Creates realistic e-commerce schema with sufficient data for testing:
- 500K users
- 50K products
- 1M orders
- 3M order items
- 100K reviews
- 200K user sessions
- Realistic data distributions and relationships

Usage:
    # Local PostgreSQL (requires .env or environment variables)
    python3 scripts/setup_test_db.py

    # Docker environment
    make setup-test
    # or
    docker-compose exec app python3 scripts/setup_test_db.py

NOTE: This script includes all functionality from scripts/init-db.sql
      and can be run independently or after Docker initialization.
      All CREATE statements use IF NOT EXISTS for idempotency.
"""
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random
import string

load_dotenv()


def create_connection():
    """Create database connection"""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )


def setup_extensions(conn):
    """Enable required PostgreSQL extensions"""
    cursor = conn.cursor()

    print("Setting up PostgreSQL extensions...")

    try:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        print("[OK] pg_stat_statements extension enabled")
    except Exception as e:
        print(f"[WARNING] Could not enable pg_stat_statements: {e}")
        print("         This extension is optional for basic testing")

    conn.commit()


def setup_optimizer_metadata(conn):
    """Create optimizer metadata schema and helper objects"""
    cursor = conn.cursor()

    print("\nSetting up optimizer metadata schema...")

    # Create schema for optimizer metadata
    cursor.execute("CREATE SCHEMA IF NOT EXISTS optimizer_metadata")

    # Grant permissions
    try:
        cursor.execute("GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO PUBLIC")
        cursor.execute("GRANT SELECT ON pg_stat_statements TO PUBLIC")
    except Exception as e:
        print(f"[WARNING] Could not grant permissions: {e}")

    # Create reset function
    cursor.execute("""
        CREATE OR REPLACE FUNCTION optimizer_metadata.reset_query_stats()
        RETURNS void AS $$
        BEGIN
            PERFORM pg_stat_statements_reset();
            RAISE NOTICE 'pg_stat_statements has been reset';
        END;
        $$ LANGUAGE plpgsql
    """)

    # Create performance view
    cursor.execute("""
        CREATE OR REPLACE VIEW optimizer_metadata.query_performance AS
        SELECT
            queryid,
            query,
            calls,
            total_exec_time,
            mean_exec_time,
            min_exec_time,
            max_exec_time,
            stddev_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            temp_blks_read,
            temp_blks_written,
            blk_read_time,
            blk_write_time
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
    """)

    conn.commit()
    print("[OK] Optimizer metadata schema created")


def setup_tables(conn):
    """Create comprehensive test schema"""
    cursor = conn.cursor()

    print("\nCreating tables...")

    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS reviews CASCADE")
    cursor.execute("DROP TABLE IF EXISTS order_items CASCADE")
    cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
    cursor.execute("DROP TABLE IF EXISTS products CASCADE")
    cursor.execute("DROP TABLE IF EXISTS categories CASCADE")
    cursor.execute("DROP TABLE IF EXISTS users CASCADE")
    cursor.execute("DROP TABLE IF EXISTS user_sessions CASCADE")
    cursor.execute("DROP TABLE IF EXISTS audit_logs CASCADE")

    # Users table - 500K users with realistic attributes
    cursor.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            username VARCHAR(100) NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            age INTEGER,
            country VARCHAR(50),
            subscription_tier VARCHAR(20),
            account_balance DECIMAL(10, 2) DEFAULT 0,
            email_verified BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP,
            status VARCHAR(20) DEFAULT 'active'
        )
    """)

    # Categories table
    cursor.execute("""
        CREATE TABLE categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            parent_id INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Products table - 50K products with categories
    cursor.execute("""
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category_id INTEGER,
            sku VARCHAR(50) UNIQUE,
            price DECIMAL(10, 2) NOT NULL,
            cost DECIMAL(10, 2),
            stock_quantity INTEGER DEFAULT 0,
            weight_kg DECIMAL(8, 3),
            is_active BOOLEAN DEFAULT TRUE,
            featured BOOLEAN DEFAULT FALSE,
            rating DECIMAL(3, 2),
            review_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Orders table - 1M orders
    cursor.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_number VARCHAR(50) UNIQUE,
            subtotal DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            shipping DECIMAL(10, 2),
            total DECIMAL(10, 2),
            status VARCHAR(50),
            payment_method VARCHAR(50),
            shipping_country VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP
        )
    """)

    # Order items table - 3M items
    cursor.execute("""
        CREATE TABLE order_items (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            discount_percent DECIMAL(5, 2) DEFAULT 0,
            total_price DECIMAL(10, 2)
        )
    """)

    # Reviews table - 100K reviews
    cursor.execute("""
        CREATE TABLE reviews (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            rating INTEGER CHECK (rating >= 1 AND rating <= 5),
            title VARCHAR(255),
            content TEXT,
            helpful_count INTEGER DEFAULT 0,
            verified_purchase BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # User sessions table - for testing time-based queries
    cursor.execute("""
        CREATE TABLE user_sessions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            session_token VARCHAR(255) NOT NULL,
            ip_address VARCHAR(45),
            user_agent TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            last_activity TIMESTAMP
        )
    """)

    # Audit logs - for testing large table queries
    cursor.execute("""
        CREATE TABLE audit_logs (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            action VARCHAR(100),
            table_name VARCHAR(100),
            record_id INTEGER,
            old_value JSONB,
            new_value JSONB,
            ip_address VARCHAR(45),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    conn.commit()
    print("[OK] Tables created successfully")


def populate_categories(conn):
    """Populate categories table"""
    cursor = conn.cursor()

    print("\nPopulating categories...")

    categories = [
        (1, 'Electronics', None),
        (2, 'Computers', 1),
        (3, 'Smartphones', 1),
        (4, 'Audio', 1),
        (5, 'Clothing', None),
        (6, 'Men', 5),
        (7, 'Women', 5),
        (8, 'Kids', 5),
        (9, 'Books', None),
        (10, 'Fiction', 9),
        (11, 'Non-Fiction', 9),
        (12, 'Home & Garden', None),
        (13, 'Furniture', 12),
        (14, 'Kitchen', 12),
        (15, 'Sports', None),
        (16, 'Outdoor', 15),
        (17, 'Fitness', 15),
        (18, 'Toys', None),
        (19, 'Games', 18),
        (20, 'Food & Beverage', None),
    ]

    for cat_id, name, parent_id in categories:
        parent_str = str(parent_id) if parent_id else 'NULL'
        cursor.execute(f"""
            INSERT INTO categories (id, name, parent_id, description)
            VALUES ({cat_id}, '{name}', {parent_str}, 'Category for {name}')
        """)

    conn.commit()
    print("[OK] Categories populated")


def populate_users(conn, num_users=500000):
    """Populate users table with realistic data"""
    cursor = conn.cursor()

    print(f"\nPopulating users table with {num_users:,} records...")
    print("This may take several minutes...")

    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia', 'Japan', 'India']
    tiers = ['free', 'basic', 'premium', 'enterprise']
    statuses = ['active', 'inactive', 'suspended']

    batch_size = 5000
    base_date = datetime.now() - timedelta(days=365*3)

    for batch_start in range(0, num_users, batch_size):
        batch_end = min(batch_start + batch_size, num_users)
        values = []

        for i in range(batch_start, batch_end):
            email = f"user{i}@example{i % 10}.com"
            username = f"user_{i}"
            first_name = f"FirstName{i % 1000}"
            last_name = f"LastName{i % 500}"
            age = random.randint(18, 75)
            country = random.choice(countries)

            # Realistic tier distribution: 60% free, 25% basic, 12% premium, 3% enterprise
            rand = random.random()
            if rand < 0.60:
                tier = 'free'
            elif rand < 0.85:
                tier = 'basic'
            elif rand < 0.97:
                tier = 'premium'
            else:
                tier = 'enterprise'

            balance = round(random.uniform(0, 1000), 2)
            email_verified = random.choice([True, True, True, False])  # 75% verified

            days_offset = random.randint(0, 1095)
            created_at = base_date + timedelta(days=days_offset)

            # Last login within last 30 days for active users
            last_login_days = random.randint(0, 30)
            last_login = datetime.now() - timedelta(days=last_login_days)

            # Status: 85% active, 10% inactive, 5% suspended
            rand_status = random.random()
            if rand_status < 0.85:
                status = 'active'
            elif rand_status < 0.95:
                status = 'inactive'
            else:
                status = 'suspended'

            values.append(
                f"('{email}', '{username}', '{first_name}', '{last_name}', {age}, "
                f"'{country}', '{tier}', {balance}, {email_verified}, "
                f"'{created_at}', '{last_login}', '{status}')"
            )

        query = f"""
            INSERT INTO users (email, username, first_name, last_name, age, country,
                             subscription_tier, account_balance, email_verified,
                             created_at, last_login, status)
            VALUES {','.join(values)}
        """
        cursor.execute(query)

        if batch_end % 50000 == 0 or batch_end == num_users:
            print(f"  Inserted {batch_end:,}/{num_users:,} users...")
            conn.commit()

    print(f"[OK] Users table populated with {num_users:,} records")


def populate_products(conn, num_products=50000):
    """Populate products table with realistic data"""
    cursor = conn.cursor()

    print(f"\nPopulating products table with {num_products:,} records...")

    batch_size = 2000

    for batch_start in range(0, num_products, batch_size):
        batch_end = min(batch_start + batch_size, num_products)
        values = []

        for i in range(batch_start, batch_end):
            name = f"Product {i}"
            category_id = random.randint(1, 20)
            sku = f"SKU-{i:08d}"

            # Price distribution: most products between $10-$100, some expensive
            if random.random() < 0.9:
                price = round(random.uniform(9.99, 199.99), 2)
            else:
                price = round(random.uniform(200, 2000), 2)

            cost = round(price * random.uniform(0.3, 0.7), 2)
            stock = random.randint(0, 500)
            weight = round(random.uniform(0.1, 25.0), 3)
            is_active = random.choice([True, True, True, False])  # 75% active
            featured = random.choice([True, False, False, False, False])  # 20% featured

            # Rating between 1.0 and 5.0
            rating = round(random.uniform(2.5, 5.0), 2)
            review_count = random.randint(0, 500)

            values.append(
                f"('{name}', {category_id}, '{sku}', {price}, {cost}, {stock}, "
                f"{weight}, {is_active}, {featured}, {rating}, {review_count})"
            )

        query = f"""
            INSERT INTO products (name, category_id, sku, price, cost, stock_quantity,
                                weight_kg, is_active, featured, rating, review_count)
            VALUES {','.join(values)}
        """
        cursor.execute(query)

        if batch_end % 10000 == 0 or batch_end == num_products:
            print(f"  Inserted {batch_end:,}/{num_products:,} products...")
            conn.commit()

    print(f"[OK] Products table populated with {num_products:,} records")


def populate_orders(conn, num_orders=1000000):
    """Populate orders table with realistic data"""
    cursor = conn.cursor()

    print(f"\nPopulating orders table with {num_orders:,} records...")
    print("This may take several minutes...")

    # Get max IDs
    cursor.execute("SELECT MAX(id) FROM users")
    max_user_id = cursor.fetchone()[0]

    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France']

    batch_size = 5000
    base_date = datetime.now() - timedelta(days=365*2)

    for batch_start in range(0, num_orders, batch_size):
        batch_end = min(batch_start + batch_size, num_orders)
        values = []

        for i in range(batch_start, batch_end):
            user_id = random.randint(1, max_user_id)
            order_number = f"ORD-{i:010d}"
            subtotal = round(random.uniform(20.0, 500.0), 2)
            tax = round(subtotal * random.uniform(0.05, 0.15), 2)
            shipping = round(random.uniform(5.0, 25.0), 2)
            total = round(subtotal + tax + shipping, 2)

            # Realistic status distribution
            rand = random.random()
            if rand < 0.05:
                status = 'pending'
            elif rand < 0.10:
                status = 'processing'
            elif rand < 0.20:
                status = 'shipped'
            elif rand < 0.90:
                status = 'delivered'
            else:
                status = 'cancelled'

            payment_method = random.choice(payment_methods)
            shipping_country = random.choice(countries)

            days_offset = random.randint(0, 730)
            created_at = base_date + timedelta(days=days_offset)
            updated_at = created_at + timedelta(hours=random.randint(1, 48))

            if status in ['shipped', 'delivered']:
                shipped_at = updated_at + timedelta(hours=random.randint(1, 24))
                delivered_at = shipped_at + timedelta(days=random.randint(2, 7)) if status == 'delivered' else 'NULL'
                shipped_at_str = f"'{shipped_at}'"
                delivered_at_str = f"'{delivered_at}'" if delivered_at != 'NULL' else 'NULL'
            else:
                shipped_at_str = 'NULL'
                delivered_at_str = 'NULL'

            values.append(
                f"({user_id}, '{order_number}', {subtotal}, {tax}, {shipping}, {total}, "
                f"'{status}', '{payment_method}', '{shipping_country}', "
                f"'{created_at}', '{updated_at}', {shipped_at_str}, {delivered_at_str})"
            )

        query = f"""
            INSERT INTO orders (user_id, order_number, subtotal, tax, shipping, total,
                              status, payment_method, shipping_country, created_at,
                              updated_at, shipped_at, delivered_at)
            VALUES {','.join(values)}
        """
        cursor.execute(query)

        if batch_end % 50000 == 0 or batch_end == num_orders:
            print(f"  Inserted {batch_end:,}/{num_orders:,} orders...")
            conn.commit()

    print(f"[OK] Orders table populated with {num_orders:,} records")


def populate_order_items(conn, avg_items_per_order=3):
    """Populate order_items table based on existing orders"""
    cursor = conn.cursor()

    print(f"\nPopulating order_items table (avg {avg_items_per_order} items per order)...")

    # Get counts
    cursor.execute("SELECT COUNT(*) FROM orders")
    num_orders = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(id) FROM products")
    max_product_id = cursor.fetchone()[0]

    # Process orders in batches
    batch_size = 1000
    total_items = 0

    cursor.execute("SELECT id FROM orders ORDER BY id")
    order_ids = [row[0] for row in cursor.fetchall()]

    for batch_start in range(0, len(order_ids), batch_size):
        batch_end = min(batch_start + batch_size, len(order_ids))
        batch_order_ids = order_ids[batch_start:batch_end]
        values = []

        for order_id in batch_order_ids:
            # Random number of items (1-7, with avg around 3)
            num_items = random.choices([1, 2, 3, 4, 5, 6, 7], weights=[10, 25, 30, 20, 10, 3, 2])[0]

            for _ in range(num_items):
                product_id = random.randint(1, max_product_id)
                quantity = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
                unit_price = round(random.uniform(10.0, 200.0), 2)
                discount = random.choices([0, 5, 10, 15, 20], weights=[60, 20, 10, 7, 3])[0]
                total_price = round(unit_price * quantity * (1 - discount/100), 2)

                values.append(
                    f"({order_id}, {product_id}, {quantity}, {unit_price}, "
                    f"{discount}, {total_price})"
                )
                total_items += 1

        if values:
            query = f"""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price,
                                       discount_percent, total_price)
                VALUES {','.join(values)}
            """
            cursor.execute(query)

        if batch_end % 50000 == 0 or batch_end == len(order_ids):
            print(f"  Processed {batch_end:,}/{len(order_ids):,} orders, {total_items:,} items created...")
            conn.commit()

    print(f"[OK] Order items populated with {total_items:,} records")


def populate_reviews(conn, num_reviews=100000):
    """Populate reviews table"""
    cursor = conn.cursor()

    print(f"\nPopulating reviews table with {num_reviews:,} records...")

    cursor.execute("SELECT MAX(id) FROM users")
    max_user_id = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(id) FROM products")
    max_product_id = cursor.fetchone()[0]

    batch_size = 5000
    base_date = datetime.now() - timedelta(days=365*2)

    for batch_start in range(0, num_reviews, batch_size):
        batch_end = min(batch_start + batch_size, num_reviews)
        values = []

        for i in range(batch_start, batch_end):
            product_id = random.randint(1, max_product_id)
            user_id = random.randint(1, max_user_id)

            # Rating distribution (skewed toward positive)
            rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 30, 42])[0]

            title = f"Review title {i}"
            content = f"This is a review content for review {i}. " * random.randint(1, 5)
            helpful_count = random.randint(0, 100)
            verified = random.choice([True, True, False])  # 67% verified

            days_offset = random.randint(0, 730)
            created_at = base_date + timedelta(days=days_offset)

            values.append(
                f"({product_id}, {user_id}, {rating}, '{title}', '{content}', "
                f"{helpful_count}, {verified}, '{created_at}')"
            )

        query = f"""
            INSERT INTO reviews (product_id, user_id, rating, title, content,
                               helpful_count, verified_purchase, created_at)
            VALUES {','.join(values)}
        """
        cursor.execute(query)

        if batch_end % 25000 == 0 or batch_end == num_reviews:
            print(f"  Inserted {batch_end:,}/{num_reviews:,} reviews...")
            conn.commit()

    print(f"[OK] Reviews populated with {num_reviews:,} records")


def populate_user_sessions(conn, num_sessions=200000):
    """Populate user sessions table"""
    cursor = conn.cursor()

    print(f"\nPopulating user_sessions table with {num_sessions:,} records...")

    cursor.execute("SELECT MAX(id) FROM users")
    max_user_id = cursor.fetchone()[0]

    batch_size = 5000
    base_date = datetime.now() - timedelta(days=90)

    for batch_start in range(0, num_sessions, batch_size):
        batch_end = min(batch_start + batch_size, num_sessions)
        values = []

        for i in range(batch_start, batch_end):
            user_id = random.randint(1, max_user_id)
            token = ''.join(random.choices(string.ascii_letters + string.digits, k=64))
            ip = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"
            user_agent = f"Mozilla/5.0 (Platform {i % 100})"

            days_offset = random.randint(0, 90)
            created_at = base_date + timedelta(days=days_offset)
            expires_at = created_at + timedelta(days=30)
            last_activity = created_at + timedelta(minutes=random.randint(1, 10000))

            values.append(
                f"({user_id}, '{token}', '{ip}', '{user_agent}', "
                f"'{created_at}', '{expires_at}', '{last_activity}')"
            )

        query = f"""
            INSERT INTO user_sessions (user_id, session_token, ip_address, user_agent,
                                     created_at, expires_at, last_activity)
            VALUES {','.join(values)}
        """
        cursor.execute(query)

        if batch_end % 25000 == 0 or batch_end == num_sessions:
            print(f"  Inserted {batch_end:,}/{num_sessions:,} sessions...")
            conn.commit()

    print(f"[OK] User sessions populated with {num_sessions:,} records")


def create_minimal_indexes(conn):
    """Create only primary keys and foreign keys for testing"""
    cursor = conn.cursor()

    print("\nCreating minimal indexes...")

    # Only create foreign key indexes for joins
    cursor.execute("CREATE INDEX idx_products_category ON products(category_id)")
    cursor.execute("CREATE INDEX idx_orders_user ON orders(user_id)")
    cursor.execute("CREATE INDEX idx_order_items_order ON order_items(order_id)")
    cursor.execute("CREATE INDEX idx_order_items_product ON order_items(product_id)")
    cursor.execute("CREATE INDEX idx_reviews_product ON reviews(product_id)")
    cursor.execute("CREATE INDEX idx_reviews_user ON reviews(user_id)")

    conn.commit()
    print("[OK] Minimal indexes created")


def analyze_tables(conn):
    """Run ANALYZE on all tables for accurate statistics"""
    cursor = conn.cursor()

    print("\nAnalyzing tables for query planner statistics...")

    tables = ['users', 'categories', 'products', 'orders', 'order_items',
              'reviews', 'user_sessions', 'audit_logs']

    for table in tables:
        cursor.execute(f"ANALYZE {table}")

    conn.commit()
    print("[OK] All tables analyzed")


def print_database_stats(conn):
    """Print comprehensive database statistics"""
    cursor = conn.cursor()

    print("\n" + "="*70)
    print(" " * 20 + "DATABASE STATISTICS")
    print("="*70)

    tables = ['users', 'categories', 'products', 'orders',
              'order_items', 'reviews', 'user_sessions', 'audit_logs']

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table}'))
        """)
        size = cursor.fetchone()[0]

        print(f"{table.upper():20} {count:>12,} rows    {size:>10}")

    print("\nIndexes:")
    cursor.execute("""
        SELECT
            tablename,
            indexname,
            pg_size_pretty(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(indexname))::regclass) as size
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
    """)

    current_table = None
    for row in cursor.fetchall():
        table, index, size = row
        if table != current_table:
            print(f"\n  {table}:")
            current_table = table
        print(f"    {index:40} {size:>10}")

    print("\n" + "="*70)


def print_example_queries():
    """Print example queries to test the optimizer"""
    print("\n" + "="*70)
    print(" " * 20 + "EXAMPLE TEST QUERIES")
    print("="*70)

    queries = [
        ("Find user by email (equality predicate)",
         "SELECT * FROM users WHERE email = 'user50000@example5.com'"),

        ("Find active premium users (composite index)",
         "SELECT * FROM users WHERE status = 'active' AND subscription_tier = 'premium'"),

        ("Recent orders (ORDER BY with range)",
         "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '30 days' ORDER BY created_at DESC LIMIT 100"),

        ("Pending orders (partial index opportunity)",
         "SELECT * FROM orders WHERE status = 'pending'"),

        ("Product search by category (JOIN)",
         "SELECT p.*, c.name as category FROM products p JOIN categories c ON p.category_id = c.id WHERE c.name = 'Electronics'"),

        ("Order details with user info (covering index)",
         "SELECT user_id, total, status FROM orders WHERE status = 'delivered' AND total > 100"),

        ("Top rated products (composite index)",
         "SELECT * FROM products WHERE rating > 4.5 AND is_active = true ORDER BY review_count DESC LIMIT 50"),

        ("User order history (JOIN with range)",
         "SELECT u.email, o.order_number, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.created_at > '2024-01-01'"),

        ("Product reviews aggregation",
         "SELECT product_id, AVG(rating), COUNT(*) FROM reviews WHERE verified_purchase = true GROUP BY product_id HAVING COUNT(*) > 10"),

        ("Active sessions (time-based partial index)",
         "SELECT * FROM user_sessions WHERE expires_at > NOW() AND last_activity > NOW() - INTERVAL '1 hour'"),
    ]

    for i, (description, query) in enumerate(queries, 1):
        print(f"\n{i}. {description}")
        print(f"   {query}")

    print("\n" + "="*70)
    print("\nYou can test these queries with:")
    print("  python scripts/analyse_cli.py \"<query>\"")
    print("Or use the batch analyzer:")
    print("  python scripts/batch_analyse.py")
    print("="*70 + "\n")


def main():
    """Main setup function"""
    print("\n" + "="*70)
    print(" " * 10 + "PostgreSQL Performance Analyzer - Test Database Setup")
    print("="*70 + "\n")

    conn = create_connection()

    try:
        # Setup extensions
        setup_extensions(conn)

        # Setup optimizer metadata schema
        setup_optimizer_metadata(conn)

        # Setup tables
        setup_tables(conn)

        # Populate reference data
        populate_categories(conn)

        # Populate main tables
        populate_users(conn, num_users=500000)
        populate_products(conn, num_products=50000)
        populate_orders(conn, num_orders=1000000)
        populate_order_items(conn, avg_items_per_order=3)
        populate_reviews(conn, num_reviews=100000)
        populate_user_sessions(conn, num_sessions=200000)

        # Create minimal indexes
        create_minimal_indexes(conn)

        # Analyze all tables
        analyze_tables(conn)

        # Print statistics
        print_database_stats(conn)

        # Print example queries
        print_example_queries()

        print("\n[SUCCESS] Database setup complete!")
        print("          The database is ready for performance testing.\n")

    except Exception as e:
        print(f"\n[ERROR] Setup failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
