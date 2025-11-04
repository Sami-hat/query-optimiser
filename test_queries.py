from db_connector import PostgresAnalyser

def run_test_queries():
    analyser = PostgresAnalyser()
    
    queries = [
        # Query 1: WHERE with equality
        ("WHERE equality", 
         "SELECT * FROM users WHERE email = 'user50000@example.com'"),
        
        # Query 2: WHERE with pattern matching
        ("LIKE pattern", 
         "SELECT * FROM users WHERE email LIKE 'user1%'"),
        
        # Query 3: ORDER BY
        ("ORDER BY", 
         "SELECT * FROM users ORDER BY created_at DESC LIMIT 10"),
        
        # Query 4: Range query
        ("Range query", 
         "SELECT * FROM users WHERE id BETWEEN 10000 AND 20000"),
        
        # Query 5: COUNT aggregate
        ("COUNT aggregate", 
         "SELECT COUNT(*) FROM users WHERE name LIKE 'User 5%'")
    ]
    
    for name, query in queries:
        print(f"\n{'='*60}")
        print(f"Test: {name}")
        print(f"Query: {query}")
        print('='*60)
        
        plan = analyser.get_explain_plan(query)
        exec_time = analyser.get_execution_time(plan)
        scans = analyser.find_sequential_scans(plan)
        
        print(f"Execution Time: {exec_time:.2f}ms")
        
        if scans:
            print("\nSequential Scans:")
            for scan in scans:
                print(f"  Table: {scan['table']}")
                print(f"  Rows Scanned: {scan['rows']}")
                print(f"  Scan Time: {scan['time']:.2f}ms")
                print(f"  Cost: {scan['cost']:.2f}")
        else:
            print("No sequential scans (using index)")
    
    analyser.close()

if __name__ == '__main__':
    run_test_queries()
    
"""
Findings:

The range query on id runs in 1.31ms using the primary key index
All other queries take 36-47ms with sequential scans. The index gives 30x speed improvement

Missing indexes identified:
- email column: 44.63ms for equality, 36.78ms for LIKE
- created_at column: 47.30ms for ORDER BY
- name column: 42.07ms for pattern matching
"""