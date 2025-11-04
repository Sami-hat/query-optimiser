from db_connector import PostgresAnalyser

def test_connection():
    analyser = PostgresAnalyser()
    
    """Test SELECT query"""
    query = "SELECT * FROM users WHERE email = 'test@example.com'"
    
    plan = analyser.get_explain_plan(query)
    
    print("Execution Time:", analyser.get_execution_time(plan), "ms")
    print("\nSequential Scans Found:")
    
    scans = analyser.find_sequential_scans(plan)
    for scan in scans:
        print(f"  Table: {scan['table']}")
        print(f"  Rows: {scan['rows']}")
        print(f"  Time: {scan['time']:.2f}ms")
        print(f"  Cost: {scan['cost']:.2f}\n")
    
    analyser.close()

if __name__ == '__main__':
    test_connection()
    
"""
Findings:

Connection established, no records retrieved but attempt made
"""