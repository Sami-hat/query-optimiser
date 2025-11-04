import psycopg2
import json
from typing import Dict, List, Tuple
from dotenv import load_dotenv
import os

load_dotenv()

class PostgresAnalyser:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        self.cursor = self.conn.cursor()
    
    def get_explain_plan(self, query: str) -> Dict:
        """Execute EXPLAIN ANALYZE and return JSON plan"""
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        self.cursor.execute(explain_query)
        result = self.cursor.fetchone()
        return result[0][0]
    
    def get_execution_time(self, plan: Dict) -> float:
        """Extract total execution time in milliseconds"""
        return plan['Execution Time']
    
    def find_sequential_scans(self, plan: Dict) -> List[Dict]:
        """Recursively find all Seq Scan nodes"""
        scans = []
        
        def traverse(node):
            if node.get('Node Type') == 'Seq Scan':
                scans.append({
                    'table': node.get('Relation Name'),
                    'rows': node.get('Actual Rows'),
                    'time': node.get('Actual Total Time'),
                    'cost': node.get('Total Cost')
                })
            
            if 'Plans' in node:
                for child in node['Plans']:
                    traverse(child)
        
        traverse(plan['Plan'])
        return scans
    
    def close(self):
        self.cursor.close()
        self.conn.close()