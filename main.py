from fastapi import FastAPI
import psycopg2

f_api = FastAPI()


def get_db_connection():
    return psycopg2.connect(user="postgres",
                            password="root",
                            database="postgres")

@f_api.get("/tasks/") 
def get_tasks(date: str = None):
    conn = get_db_connection()
    cur = conn.cursor()
    if date:
        cur.execute("SELECT * FROM tasks WHERE date = %s", (date,))
    else:
        cur.execute("SELECT * FROM tasks")
    tasks = cur.fetchall()
    cur.close()
    conn.close()
    return {"tasks": tasks}

@f_api.get("/legitimate_sellers/")
def get_legitimate_sellers(domain: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM test WHERE site = %s", (domain,))
    sellers = cur.fetchall()
    cur.close()
    conn.close()
    return {"sellers": sellers}

@f_api.get("/stats") 
def get_stats(from_date: str, to_date: str):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) 
        FROM tasks 
        WHERE date BETWEEN %s AND %s AND status = 'FINISHED'
    """, (from_date, to_date))
    
    avg_execution_time = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    return {"average_execution_time": avg_execution_time}
