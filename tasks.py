from celery import Celery
from datetime import datetime
import uuid
import sqlalchemy as sa
import psycopg2
from  testapiconn import execute_task
from celery.schedules import crontab



print('in celery instance')
app = Celery('tasks', broker='redis://localhost:6379/0', backend='db+postgresql://postgres:root@localhost/postgres')


print('in beat schedule')
app.conf.beat_schedule = {
    'run-scheduler-daily': {
        'task': 'tasks.schedule_task',
        'schedule': crontab(minute=0, hour=0) # 86400
    }
    ,
    
    'run-executor-every-5-minutes': {
        'task': 'tasks.run_executor',
        'schedule': crontab(minute=5, hour=0) # 300
    }
}


@app.task
def schedule_task():
    print("Database access successful")
    conn = psycopg2.connect(user="postgres",
                                  password="root",
                                  database="postgres")
    cur = conn.cursor()
    run_id = str(uuid.uuid4())
    today = datetime.now().date()

    print("In insert query")
    cur.execute("""
        INSERT INTO tasks (run_id, date, status)
        VALUES (%s, %s, %s)
    """, (run_id, today, 'SCHEDULED'))
    conn.commit()
    cur.close()
    conn.close()

@app.task
def run_executor():
    print('in execute task')
    execute_task()
