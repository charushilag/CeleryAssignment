import requests
import pandas as pd
import io
from datetime import date,datetime
import psycopg2
from psycopg2.extras  import execute_values
import psycopg2.extras as extras
import json
# for line in response.text.strip().split('\n'):
#         fields = line.split(',')
#         if len(fields) >= 3:
def execute_task():
    connection = psycopg2.connect(user="postgres",
                                        password="root",
                                        database="postgres")
    cur = connection.cursor()

    cur.execute("SELECT * FROM tasks WHERE status = 'SCHEDULED'")
    tasks = cur.fetchall()
    print(f'tasks : {tasks}')

    for task in tasks:
        print(task)
        run_id = task[0]

        # Mark as STARTED
        cur.execute("""
            UPDATE tasks
            SET status = 'STARTED', started_at = %s
            WHERE run_id = %s
        """, (datetime.now(), run_id))
        connection.commit()

        with open('sites.json', 'r') as file:
            json_data = json.load(file)
            print(json_data)
            for i in json_data['sites']:
                print(i)
                domain_name = i
                # df['site'] = domain_name
                api_url = "https://"+domain_name+"/ads.txt"
                response = requests.get(api_url)
                print(response.status_code)
                print(response.headers.get('Content-Type'))
                data=response.text
                for line in data.strip().split('\n'):
                    print(f'liine = {line}')
                    fields = line.split(',')
                    print(f'field = {fields}')
                    print(len(fields))
                    if len(fields) <= 4 and len(fields)>=2:
                        with open('good_data.csv', 'a+') as gd:
                            gd.writelines(line)
                    # ads_txt = io.StringIO(line)
                    #print(data)
                df = pd.read_csv('good_data.csv', header=None, names=['ssp_domain_name', 'publisher_id','relationship', 'run_id'] ,on_bad_lines='warn')
                df['site'] = domain_name
                df['date'] = date.today()
                df=df[['site','ssp_domain_name','publisher_id','relationship','date','run_id']]
                print(df.shape[0])
                #print(df.to_numpy())

                try:
                    # connection = psycopg2.connect(user="postgres",
                    #                             password="root",
                    #                             database="postgres")
                    # cursor = connection.cursor()
                    tuples = [tuple(x) for x in df.to_numpy()] 
                    #print (tuples)
                    cols = ','.join(list(df.columns)) 
                    print(cols)
                    query = "INSERT INTO %s(%s) VALUES %%s" % ('test', cols) 
                    extras.execute_values(cur, query, tuples) 
                    connection.commit() 
                    count = cur.rowcount
                    print(count, "Record inserted successfully into db table")  

                    # Mark as FINISHED
                    cur.execute("""
                        UPDATE tasks
                        SET status = 'FINISHED', finished_at = %s
                        WHERE run_id = %s
                    """, (datetime.now(), run_id))
                    connection.commit()
    

                #     postgres_insert_query = """ INSERT INTO legitimate_seller (ssp_domain_name,publisher_id,relationship,run_id) VALUES (%s)"""
                #     data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
                #     execute_values(cursor, postgres_insert_query, data_tuples)
                #     connection.commit()
                #     count = cursor.rowcount
                #     print(count, "Record inserted successfully into db table")

                except (Exception, psycopg2.Error) as error:
                    # Mark as FAILED
                    cur.execute("""
                        UPDATE tasks
                        SET status = 'FAILED', error = %s, failed_at = %s
                        WHERE run_id = %s
                    """, (str(error), datetime.now(), run_id))
                    connection.commit()
                    print("Failed to insert record into db table", error)
                    
                    # cur.close()
                    # connection.close()
                    # print("PostgreSQL connection is closed") 
                  
            
    cur.close()
    connection.close()
    print("PostgreSQL connection is closed") 
