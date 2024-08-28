### Execution Steps

- First start the celery worker
`celery -A tasks worker --loglevel=info --concurrency=2 -n worker4@%h -P solo`
- secondly run the celery beat scheduler
`celery -A tasks  beat --loglevel=info`
- And to export data onto fastapi use below command 
`uvicorn main:f_api --reload` 
