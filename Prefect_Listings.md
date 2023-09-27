Das im Artikel verwendete und abgewandelte YAML-File für Docker Compose findet sich unter:
https://github.com/rpeden/prefect-docker-compose/blob/main/docker-compose.yml

# Listing 1: Erster Beispiel-Flow first_flow.pyfrom prefect import flow, task
``` python
@task(name='hello')
def say_hello(name):
   print(f"hello {name}")

@task(name='goodbye')
def say_goodbye(name):
   print(f"goodbye {name}")

@flow(name="greetings")
def greetings(names=["arthur", "trillian", "ford", "marvin"]):
   for name in names:
       say_hello(name)
       say_goodbye(name)

if __name__ == "__main__":
   greetings()
```

# Listing 2: Im Docker-File das Prefect-Image erweitern
```
FROM prefecthq/prefect:2.10-python3.10

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN pip install --upgrade pip
```

# Listing 3: Daten auslesen und ablegen
``` python
@flow(name='ix_etl_pipeline', description='Prefect ETL flow', retries=2, retry_delay_seconds=60, version='2.0')
def prefect_flow(url: str ='https://jsonplaceholder.typicode.com/users') -> None:
   users = extract(url)
   df_users = transform(users)
   load(data=df_users)

def main() -> None:
   prefect_flow()


if __name__ == '__main__':
   main()
```

# Listing 4: Auslesen auf Task-Ebene
``` python
@task(name='extract_task', description='Request data from website.')
def extract(url: str) -> dict:
   logger = get_run_logger()

   res = requests.get(url)

   if not res:
       raise Exception('No data fetched!')

   logger.info(json.loads(res.content))

   return json.loads(res.content)
```

# Listing 5: Datentransformation
``` python
@task(name='transform_task', description='Transform JSON to Dataframe.')
def transform(data: dict) -> pd.DataFrame:
   transformed = []

   for user in data:
       transformed.append({
           'ID': user['id'],
           'Name': user['name'],
           'Username': user['username'],
           'Email': user['email'],
           'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
           'PhoneNumber': user['phone'],
           'Company': user['company']['name']
       })

   logger = get_run_logger()
   logger.info(transformed)

   return pd.DataFrame(transformed)
```

# Listing 6: Daten in DB speichern
``` python
@task(name='load_task', description='Saves file to database.')
def load(data: pd.DataFrame) -> None:
   secret_block = Secret.load("pg-connection-string")
   # Access the stored secret
   conn_string = secret_block.get()
   db = create_engine(conn_string)

   logger = get_run_logger()

   with db.connect() as conn:
       data.to_sql('account', con=conn, if_exists='append', index=False)
       logger.info('Data successfully saved to DB.')
```

# Listing 7: MinIO-Speicherplatz erstellen
``` python
       minio_client = Minio(
       "localhost:9000",
       access_key="minioadmin",
       secret_key="minioadmin",
       secure=False
   )

   # Create bucket, if it not already exists
   if not minio_client.bucket_exists("ix-flows"):
       minio_client.make_bucket("ix-flows")
```

# Listing 8: Referenzieren des MinIO-Basepath
``` python
       minio_block = RemoteFileSystem(
       basepath=f"s3://ix-flows",
       settings={
           "key": "minioadmin",
           "secret": "minioadmin",
           "client_kwargs": {
               "endpoint_url": "http://localhost:9000"
           }
       },
       validate=False
   )

   minio_block.save("ix-fs-block", overwrite=True)
```

# Listing 9: Timing mit RRule
``` python
   def create_flow_schedule() -> str:
   now = datetime.now()
   # add 5 minutes
   now = now + timedelta(minutes=5)

   # convert to string
   now_parts_all = now.strftime("%Y %m %d %H %M")
   now_parts = now_parts_all.split(" ")
   now_parts = [int(part) for part in now_parts]

   bus_hours = rrule(
       MINUTELY,
       interval=5,
       count=6,
       dtstart=pendulum.datetime(now_parts[0], now_parts[1], now_parts[2], now_parts[3], now_parts[4])
   )

   return bus_hours.__str__()
```

# Listing 10: Steuerung von Prefect
``` python
   command_1 = 'prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api'
   command_2 = f'prefect deployment build -n {queue_name} -q {queue_name} "{flow_name}.py:{flow_name}" ' f'--rrule "{flow_schedule}" -sb "remote-file-system/ix-fs-block"'
   command_3 = f'prefect deployment apply {flow_name}-deployment.yaml'
   command_4 = f'nohup prefect agent start -q "{queue_name}" &'
   commands = [command_1, command_2, command_3, command_4]

   for command in commands:
       stream = os.popen(command)
       output = stream.read()
       print(output)
```
