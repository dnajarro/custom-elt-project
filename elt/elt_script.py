import subprocess
import time


# Set up a connection to PostgreSQL
def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False


# connect to database
if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script...")

# define configuration parameters for connecting to source database
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    # use the service name from docker-compose.yaml as hostname
    'host': 'source_postgres'
}

# define configuration parameters for connecting to destination database
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    # use the service name from docker-compose.yaml as hostname
    'host': 'destination_postgres'
}

# uses pg_dump command to dump the data from source database to SQL file
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    # avoid being prompted for password every time
    '-w'
]

# set the PGPASSWORD environment variable to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config['password'])

# execute dump command
subprocess.run(dump_command, env=subprocess_env, check=True)

# use psql to load the dumped SQL file into the destination database
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

# set the PGPASSWORD environment variable for the destination database
subprocess_env = dict(PGPASSWORD=destination_config['password'])

# execute the load command
subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")
