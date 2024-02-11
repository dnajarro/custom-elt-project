# stuff for cron job
# cron &

# python /app/elt_script.py
# ----------------------------
docker compose up init airflow

sleep 5

docker compose up -d

sleep 5

cd airbyte

if [ -f  "docker-compose.yaml" ]; then
    docker compose up -d
else
    ./run-ab-platform.sh
fi
