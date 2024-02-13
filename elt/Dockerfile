# indicates the Python version to be installed for ELT script
FROM python:3.8-slim

# install and update postgres client and cron for cron job
RUN apt-get update && apt-get install -y postgresql-client cron

# copies the start bash script to app folder in Docker container
COPY start.sh /app/start.sh
# copies the ELT script to Docker
COPY elt_script.py .
# indicates working directory for the following RUN and Python execution command
WORKDIR /app

RUN echo "0 3 * * * python /app/elt_script.py" | crontab -

CMD ["python", "elt_script.py"]