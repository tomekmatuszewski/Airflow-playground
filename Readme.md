To run: docker-compose up --build

Playing with Airflow -> orchestrate simply ETL job

    - pull data from Spotify API (EXTRACT)
    - transform data (TRANSFORM)
    - load data to sqlite3 database (LOAD)
    - last task is printng final statement to logs

to run job creation of .env file/ exporting env variables/ adding them
to docker-compose path (--env-file) is needed ->
variables URL and TOKEN (credentials to Spotify API)

