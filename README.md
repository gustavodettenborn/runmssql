# runmssql

docker-compose down

docker-compose up --build -d mssql-client

docker-compose exec mssql-client python3 run_sql_csv.py
