docker run --name my_postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres

PGPASSWORD=postgres psql -h localhost -U postgres -d postgres -a -f DELETE_TABLE.sql


