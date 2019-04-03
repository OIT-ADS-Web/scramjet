#!/bin/bash
echo "trying to run init test db script"
set -e
echo $DB_USER
echo $DB_DATABASE

psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "$DB_DATABASE" <<-EOSQL
    CREATE USER docker WITH PASSWORD 'docker';
    CREATE DATABASE docker;
    GRANT ALL PRIVILEGES ON DATABASE docker TO docker;
EOSQL