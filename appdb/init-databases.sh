#!/bin/bash
set -e

# Este script se ejecuta automáticamente cuando PostgreSQL se inicia por primera vez
# Crea las bases de datos necesarias para vAnalyzer y Metabase

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Crear base de datos para Metabase si no existe
    SELECT 'CREATE DATABASE metabase'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

    -- Otorgar permisos al usuario en la base de datos metabase
    GRANT ALL PRIVILEGES ON DATABASE metabase TO "$POSTGRES_USER";

    -- Mensaje de confirmación
    \echo 'Bases de datos inicializadas correctamente'
EOSQL

echo "✅ Inicialización de bases de datos completada"
