#!/bin/bash
# Script para crear la base de datos metabase si no existe
# Ejecutar este script en el contenedor appdb

echo "🔍 Verificando base de datos metabase..."

# Verificar si la base de datos metabase existe
DB_EXISTS=$(psql -U "${POSTGRES_USER:-vanalyzer_user}" -d "${POSTGRES_DB:-vanalyzer}" -tAc "SELECT 1 FROM pg_database WHERE datname='metabase'")

if [ "$DB_EXISTS" = "1" ]; then
    echo "✅ La base de datos 'metabase' ya existe"
else
    echo "📝 Creando base de datos 'metabase'..."
    psql -U "${POSTGRES_USER:-vanalyzer_user}" -d "${POSTGRES_DB:-vanalyzer}" -c "CREATE DATABASE metabase;"
    psql -U "${POSTGRES_USER:-vanalyzer_user}" -d "${POSTGRES_DB:-vanalyzer}" -c "GRANT ALL PRIVILEGES ON DATABASE metabase TO ${POSTGRES_USER:-vanalyzer_user};"
    echo "✅ Base de datos 'metabase' creada exitosamente"
fi

# Listar todas las bases de datos
echo ""
echo "📊 Bases de datos disponibles:"
psql -U "${POSTGRES_USER:-vanalyzer_user}" -d "${POSTGRES_DB:-vanalyzer}" -c "\l"
