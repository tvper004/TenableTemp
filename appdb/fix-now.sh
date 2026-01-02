#!/bin/bash
# Script de solución inmediata para ejecutar en la consola de appdb
# Copia y pega TODO este script en la consola

echo "🔧 Solucionando configuración de PostgreSQL..."

# Ejecutar como usuario postgres (que siempre existe)
psql -U postgres -d postgres <<'EOSQL'
-- Crear usuario vanalyzer_user si no existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'vanalyzer_user') THEN
        CREATE USER vanalyzer_user WITH PASSWORD 'CAMBIAR_ESTA_CONTRASEÑA';
        ALTER USER vanalyzer_user WITH SUPERUSER;
        RAISE NOTICE '✅ Usuario vanalyzer_user creado';
    ELSE
        RAISE NOTICE '✅ Usuario vanalyzer_user ya existe';
    END IF;
END
$$;

-- Crear base de datos vanalyzer si no existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'vanalyzer') THEN
        CREATE DATABASE vanalyzer OWNER vanalyzer_user;
        RAISE NOTICE '✅ Base de datos vanalyzer creada';
    ELSE
        RAISE NOTICE '✅ Base de datos vanalyzer ya existe';
    END IF;
END
$$;

-- Crear base de datos metabase si no existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase') THEN
        CREATE DATABASE metabase OWNER vanalyzer_user;
        RAISE NOTICE '✅ Base de datos metabase creada';
    ELSE
        RAISE NOTICE '✅ Base de datos metabase ya existe';
    END IF;
END
$$;

-- Otorgar permisos
GRANT ALL PRIVILEGES ON DATABASE vanalyzer TO vanalyzer_user;
GRANT ALL PRIVILEGES ON DATABASE metabase TO vanalyzer_user;

-- Mostrar bases de datos
\l
EOSQL

echo ""
echo "✅ ¡Configuración completada!"
echo "📊 Ahora reinicia el servicio metabase en Easypanel"
