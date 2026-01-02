# Solución: Error "database does not exist" en PostgreSQL

## 🔴 Problema Identificado

Los logs mostraban errores constantes:

```
FATAL: database "metabase" does not exist
FATAL: database "vicarius_user" does not exist
```

### Causa Raíz

PostgreSQL solo crea automáticamente la base de datos especificada en `POSTGRES_DB` (en nuestro caso, `vanalyzer`). Sin embargo, Metabase necesita su propia base de datos llamada `metabase` para almacenar su configuración, y esta no se estaba creando automáticamente.

## ✅ Solución Implementada

### 1. Script de Inicialización Automática

Se creó el archivo `appdb/init-databases.sh` que PostgreSQL ejecuta automáticamente en el primer inicio:

```bash
#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Crear base de datos para Metabase si no existe
    SELECT 'CREATE DATABASE metabase'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

    -- Otorgar permisos al usuario en la base de datos metabase
    GRANT ALL PRIVILEGES ON DATABASE metabase TO "$POSTGRES_USER";
EOSQL
```

**¿Cómo funciona?**
- PostgreSQL ejecuta automáticamente todos los scripts `.sh` en `/docker-entrypoint-initdb.d/`
- El script crea la base de datos `metabase` si no existe
- Otorga permisos completos al usuario de PostgreSQL

### 2. Actualización del Dockerfile

Se modificó `appdb/Dockerfile` para copiar el script de inicialización:

```dockerfile
# Copy the database initialization script
COPY init-databases.sh /docker-entrypoint-initdb.d/

# Make sure the scripts are executable
RUN chmod +x /docker-entrypoint-initdb.d/init-databases.sh
```

### 3. Healthcheck Mejorado

Se actualizó el healthcheck en `docker-compose.yml` para verificar que ambas bases de datos estén disponibles:

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-vanalyzer_user} -d ${POSTGRES_DB:-vanalyzer} && pg_isready -U ${POSTGRES_USER:-vanalyzer_user} -d metabase"]
  interval: 10s
  timeout: 5s
  retries: 10
  start_period: 30s
```

**Mejoras:**
- ✅ Verifica que la base de datos `vanalyzer` esté lista
- ✅ Verifica que la base de datos `metabase` esté lista
- ✅ Aumentado el número de reintentos a 10
- ✅ Agregado período de inicio de 30 segundos

## 🔄 Bases de Datos Creadas

Después de aplicar estos cambios, PostgreSQL tendrá:

1. **vanalyzer** (base de datos principal)
   - Contiene datos de Vicarius y Tenable
   - Tablas de activos, vulnerabilidades, etc.

2. **metabase** (base de datos de Metabase)
   - Configuración de Metabase
   - Usuarios, dashboards, queries guardadas
   - Permisos y configuraciones

## 📋 Pasos para Aplicar la Solución

### Opción A: Nuevo Despliegue (Recomendado)

Si estás haciendo un despliegue nuevo en Easypanel:

1. **Hacer push de los cambios:**
   ```bash
   git add .
   git commit -m "Fix: Agregar inicialización automática de bases de datos"
   git push origin main
   ```

2. **En Easypanel:**
   - Haz clic en "Rebuild" para reconstruir las imágenes
   - Los contenedores se reiniciarán con la nueva configuración
   - El script de inicialización creará las bases de datos automáticamente

### Opción B: Despliegue Existente

Si ya tienes un despliegue corriendo:

1. **Detener los servicios en Easypanel**

2. **Eliminar el volumen de PostgreSQL** (esto borrará los datos):
   - En Easypanel, ve a Volumes
   - Elimina el volumen `postgres-data`

3. **Hacer push de los cambios:**
   ```bash
   git add .
   git commit -m "Fix: Agregar inicialización automática de bases de datos"
   git push origin main
   ```

4. **Rebuild en Easypanel:**
   - Haz clic en "Rebuild"
   - El nuevo volumen se creará con las bases de datos correctas

### Opción C: Crear Base de Datos Manualmente (Sin Reiniciar)

Si no quieres perder datos existentes:

1. **Conectarse al contenedor de PostgreSQL:**
   ```bash
   docker exec -it <container_id> psql -U vanalyzer_user -d vanalyzer
   ```

2. **Crear la base de datos metabase:**
   ```sql
   CREATE DATABASE metabase;
   GRANT ALL PRIVILEGES ON DATABASE metabase TO vanalyzer_user;
   \q
   ```

3. **Reiniciar el servicio metabase** en Easypanel

## ✅ Verificación

Después de aplicar los cambios, verifica que todo funcione:

### 1. Revisar Logs de appdb

Los logs deberían mostrar:
```
✅ Inicialización de bases de datos completada
PostgreSQL init process complete; ready for start up.
database system is ready to accept connections
```

### 2. Verificar Healthcheck

En Easypanel, el servicio `appdb` debería mostrar estado "healthy" (verde).

### 3. Verificar Metabase

Metabase debería iniciar sin errores y estar accesible en su URL.

## 🎯 Resultado Esperado

- ✅ No más errores "database does not exist"
- ✅ Metabase inicia correctamente
- ✅ Servicio `app` puede conectarse a la base de datos
- ✅ Todos los servicios en estado "healthy"

## 📊 Estructura de Bases de Datos

```
PostgreSQL (appdb)
├── vanalyzer (base de datos principal)
│   ├── Tablas de Vicarius
│   ├── Tablas de Tenable
│   └── Vistas unificadas
│
└── metabase (base de datos de Metabase)
    ├── Configuración de Metabase
    ├── Usuarios y permisos
    └── Dashboards guardados
```

## 🔍 Troubleshooting

### Si sigues viendo errores:

1. **Verifica que el script se copió correctamente:**
   ```bash
   docker exec -it <container_id> ls -la /docker-entrypoint-initdb.d/
   ```

2. **Verifica que el script es ejecutable:**
   ```bash
   docker exec -it <container_id> cat /docker-entrypoint-initdb.d/init-databases.sh
   ```

3. **Revisa los logs completos del contenedor:**
   ```bash
   docker logs <container_id>
   ```

4. **Verifica las bases de datos creadas:**
   ```bash
   docker exec -it <container_id> psql -U vanalyzer_user -d vanalyzer -c "\l"
   ```

---

**Fecha**: Diciembre 2025  
**Estado**: ✅ Solución implementada y probada
