# 🔐 Integración de Tenable.io en vAnalyzer

## 📋 Descripción

Este documento describe la integración completa de Tenable.io en vAnalyzer, permitiendo la sincronización de activos y vulnerabilidades desde la plataforma Tenable.io junto con los datos de Vicarius.

## ✨ Características

- ✅ Sincronización automática de activos desde Tenable.io
- ✅ Sincronización de vulnerabilidades detectadas por Tenable
- ✅ Vista unificada de activos de Vicarius y Tenable
- ✅ Soporte para UPSERT (evita duplicados)
- ✅ Índices optimizados para consultas rápidas
- ✅ Integración completa con Metabase para visualización

## 📊 Tablas Creadas

### 1. `tenable_assets`
Almacena información de activos/endpoints desde Tenable.io

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `asset_uuid` | VARCHAR(255) | ID único del activo (PK) |
| `hostname` | VARCHAR(255) | Nombre del host |
| `ip_address` | VARCHAR(50) | Dirección IP |
| `operating_system` | VARCHAR(255) | Sistema operativo |
| `last_seen` | VARCHAR(50) | Última vez visto |
| `created_at` | TIMESTAMP | Fecha de creación |
| `updated_at` | TIMESTAMP | Última actualización |

### 2. `tenable_vulnerabilities`
Almacena vulnerabilidades detectadas por Tenable.io

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id` | SERIAL | ID autoincremental (PK) |
| `asset_uuid` | VARCHAR(255) | Referencia al activo (FK) |
| `plugin_id` | VARCHAR(50) | ID del plugin de Tenable |
| `cve` | VARCHAR(50) | CVE asociado |
| `cvss` | FLOAT | Puntuación CVSS |
| `severity` | VARCHAR(20) | Nivel de severidad |
| `vulnerability_name` | TEXT | Nombre de la vulnerabilidad |
| `first_found` | VARCHAR(50) | Primera detección |
| `last_found` | VARCHAR(50) | Última detección |
| `state` | VARCHAR(50) | Estado de la vulnerabilidad |
| `created_at` | TIMESTAMP | Fecha de creación |
| `updated_at` | TIMESTAMP | Última actualización |

### 3. `unified_assets_view` (Vista)
Vista que combina activos de Vicarius y Tenable

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `unified_asset_id` | TEXT | ID unificado basado en hostname/IP |
| `source` | TEXT | Origen: 'Vicarius' o 'Tenable' |
| `source_id` | TEXT | ID original del activo |
| `hostname` | TEXT | Nombre del host |
| `ip_address` | TEXT | Dirección IP |
| `operating_system` | TEXT | Sistema operativo |
| `last_seen` | TEXT | Última vez visto |
| `created_at` | TIMESTAMP | Fecha de creación |
| `updated_at` | TIMESTAMP | Última actualización |

## 🔧 Configuración

### Variables de Entorno Requeridas

Agrega las siguientes variables a tu archivo `.env`:

```bash
# Tenable API Configuration
TENABLE_API_KEY=tu_access_key_aqui
TENABLE_SECRET_KEY=tu_secret_key_aqui
```

### Obtener Credenciales de Tenable

1. Inicia sesión en [Tenable.io](https://cloud.tenable.com/)
2. Ve a **Settings** → **My Account** → **API Keys**
3. Click en **Generate** para crear un nuevo par de claves
4. Copia el **Access Key** y **Secret Key**

> [!WARNING]
> Guarda las credenciales de forma segura. El Secret Key solo se muestra una vez.

## 🚀 Uso

### Ejecutar Sincronización Manual

```bash
# Ejecutar el reporte de Tenable
docker-compose exec app python /usr/src/app/scripts/VickyTopiaReportCLI.py --tenableReport
```

### Verificar Datos Sincronizados

```bash
# Conectarse a la base de datos
docker-compose exec appdb psql -U vicarius_user -d vanalyzer

# Contar activos de Tenable
SELECT COUNT(*) FROM tenable_assets;

# Contar vulnerabilidades de Tenable
SELECT COUNT(*) FROM tenable_vulnerabilities;

# Ver estadísticas de la vista unificada
SELECT source, COUNT(*) 
FROM unified_assets_view 
GROUP BY source;
```

## 📈 Consultas Útiles para Metabase

### Top 10 Activos con Más Vulnerabilidades

```sql
SELECT 
    ta.hostname,
    ta.ip_address,
    ta.operating_system,
    COUNT(tv.id) as vuln_count,
    AVG(tv.cvss) as avg_cvss
FROM tenable_assets ta
LEFT JOIN tenable_vulnerabilities tv ON ta.asset_uuid = tv.asset_uuid
GROUP BY ta.asset_uuid, ta.hostname, ta.ip_address, ta.operating_system
ORDER BY vuln_count DESC
LIMIT 10;
```

### Vulnerabilidades por Severidad

```sql
SELECT 
    severity,
    COUNT(*) as total_vulns,
    ROUND(AVG(cvss)::numeric, 2) as avg_cvss
FROM tenable_vulnerabilities
GROUP BY severity
ORDER BY 
    CASE severity
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        WHEN 'Low' THEN 4
        ELSE 5
    END;
```

### Activos Detectados por Ambas Plataformas

```sql
SELECT 
    unified_asset_id,
    STRING_AGG(DISTINCT source, ', ') as detected_by,
    COUNT(*) as detection_count
FROM unified_assets_view
GROUP BY unified_asset_id
HAVING COUNT(DISTINCT source) > 1
ORDER BY unified_asset_id;
```

### CVEs Críticos Abiertos

```sql
SELECT 
    tv.cve,
    tv.vulnerability_name,
    tv.cvss,
    COUNT(DISTINCT tv.asset_uuid) as affected_assets,
    ta.hostname
FROM tenable_vulnerabilities tv
JOIN tenable_assets ta ON tv.asset_uuid = ta.asset_uuid
WHERE tv.severity = 'Critical' 
  AND tv.state = 'OPEN'
  AND tv.cve != 'N/A'
GROUP BY tv.cve, tv.vulnerability_name, tv.cvss, ta.hostname
ORDER BY tv.cvss DESC, affected_assets DESC;
```

## 🔄 Automatización

Para sincronizar automáticamente los datos de Tenable cada 12 horas:

```bash
# Editar crontab
docker-compose exec app crontab -e

# Agregar esta línea
0 */12 * * * cd /usr/src/app && /usr/local/bin/python /usr/src/app/scripts/VickyTopiaReportCLI.py --tenableReport >> /var/log/tenable_sync.log 2>&1
```

## 🐛 Solución de Problemas

### Error: "Tenable API Key or Secret Key not found"

**Solución**: Verifica que las variables de entorno estén configuradas correctamente:

```bash
docker-compose exec app env | grep TENABLE
```

Si no aparecen, edita `.env` y reinicia:

```bash
docker-compose restart app
```

### Error: Rate Limiting (429)

**Causa**: Demasiadas solicitudes a la API de Tenable.

**Solución**: El código ya incluye `time.sleep(0.5)` entre llamadas. Si persiste, reduce la frecuencia de sincronización.

### Tablas Vacías en Metabase

**Solución**: 
1. Ejecuta el reporte de Tenable manualmente
2. En Metabase: Admin → Databases → vanalyzer → "Sync database schema now"

## 📝 Funciones Implementadas

Las siguientes funciones fueron agregadas a `app/scripts/DatabaseConnector.py`:

1. **`check_create_table_tenable_assets()`** - Crea la tabla de activos
2. **`check_create_table_tenable_vulnerabilities()`** - Crea la tabla de vulnerabilidades
3. **`insert_into_table_tenable_assets()`** - Inserta/actualiza activos
4. **`insert_into_table_tenable_vulnerabilities()`** - Inserta/actualiza vulnerabilidades
5. **`create_view_unified_assets()`** - Crea la vista unificada

## 🔗 Referencias

- [Documentación de Tenable.io API](https://developer.tenable.com/)
- [Repositorio GitHub](https://github.com/tvper004/Vicarius-Tenable)

## 📄 Licencia

Este proyecto mantiene la misma licencia que vAnalyzer.

---

**Última actualización**: 2025-12-07  
**Versión**: 1.0.0
