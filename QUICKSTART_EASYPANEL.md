# 🚀 Guía Rápida - Configuración en Easypanel

## Variables de Entorno a Configurar

Copia y pega estas variables en la sección "Environment" de Easypanel, reemplazando los valores:

```env
# ============================================
# VARIABLES OBLIGATORIAS
# ============================================

# Base de Datos PostgreSQL
POSTGRES_PASSWORD=TuContraseñaSegura123!

# API de Vicarius
VICARIUS_API_KEY=tu_api_key_de_vicarius_aqui
VICARIUS_DASHBOARD_ID=tu_dashboard_id

# API de Tenable
TENABLE_API_KEY=tu_tenable_access_key_aqui
TENABLE_SECRET_KEY=tu_tenable_secret_key_aqui

# ============================================
# VARIABLES OPCIONALES (puedes omitirlas)
# ============================================

# Configuración de Base de Datos (usa defaults si no se especifican)
POSTGRES_DB=vanalyzer
POSTGRES_USER=vanalyzer_user

# Herramientas Opcionales
OPTIONAL_TOOLS=
```

## 📋 Checklist de Despliegue

- [ ] 1. Hacer push de los cambios a GitHub
- [ ] 2. En Easypanel, ir a tu proyecto
- [ ] 3. Configurar las variables de entorno (ver arriba)
- [ ] 4. Hacer clic en "Deploy" o "Rebuild"
- [ ] 5. Esperar a que los servicios se inicien (2-5 minutos)
- [ ] 6. Verificar que los 3 servicios estén "Running" (verde)
- [ ] 7. Acceder a Metabase y completar configuración inicial

## 🔗 Acceso a Servicios

### Metabase (Dashboard)
- **URL**: La que configures en Easypanel o la auto-generada
- **Puerto**: 3000
- **Primera vez**: Configurar cuenta de administrador

### Base de Datos (Interno)
- **Host**: `appdb` (nombre del servicio)
- **Puerto**: 5432
- **Database**: `vanalyzer`
- **User**: `vanalyzer_user` (o el que configures)

## ⚠️ Errores Comunes

### Error: "unsupported external secret"
✅ **Solucionado** - Ya no se usan secrets externos

### Error: "ports is used in appdb"
✅ **Solucionado** - Puertos removidos del servicio appdb

### Error: "service appdb unhealthy"
🔍 **Verificar**:
- Que `POSTGRES_PASSWORD` esté configurada
- Logs del servicio appdb en Easypanel

### Error: "app cannot connect to database"
🔍 **Verificar**:
- Que `POSTGRES_PASSWORD` sea la misma en todos los servicios
- Que el servicio appdb esté "healthy" antes de que app inicie

## 📊 Orden de Inicio de Servicios

```
1. appdb (PostgreSQL)
   ↓ (espera healthcheck)
2. metabase + app (en paralelo)
   ↓
3. Sistema listo
```

## 🔐 Seguridad

### ✅ Hacer:
- Usar contraseñas fuertes para `POSTGRES_PASSWORD`
- Mantener las API keys seguras
- Configurar las variables en Easypanel, no en archivos

### ❌ NO Hacer:
- NO commitear archivos `.env` con credenciales
- NO compartir las API keys públicamente
- NO usar contraseñas simples en producción

## 📖 Documentación Completa

Para más detalles, consulta:
- **Guía completa**: [EASYPANEL_DEPLOYMENT.md](./EASYPANEL_DEPLOYMENT.md)
- **Resumen de cambios**: [CAMBIOS_EASYPANEL.md](./CAMBIOS_EASYPANEL.md)
- **Variables de entorno**: [.env.example](./.env.example)

## 🆘 Ayuda

Si algo no funciona:

1. **Revisa los logs** en Easypanel de cada servicio
2. **Verifica las variables** de entorno estén todas configuradas
3. **Consulta** la sección de "Solución de Problemas" en EASYPANEL_DEPLOYMENT.md

---

**Versión**: 2.0  
**Última actualización**: Diciembre 2025  
**Estado**: ✅ Listo para producción
