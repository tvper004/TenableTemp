# Guía de Despliegue en Easypanel - vAnalyzer

## 📋 Descripción General

Esta guía te ayudará a desplegar vAnalyzer en Easypanel. El proyecto incluye tres servicios principales:

- **appdb**: Base de datos PostgreSQL 16.0
- **app**: Aplicación Python que sincroniza datos de Vicarius y Tenable
- **metabase**: Herramienta de visualización y reportes

## 🔧 Cambios Recientes

### ✅ Problemas Corregidos

1. **Eliminación de Docker Secrets**: Se reemplazaron los `external secrets` (no soportados por Easypanel) por variables de entorno estándar.

2. **Eliminación de puertos expuestos en appdb**: Se removió la exposición de puertos del servicio `appdb` para evitar conflictos con otros servicios.

3. **Healthchecks mejorados**: Se agregaron verificaciones de salud para asegurar que los servicios se inicien en el orden correcto.

## 📝 Requisitos Previos

Antes de comenzar, necesitas tener:

1. Una cuenta en Easypanel con acceso a un proyecto
2. Acceso a las APIs de:
   - Vicarius (API Key y Dashboard ID)
   - Tenable.io (API Access Key y Secret Key)
3. Repositorio Git con el código del proyecto

## 🚀 Pasos de Despliegue

### Paso 1: Preparar el Repositorio

1. Asegúrate de que todos los cambios estén commiteados y pusheados a tu repositorio Git:

```bash
git add .
git commit -m "Fix: Configuración para Easypanel - Variables de entorno"
git push origin main
```

### Paso 2: Crear el Proyecto en Easypanel

1. Accede a tu panel de Easypanel
2. Crea un nuevo proyecto o selecciona uno existente
3. Agrega un nuevo servicio de tipo "Docker Compose"
4. Conecta tu repositorio Git (GitHub, GitLab, etc.)
5. Selecciona la rama `main` (o la rama que uses)

### Paso 3: Configurar Variables de Entorno

En la sección "Environment" de Easypanel, configura las siguientes variables:

#### Variables Requeridas (OBLIGATORIAS):

```env
# Base de Datos
POSTGRES_PASSWORD=tu_contraseña_segura_aqui

# Vicarius API
VICARIUS_API_KEY=tu_api_key_de_vicarius
VICARIUS_DASHBOARD_ID=tu_dashboard_id

# Tenable API
TENABLE_API_KEY=tu_tenable_access_key
TENABLE_SECRET_KEY=tu_tenable_secret_key
```

#### Variables Opcionales (con valores por defecto):

```env
# Base de Datos (opcional, usa los defaults si no se especifican)
POSTGRES_DB=vanalyzer
POSTGRES_USER=vanalyzer_user

# Herramientas opcionales
OPTIONAL_TOOLS=
```

> ⚠️ **IMPORTANTE**: NO incluyas espacios alrededor del signo `=` en las variables de entorno.

### Paso 4: Configurar Dominios (Opcional)

Si deseas acceder a Metabase desde un dominio personalizado:

1. En Easypanel, ve a la configuración del servicio `metabase`
2. Agrega un dominio personalizado (ej: `metabase.tudominio.com`)
3. Easypanel configurará automáticamente SSL con Let's Encrypt

### Paso 5: Desplegar

1. Haz clic en "Deploy" en Easypanel
2. Easypanel comenzará a:
   - Clonar el repositorio
   - Construir las imágenes Docker
   - Iniciar los servicios en el orden correcto

### Paso 6: Monitorear el Despliegue

Puedes ver los logs en tiempo real en Easypanel:

1. **Servicio appdb**: Debería mostrar que PostgreSQL está listo para aceptar conexiones
2. **Servicio app**: Mostrará el progreso de la sincronización inicial de datos
3. **Servicio metabase**: Indicará cuando esté listo en el puerto 3000

## 🔍 Verificación Post-Despliegue

### 1. Verificar que los servicios estén corriendo

En Easypanel, todos los servicios deberían mostrar estado "Running" (verde).

### 2. Acceder a Metabase

1. Accede a Metabase usando el dominio configurado o la URL proporcionada por Easypanel
2. En el primer acceso, Metabase te pedirá:
   - Crear una cuenta de administrador
   - Configurar la conexión a la base de datos (ya está preconfigurada)

### 3. Verificar la sincronización de datos

Revisa los logs del servicio `app` para confirmar que:
- La sincronización inicial se completó exitosamente
- No hay errores de conexión a las APIs de Vicarius o Tenable

## 📊 Estructura de la Base de Datos

El proyecto crea automáticamente las siguientes bases de datos en PostgreSQL:

- **vanalyzer**: Base de datos principal con datos de Vicarius y Tenable
- **metabase**: Base de datos interna de Metabase para configuración

## 🔐 Seguridad

### Mejores Prácticas:

1. **Contraseñas Seguras**: Usa contraseñas fuertes para `POSTGRES_PASSWORD`
2. **API Keys**: Nunca compartas tus API keys en repositorios públicos
3. **Variables de Entorno**: Configura las variables directamente en Easypanel, no en archivos `.env` commiteados
4. **Acceso a Metabase**: Configura autenticación fuerte en Metabase

## 🐛 Solución de Problemas

### Error: "unsupported external secret"

**Causa**: Estás usando una versión antigua del `docker-compose.yml` con secrets externos.

**Solución**: Asegúrate de usar la versión actualizada del archivo que usa variables de entorno.

### Error: "ports is used in appdb"

**Causa**: El servicio `appdb` tiene puertos expuestos que causan conflictos.

**Solución**: La versión actualizada del `docker-compose.yml` ya no expone puertos en `appdb`.

### El servicio `app` no puede conectarse a la base de datos

**Verificar**:
1. Que `POSTGRES_PASSWORD` esté configurada correctamente
2. Que el servicio `appdb` esté en estado "healthy"
3. Los logs del servicio `appdb` para errores

### Metabase no inicia

**Verificar**:
1. Que el servicio `appdb` esté corriendo y saludable
2. Que `POSTGRES_PASSWORD` sea la misma en todos los servicios
3. Los logs de Metabase para mensajes de error específicos

### La sincronización inicial falla

**Verificar**:
1. Que las API keys de Vicarius y Tenable sean correctas
2. Que tengas acceso a las APIs desde el servidor de Easypanel
3. Los logs del servicio `app` para detalles del error

## 🔄 Actualización del Proyecto

Para actualizar el proyecto después de hacer cambios en el código:

1. Haz push de tus cambios al repositorio Git:
```bash
git add .
git commit -m "Descripción de los cambios"
git push origin main
```

2. En Easypanel:
   - Ve a tu servicio
   - Haz clic en "Rebuild" para reconstruir las imágenes
   - O configura auto-deploy para que se actualice automáticamente con cada push

## 📞 Soporte

Si encuentras problemas no cubiertos en esta guía:

1. Revisa los logs de cada servicio en Easypanel
2. Verifica que todas las variables de entorno estén configuradas correctamente
3. Asegúrate de estar usando la última versión del código

## 📚 Recursos Adicionales

- [Documentación de Easypanel](https://easypanel.io/docs)
- [Documentación de Docker Compose](https://docs.docker.com/compose/)
- [Documentación de Metabase](https://www.metabase.com/docs/latest/)
- [API de Vicarius](https://docs.vicarius.io/)
- [API de Tenable](https://developer.tenable.com/)

---

**Última actualización**: Diciembre 2025  
**Versión**: 2.0 - Configuración con Variables de Entorno
