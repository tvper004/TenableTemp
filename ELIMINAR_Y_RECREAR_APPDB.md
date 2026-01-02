# 🚀 Guía Paso a Paso: Eliminar y Recrear el Servicio appdb en Easypanel

## ✅ Cambios Realizados

He actualizado todos los archivos para usar `vicarius_user` en lugar de `vanalyzer_user`:
- ✅ `docker-compose.yml`
- ✅ `appdb/init-databases.sh`
- ✅ `.env.example`

Los cambios ya están en tu repositorio GitHub.

---

## 📋 Pasos para Solucionar en Easypanel

### Paso 1: Detener Todos los Servicios

1. Ve a tu proyecto en Easypanel
2. Haz clic en el servicio **`app`** → botón **"Stop"** (⏹️)
3. Haz clic en el servicio **`metabase`** → botón **"Stop"** (⏹️)
4. Haz clic en el servicio **`appdb`** → botón **"Stop"** (⏹️)

Espera a que todos los servicios muestren estado "Stopped" (rojo).

---

### Paso 2: Eliminar el Servicio appdb

**Opción A: Desde la interfaz de servicios**

1. Haz clic en el servicio **`appdb`**
2. Busca el menú de opciones (generalmente tres puntos verticales **⋮** o un botón "Settings")
3. Busca la opción **"Delete Service"** o **"Eliminar Servicio"**
4. **Confirma** la eliminación

**Opción B: Si no encuentras la opción de eliminar**

Algunos paneles de Easypanel requieren que elimines el servicio desde la vista de proyecto:

1. Ve a la vista principal de tu proyecto
2. Busca una lista de servicios
3. Junto al servicio `appdb`, debería haber un icono de eliminar (🗑️)
4. Haz clic y confirma

---

### Paso 3: Verificar Variables de Entorno

Antes de hacer rebuild, verifica que las variables estén correctas:

1. En tu proyecto, ve a **"Environment"** o **"Variables de entorno"**
2. Verifica que tengas:

```env
POSTGRES_DB=vanalyzer
POSTGRES_USER=vicarius_user
POSTGRES_PASSWORD=VicariusT3N48l3
```

3. Si `POSTGRES_USER` todavía dice `vanalyzer_user`, **cámbialo a `vicarius_user`**
4. Haz clic en **"Save"** o **"Guardar"**

---

### Paso 4: Hacer Rebuild del Proyecto

1. Ve a la vista principal de tu proyecto
2. Haz clic en el botón **"Rebuild"** o **"Deploy"**
3. Easypanel:
   - Descargará los cambios del repositorio
   - Reconstruirá las imágenes Docker
   - Creará un nuevo servicio `appdb` con un volumen limpio
   - Ejecutará el script `init-databases.sh` que creará:
     - Usuario `vicarius_user`
     - Base de datos `vanalyzer`
     - Base de datos `metabase`

---

### Paso 5: Esperar y Verificar

1. **Espera 3-5 minutos** mientras los servicios se inician

2. **Verifica el estado de los servicios:**
   - `appdb`: Debería estar **"healthy"** (verde) ✅
   - `metabase`: Debería estar **"running"** (verde) ✅
   - `app`: Debería estar **"running"** (verde) ✅

3. **Revisa los logs de `appdb`:**
   - Haz clic en el servicio `appdb`
   - Ve a la pestaña **"Registros"** o **"Logs"**
   - Deberías ver:
     ```
     🔧 Iniciando configuración de PostgreSQL...
     Usuario vicarius_user creado
     ✅ Configuración de PostgreSQL completada
     ```
   - **NO** deberías ver errores de "database does not exist"

---

## 🎯 Resultado Esperado

Después de completar estos pasos:

- ✅ PostgreSQL tendrá el usuario `vicarius_user` creado
- ✅ Las bases de datos `vanalyzer` y `metabase` existirán
- ✅ Metabase podrá conectarse sin errores
- ✅ El servicio `app` podrá sincronizar datos
- ✅ No más errores en los logs

---

## 🔍 Si No Puedes Eliminar el Servicio

Si Easypanel no te permite eliminar el servicio `appdb`, hay una alternativa:

### Alternativa: Forzar Recreación del Volumen

1. **Detén todos los servicios** (como en Paso 1)

2. **En la consola de tu computadora local**, ejecuta:

```bash
# Conectarte por SSH a tu servidor de Easypanel (si tienes acceso)
ssh usuario@tu-servidor-easypanel

# Listar volúmenes de Docker
docker volume ls | grep postgres

# Eliminar el volumen (reemplaza el nombre exacto)
docker volume rm desarrollo_vanalyzer_postgres-data
```

3. **Haz Rebuild** en Easypanel

---

## ❓ Preguntas Frecuentes

### ¿Perderé datos al eliminar el servicio appdb?

**Sí**, pero no te preocupes:
- El servicio `app` volverá a sincronizar automáticamente los datos de Vicarius y Tenable
- Metabase se reconfigurará en el primer acceso
- Es la forma más limpia de solucionar el problema

### ¿Cuánto tiempo tarda la sincronización inicial?

Depende del número de activos:
- Menos de 500 activos: 5-10 minutos
- 500-1000 activos: 10-20 minutos
- Más de 1000 activos: 20-30 minutos

### ¿Qué hago si sigo viendo errores después del rebuild?

1. Verifica que las variables de entorno estén correctas
2. Revisa los logs de `appdb` para ver mensajes de error específicos
3. Verifica que el script `init-databases.sh` se haya ejecutado

---

## 📞 Siguiente Paso

Una vez que hayas eliminado el servicio `appdb` y hecho rebuild, avísame y verificaremos juntos que todo funcione correctamente.

---

**Última actualización**: Diciembre 2025  
**Tiempo estimado**: 10-15 minutos
