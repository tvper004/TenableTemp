# 🚨 Guía de Solución Rápida - Error "database metabase does not exist"

## ⚡ Solución Más Rápida (5 minutos)

### Paso 1: Acceder a la Terminal de appdb en Easypanel

1. **Abre Easypanel** en tu navegador
2. **Ve a tu proyecto** (vAnalyzer o como lo hayas nombrado)
3. **Haz clic en el servicio `appdb`**
4. **Busca y haz clic en la pestaña "Terminal" o "Console"** (generalmente está en la parte superior)

### Paso 2: Ejecutar Comandos en la Terminal

Una vez en la terminal, copia y pega estos comandos **UNO POR UNO**:

```bash
# Comando 1: Crear la base de datos metabase
psql -U vanalyzer_user -d vanalyzer -c "CREATE DATABASE metabase;"
```

Deberías ver: `CREATE DATABASE`

```bash
# Comando 2: Dar permisos al usuario
psql -U vanalyzer_user -d vanalyzer -c "GRANT ALL PRIVILEGES ON DATABASE metabase TO vanalyzer_user;"
```

Deberías ver: `GRANT`

```bash
# Comando 3: Verificar que se creó correctamente
psql -U vanalyzer_user -d vanalyzer -c "\l"
```

Deberías ver una lista de bases de datos que incluye:
- `vanalyzer`
- `metabase` ← **Esta es la nueva**

### Paso 3: Reiniciar el Servicio Metabase

1. **Sal de la terminal de appdb**
2. **Ve al servicio `metabase`** en tu proyecto
3. **Haz clic en el botón "Restart"** (icono de reinicio ⟳)
4. **Espera 1-2 minutos** a que Metabase se reinicie

### Paso 4: Verificar que Funciona

1. **Ve a los logs del servicio `appdb`**
2. **Ya NO deberías ver** los errores "database metabase does not exist"
3. **El servicio `metabase`** debería estar en estado "Running" (verde)

---

## 🗑️ Alternativa: Borrar Volumen y Empezar de Cero

Si prefieres empezar limpio (esto **BORRARÁ** todos los datos):

### Paso 1: Detener Todos los Servicios

1. En Easypanel, ve a tu proyecto
2. Para cada servicio (`app`, `metabase`, `appdb`):
   - Haz clic en el servicio
   - Haz clic en "Stop" o el icono de parar ⏹️
   - Espera a que el estado cambie a "Stopped"

### Paso 2: Borrar el Volumen de PostgreSQL

**Opción A: Desde la Interfaz de Easypanel**

1. En el menú lateral izquierdo, busca:
   - "Volumes" o
   - "Volúmenes" o
   - "Storage"

2. Busca el volumen llamado:
   - `postgres-data` o
   - `desarrollo_vanalyzer_postgres-data` o
   - Similar con el nombre de tu proyecto

3. Haz clic en el icono de **eliminar** (🗑️) o botón "Delete"

4. **Confirma** la eliminación cuando te pregunte

**Opción B: Si no encuentras la sección de Volumes**

Algunos paneles de Easypanel tienen la opción en:
- Settings → Volumes
- Storage → Volumes
- Directamente en el servicio `appdb` → Storage/Volumes

### Paso 3: Rebuild el Proyecto

1. Ve a la vista principal de tu proyecto
2. Haz clic en **"Rebuild"** o **"Deploy"**
3. Easypanel:
   - Reconstruirá las imágenes Docker
   - Creará un nuevo volumen vacío
   - El script `init-databases.sh` se ejecutará automáticamente
   - Creará las bases de datos `vanalyzer` y `metabase`

### Paso 4: Esperar y Verificar

1. **Espera 3-5 minutos** a que todos los servicios se inicien
2. **Verifica el estado** de cada servicio:
   - `appdb`: Debería estar "healthy" (verde)
   - `metabase`: Debería estar "running" (verde)
   - `app`: Debería estar "running" (verde)

3. **Revisa los logs de `appdb`**:
   - Deberías ver: `✅ Inicialización de bases de datos completada`
   - NO deberías ver errores de "database does not exist"

---

## 🔍 Cómo Encontrar las Opciones en Easypanel

### Ubicación de la Terminal

La terminal del servicio generalmente está en:
```
Proyecto → Servicio (appdb) → Pestañas superiores → "Terminal" o "Console" o "Shell"
```

### Ubicación de Volumes

Los volúmenes generalmente están en:
```
Opción 1: Menú lateral → "Volumes"
Opción 2: Proyecto → "Storage" → "Volumes"
Opción 3: Servicio (appdb) → "Volumes" o "Storage"
```

### Ubicación del Botón Restart

El botón de reinicio generalmente está en:
```
Proyecto → Servicio (metabase) → Botón "Restart" (⟳) en la parte superior
```

---

## ❓ Preguntas Frecuentes

### ¿Perderé datos si ejecuto los comandos de la Opción 1?

**No.** La Opción 1 solo crea una base de datos nueva (`metabase`). No toca tus datos existentes en `vanalyzer`.

### ¿Perderé datos si borro el volumen (Opción 2)?

**Sí.** Borrar el volumen eliminará:
- Todos los datos de Vicarius y Tenable en la base de datos `vanalyzer`
- Configuraciones de Metabase

Pero no te preocupes:
- El servicio `app` volverá a sincronizar los datos automáticamente
- Metabase se reconfigurará en el primer acceso

### ¿Cuál opción debo elegir?

- **Opción 1 (Comandos manuales)**: Si ya tienes datos y no quieres perderlos
- **Opción 2 (Borrar volumen)**: Si estás empezando o no te importa volver a sincronizar

### ¿Cuánto tiempo tarda la sincronización inicial?

Depende del número de activos:
- Menos de 500 activos: 5-10 minutos
- 500-1000 activos: 10-20 minutos
- Más de 1000 activos: 20-30 minutos

---

## ✅ Checklist de Verificación

Después de aplicar la solución, verifica:

- [ ] Los logs de `appdb` NO muestran errores "database does not exist"
- [ ] El servicio `appdb` está en estado "healthy" (verde)
- [ ] El servicio `metabase` está en estado "running" (verde)
- [ ] El servicio `app` está en estado "running" (verde)
- [ ] Puedes acceder a Metabase desde tu navegador
- [ ] Metabase muestra la pantalla de configuración inicial (si es nuevo) o el dashboard (si ya estaba configurado)

---

## 🆘 Si Nada Funciona

Si después de intentar ambas opciones sigues viendo errores:

1. **Toma capturas de pantalla** de:
   - Los logs completos de `appdb`
   - Los logs de `metabase`
   - La configuración de variables de entorno

2. **Verifica las variables de entorno** en Easypanel:
   - `POSTGRES_PASSWORD` debe estar configurada
   - `POSTGRES_USER` debe ser `vanalyzer_user` (o el que uses)
   - `POSTGRES_DB` debe ser `vanalyzer`

3. **Intenta acceder a la terminal de `appdb`** y ejecuta:
   ```bash
   psql -U vanalyzer_user -d vanalyzer -c "\l"
   ```
   Esto te mostrará qué bases de datos existen realmente.

---

**Última actualización**: Diciembre 2025  
**Tiempo estimado**: 5-10 minutos (Opción 1) o 15-20 minutos (Opción 2)
