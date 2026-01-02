# 🔄 Guía: Cambio de Puerto de Metabase a 3030

## ✅ Cambios Realizados

He actualizado el `docker-compose.yml` para que Metabase use el puerto **3030** en lugar del puerto 3000 (que está siendo usado por Easypanel).

### Configuración Aplicada:

```yaml
metabase:
  ports:
    - "3030:3000"  # Puerto externo 3030 → Puerto interno 3000
  environment:
    MB_JETTY_PORT: 3000  # Puerto interno de Metabase
```

---

## 🛡️ Protección de la Base de Datos

### ¿Se Corromperá mi Base de Datos?

**NO.** Cambiar el puerto de Metabase es completamente seguro porque:

#### 1. **Separación de Datos**
- **Base de datos `vanalyzer`**: Contiene tus datos de Vicarius y Tenable
- **Base de datos `metabase`**: Solo contiene configuraciones de Metabase (usuarios, dashboards, queries guardadas)
- Ambas están en PostgreSQL y son independientes

#### 2. **El Puerto Solo Afecta el Acceso Web**
- El cambio de puerto solo modifica cómo accedes a la interfaz web de Metabase
- **NO afecta** cómo Metabase se conecta a PostgreSQL (sigue usando el puerto 5432 internamente)
- **NO afecta** los datos almacenados

#### 3. **Volúmenes Persistentes**
- Los datos de Metabase se guardan en el volumen `metabase-data`
- Este volumen permanece intacto durante el cambio de puerto
- Tus dashboards y configuraciones se mantendrán

---

## 📋 Pasos para Aplicar el Cambio

### Paso 1: Commit y Push de los Cambios

```bash
git add docker-compose.yml
git commit -m "Config: Cambiar puerto de Metabase a 3030 para evitar conflicto con Easypanel"
git push origin main
```

### Paso 2: En Easypanel - Actualizar Configuración de Dominio

1. **Ve al servicio `metabase`** en Easypanel
2. **Ve a la pestaña "Dominios"**
3. **Si ya tienes un dominio configurado:**
   - Edítalo
   - Cambia el **Puerto** de `3000` a `3030`
   - **Guarda**

4. **Si no tienes dominio configurado:**
   - Haz clic en "Agregar dominio"
   - Configura:
     ```
     Host: desarrollo-vanalyzerunacem.plifcq.easypanel.host
     (o el dominio que prefieras)
     Ruta: /
     Protocolo: HTTP
     Puerto: 3030  ← IMPORTANTE
     Compose Service: metabase
     HTTPS: Activado
     ```

### Paso 3: Rebuild del Proyecto

1. **Ve a la vista principal de tu proyecto**
2. **Haz clic en "Rebuild"** o "Deploy"
3. **Espera 2-3 minutos** a que los servicios se reinicien

---

## ✅ Verificación Post-Cambio

### 1. Verificar que Metabase Esté Corriendo

En Easypanel:
- Servicio `metabase` debe estar en estado **"Running"** (verde)

### 2. Acceder a Metabase

Abre tu navegador y ve a:
```
https://desarrollo-vanalyzerunacem.plifcq.easypanel.host
```

Deberías ver:
- **Primera vez:** Pantalla de configuración inicial de Metabase
- **Ya configurado:** Pantalla de login de Metabase

### 3. Verificar Conexión a Base de Datos

Una vez dentro de Metabase:
1. Ve a **Settings** → **Admin** → **Databases**
2. Verifica que la conexión a `vanalyzer` esté activa (punto verde)
3. Haz clic en "Test connection" para confirmar

---

## 🔍 Datos que se Mantienen Intactos

Después del cambio de puerto, conservarás:

✅ **Base de datos vanalyzer:**
- Todos los datos de Vicarius
- Todos los datos de Tenable
- Todas las tablas y vistas

✅ **Base de datos metabase:**
- Usuarios y permisos
- Dashboards creados
- Queries guardadas
- Configuraciones

✅ **Volúmenes:**
- `postgres-data`: Datos de PostgreSQL
- `metabase-data`: Configuraciones de Metabase

---

## 🚨 Troubleshooting

### Si no puedes acceder después del cambio:

**1. Verifica el puerto en la configuración del dominio:**
- Debe ser `3030`, no `3000`

**2. Revisa los logs de Metabase:**
```
Easypanel → Servicio metabase → Logs
```
Busca:
```
Metabase Initialization COMPLETE
```

**3. Verifica que el puerto esté expuesto:**
En los logs de Docker deberías ver:
```
0.0.0.0:3030->3000/tcp
```

**4. Reinicia el servicio si es necesario:**
```
Easypanel → Servicio metabase → Restart
```

---

## 📊 Arquitectura Actualizada

```
Internet
    ↓
https://desarrollo-vanalyzerunacem.plifcq.easypanel.host
    ↓
Easypanel (puerto 3030) ← NUEVO PUERTO
    ↓
Servicio: metabase (puerto interno 3000)
    ↓
Base de datos: appdb (puerto 5432)
    ├── vanalyzer (datos de Vicarius/Tenable)
    └── metabase (configuraciones de Metabase)
```

---

## 🎯 Resumen

| Aspecto | Antes | Después |
|---------|-------|---------|
| Puerto externo | 3000 (conflicto) | 3030 ✅ |
| Puerto interno Metabase | 3000 | 3000 (sin cambios) |
| Puerto PostgreSQL | 5432 | 5432 (sin cambios) |
| Datos en vanalyzer | Intactos | Intactos ✅ |
| Configuración Metabase | Intacta | Intacta ✅ |

---

## ✅ Próximos Pasos

1. **Haz commit y push** de los cambios
2. **Actualiza el puerto a 3030** en la configuración del dominio en Easypanel
3. **Haz Rebuild** del proyecto
4. **Accede a Metabase** en tu dominio
5. **Configura Metabase** (si es primera vez) o **inicia sesión** (si ya estaba configurado)

---

**Fecha**: Diciembre 2025  
**Puerto anterior**: 3000  
**Puerto nuevo**: 3030  
**Impacto en datos**: Ninguno ✅
