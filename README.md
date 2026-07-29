# IDEUSS Lead Intake API

Servicio receptor de leads multi-fuente para el sistema de prospección IDEUSS.

## ¿Qué hace?

Recibe leads desde cualquier agente IDEUSS y ejecuta automáticamente:
1. Diagnóstico StoryBrand del sitio web del prospecto
2. Crea Organización + Deal en Pipedrive (Pipeline **AI Web Factory**)
3. Añade nota HTML con señal de dolor detectada
4. Programa actividad de seguimiento
5. Envía notificación inmediata a Telegram

## Endpoint

```
POST /api/lead
GET  /health
```

## JSON de entrada

```json
{
  "fuente":    "whatsapp_agente",
  "nombre":    "Clínica Dental Sonrisas",
  "email":     "info@sonrisas.com",
  "telefono":  "3001234567",
  "url_sitio": "https://sonrisas.com",
  "ciudad":    "Cali",
  "niche":     "Clínica Dental",
  "contexto":  "Cliente preguntó por evaluación web en WhatsApp"
}
```

### Valores válidos para `fuente`

| Valor | Origen |
|---|---|
| `whatsapp_agente` | agente.ideuss.com (WhatsApp/Chatbot) |
| `diagnostico_procesos` | diagnostico.ideuss.com |
| `landing_contenido` | Formularios / landing pages |
| `brief_completado` | Brief sitio web diligenciado |

## Despliegue en EasyPanel

### Variables de entorno requeridas

| Variable | Descripción |
|---|---|
| `PIPEDRIVE_API_KEY` | Token API de Pipedrive |
| `TELEGRAM_BOT_TOKEN` | Token del bot de Telegram |
| `TELEGRAM_HOME_CHANNEL` | Chat ID de Telegram (default: 8808084550) |
| `PORT` | Puerto del servicio (default: 8765) |

### Pasos

1. Crear repo en GitHub → subir estos archivos
2. EasyPanel → **New Service** → **App**
3. Conectar el repo de GitHub
4. Configurar las variables de entorno
5. Puerto: `8765`
6. EasyPanel asigna dominio automático

## Integración en agentes externos

```python
import requests

INTAKE_URL = "https://intake.ideuss.com/api/lead"  # URL de EasyPanel

def send_lead(nombre, email="", telefono="", url_sitio="",
              ciudad="", niche="", contexto="",
              fuente="whatsapp_agente"):
    payload = {k: v for k, v in {
        "fuente": fuente, "nombre": nombre, "email": email,
        "telefono": telefono, "url_sitio": url_sitio,
        "ciudad": ciudad, "niche": niche, "contexto": contexto,
    }.items() if v}
    try:
        r = requests.post(INTAKE_URL, json=payload, timeout=10)
        return r.status_code == 202
    except Exception:
        return False
```
