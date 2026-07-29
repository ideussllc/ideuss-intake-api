# ideuss_intake.py
# ─────────────────────────────────────────────────────────────────────────────
# Cliente Python para enviar leads al IDEUSS Lead Intake API
# Copiar este archivo en la raíz de cada proyecto agente y usar send_lead()
# ─────────────────────────────────────────────────────────────────────────────

import logging
import os

import requests

# URL del servicio en EasyPanel — cambiar por la URL real asignada
INTAKE_URL = os.environ.get(
    "IDEUSS_INTAKE_URL",
    "https://intake.ideuss.com/api/lead"  # ← actualizar con URL de EasyPanel
)
TIMEOUT = 10  # segundos


def send_lead(
    nombre:    str,
    fuente:    str  = "whatsapp_agente",
    email:     str  = "",
    telefono:  str  = "",
    url_sitio: str  = "",
    ciudad:    str  = "Colombia",
    niche:     str  = "",
    contexto:  str  = "",
) -> bool:
    """
    Envía un lead al sistema de prospección IDEUSS.
    Retorna True si fue aceptado (HTTP 202), False si falló.

    Parámetros
    ----------
    nombre    : Nombre del negocio (REQUERIDO)
    fuente    : "whatsapp_agente" | "diagnostico_procesos" |
                "landing_contenido" | "brief_completado"
    email     : Email de contacto del negocio
    telefono  : Teléfono/WhatsApp (formato colombiano o solo dígitos)
    url_sitio : URL del sitio web actual — usado para diagnóstico StoryBrand
    ciudad    : Ciudad de operación (ej: "Cali", "Bogotá")
    niche     : Tipo de negocio (ej: "Clínica Dental", "Estética")
    contexto  : Texto libre — qué dijo el cliente, resultado del diagnóstico

    Lo que ocurre automáticamente al enviar
    ----------------------------------------
    1. Diagnóstico StoryBrand del sitio web
    2. Deal creado en Pipedrive → Pipeline AI Web Factory
    3. Nota con señal de dolor detectada
    4. Actividad de seguimiento programada
    5. Notificación inmediata en Telegram a Alejandro Torres
    """
    if not nombre:
        logging.error("[IDEUSS] 'nombre' es requerido")
        return False

    payload = {
        "fuente":    fuente,
        "nombre":    nombre,
        "email":     email,
        "telefono":  telefono,
        "url_sitio": url_sitio,
        "ciudad":    ciudad,
        "niche":     niche,
        "contexto":  contexto,
    }
    # Limpiar campos vacíos para no enviar ruido
    payload = {k: v for k, v in payload.items() if v}

    try:
        resp = requests.post(INTAKE_URL, json=payload, timeout=TIMEOUT)
        if resp.status_code == 202:
            logging.info(f"[IDEUSS] ✅ Lead enviado: {nombre} [{fuente}]")
            return True
        else:
            logging.warning(
                f"[IDEUSS] ⚠️  Error {resp.status_code}: {resp.text[:200]}"
            )
            return False
    except requests.exceptions.ConnectionError:
        logging.error(f"[IDEUSS] ❌ No se pudo conectar a {INTAKE_URL}")
        return False
    except requests.exceptions.Timeout:
        logging.error("[IDEUSS] ❌ Timeout al conectar con el servicio")
        return False
    except Exception as e:
        logging.error(f"[IDEUSS] ❌ Error inesperado: {e}")
        return False  # Nunca crashear el agente por esto
