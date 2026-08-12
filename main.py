#!/usr/bin/env python3
"""
IDEUSS Lead Intake API  v1.0
Servicio receptor de leads multi-fuente para el sistema de prospección IDEUSS.

Fuentes que alimentan este servicio:
  - agente.ideuss.com   (WhatsApp/Chatbot)
  - diagnostico.ideuss.com (Diagnóstico de procesos)
  - landing pages / formularios

Acciones automáticas por cada lead recibido:
  1. Diagnóstico StoryBrand del sitio web
  2. Registro en Pipedrive → Pipeline AI Web Factory
  3. Nota HTML con señal de dolor detectada
  4. Actividad de seguimiento programada
  5. Notificación inmediata en Telegram

Endpoints:
  POST /api/lead   → Recibir nuevo lead
  GET  /health     → Estado del servicio
"""

import json
import logging
import os
import re
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "[%(asctime)s] %(message)s",
    datefmt = "%H:%M:%S"
)
log = logging.getLogger("ideuss-intake")

# ── SSL ───────────────────────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode    = ssl.CERT_NONE

# ── Credenciales desde variables de entorno ───────────────────────────────────
PIPEDRIVE_API_KEY  = os.environ.get("PIPEDRIVE_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_HOME_CHANNEL", "8808084550")
PORT               = int(os.environ.get("PORT", "8765"))

# ── Pipeline AI Web Factory (Pipedrive) ───────────────────────────────────────
PIPELINE_ID = 28
STAGES = {
    "cualificado":          145,
    "contacto_establecido": 146,
    "definiendo_mockup":    147,
    "propuesta_realizada":  148,
    "en_negociacion":       149,
}

# ── Fuentes y prioridades ─────────────────────────────────────────────────────
FUENTES = {
    "whatsapp_agente":      {"label": "💬 WhatsApp",    "prioridad": "alta"},
    "diagnostico_procesos": {"label": "🔍 Diagnóstico", "prioridad": "alta"},
    "landing_contenido":    {"label": "📄 Landing",     "prioridad": "alta"},
    "brief_completado":     {"label": "📋 Brief Web",   "prioridad": "muy_alta"},
    "hermes_saliente":      {"label": "🤖 Hermes",      "prioridad": "normal"},
}

# ── Schema de referencia para documentación ───────────────────────────────────
SCHEMA = {
    "fuente":    "whatsapp_agente | diagnostico_procesos | landing_contenido | brief_completado",
    "nombre":    "Nombre del negocio (requerido)",
    "email":     "email@negocio.com",
    "telefono":  "3001234567",
    "url_sitio": "https://negocio.com",
    "ciudad":    "Cali",
    "niche":     "Clínica Dental / Veterinaria / Estética...",
    "contexto":  "Qué dijo el cliente, qué necesita (texto libre)",
}

# ═════════════════════════════════════════════════════════════════════════════
# UTILIDADES HTTP
# ═════════════════════════════════════════════════════════════════════════════

def http_post(url: str, payload: dict, headers: dict = None) -> dict | None:
    body = json.dumps(payload).encode()
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning(f"HTTP POST error [{url[:50]}]: {e}")
    return None


def http_put(url: str, payload: dict) -> bool:
    body = json.dumps(payload).encode()
    req  = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="PUT"
    )
    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=15) as r:
            data = json.loads(r.read())
            return data.get("success", False)
    except Exception as e:
        log.warning(f"HTTP PUT error: {e}")
    return False


# ═════════════════════════════════════════════════════════════════════════════
# PIPEDRIVE
# ═════════════════════════════════════════════════════════════════════════════

def pd_post(endpoint: str, payload: dict):
    """POST a Pipedrive API. Retorna ID del recurso creado o None."""
    if not PIPEDRIVE_API_KEY:
        return None
    url  = f"https://api.pipedrive.com/v1/{endpoint}?api_token={PIPEDRIVE_API_KEY}"
    data = http_post(url, payload)
    if data and data.get("success"):
        return data["data"].get("id")
    if data:
        log.warning(f"Pipedrive [{endpoint}]: {data.get('error')}")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═════════════════════════════════════════════════════════════════════════════

def tg_send(message: str):
    """Envía mensaje a Telegram."""
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    http_post(url, {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "Markdown",
    })


# ═════════════════════════════════════════════════════════════════════════════
# DIAGNÓSTICO STORYBRAND
# ═════════════════════════════════════════════════════════════════════════════

def fetch_page(url: str, timeout=10) -> str | None:
    """Descarga una página y devuelve el texto limpio."""
    if not url:
        return None
    try:
        if not url.startswith("http"):
            url = "https://" + url
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; IDEUSSBot/1.0)"
        })
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as r:
            raw  = r.read(80000).decode("utf-8", errors="ignore")
            text = re.sub(r"<script[^>]*>.*?</script>", " ", raw, flags=re.DOTALL)
            text = re.sub(r"<style[^>]*>.*?</style>",   " ", text, flags=re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            return re.sub(r"\s+", " ", text).lower().strip()
    except Exception:
        return None


PAIN_SIGNALS = [
    {
        "name":        "sin_web",
        "description": "No tiene sitio web propio",
        "message":     "No encontramos sitio web propio. El 87% de los clientes buscan servicios online antes de llamar. Sin web, son invisibles para la mayoría de sus clientes potenciales.",
        "web_proposal": True,
        "check": lambda t, u: not u and t is None,
    },
    {
        "name":        "sin_cita_online",
        "description": "No ofrece reserva de citas online",
        "message":     "Su sitio web no permite reservar citas online. Los clientes modernos esperan poder agendar en 30 segundos desde el móvil — sin llamar, sin esperar.",
        "web_proposal": True,
        "check": lambda t, u: bool(u) and not any(
            w in (t or "") for w in ["agenda","reserva","cita online","book","turnos","calendar","appointment"]
        ),
    },
    {
        "name":        "whatsapp_manual",
        "description": "Usa WhatsApp manual como único canal digital",
        "message":     "Usan WhatsApp como canal principal sin automatización. Cada mensaje fuera de horario es un cliente perdido. Un chatbot IA atiende 24/7 sin costo adicional.",
        "web_proposal": True,
        "check": lambda t, u: bool(u) and (
            "whatsapp" in (t or "") and
            not any(w in (t or "") for w in ["chatbot","bot","automatico","automático","24/7"])
        ),
    },
    {
        "name":        "web_desactualizada",
        "description": "Sitio web sin propuesta de valor clara (StoryBrand)",
        "message":     "Su sitio web no comunica claramente qué problema resuelve ni por qué elegirlos. Los visitantes se van en 8 segundos si no ven la propuesta de valor de inmediato.",
        "web_proposal": True,
        "check": lambda t, u: bool(u) and (
            t is not None and len(t) < 3000 and
            not any(w in (t or "") for w in ["resultado","beneficio","transformación","garantía","testimonios","reseñas"])
        ),
    },
    {
        "name":        "sin_reseñas_gestionadas",
        "description": "Sin sistema de gestión de reseñas online",
        "message":     "No gestionan activamente sus reseñas online. El 93% de los consumidores lee reseñas antes de elegir un proveedor. Un sistema automático puede duplicar su calificación en 60 días.",
        "web_proposal": True,
        "check": lambda t, u: bool(u) and not any(
            w in (t or "") for w in ["google","reseña","opinión","valoración","review","calificación"]
        ),
    },
]


def run_diagnostic(url_sitio: str) -> dict:
    """Analiza el sitio web y retorna la señal de dolor StoryBrand detectada."""
    page_text = fetch_page(url_sitio) if url_sitio else None

    for signal in PAIN_SIGNALS:
        try:
            if signal["check"](page_text, url_sitio):
                return {
                    "name":         signal["name"],
                    "description":  signal["description"],
                    "message":      signal["message"],
                    "web_proposal": signal.get("web_proposal", False),
                }
        except Exception:
            continue

    return {
        "name":         "procesos_manuales",
        "description":  "Procesos operativos no automatizados",
        "message":      "Sus procesos de atención, seguimiento y marketing dependen de tareas manuales que consumen tiempo y generan errores. La automatización IA puede recuperar 15+ horas semanales.",
        "web_proposal": False,
    }


# ═════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL DE INGESTA
# ═════════════════════════════════════════════════════════════════════════════

def process_lead(data: dict) -> dict:
    """
    Ejecuta el pipeline completo para un lead entrante:
    diagnóstico → Pipedrive → Telegram
    """
    fuente    = data.get("fuente", "desconocida")
    nombre    = data.get("nombre", "").strip()
    email     = data.get("email", "").strip()
    telefono  = data.get("telefono", "").strip()
    url_sitio = data.get("url_sitio", "").strip()
    ciudad    = data.get("ciudad", "Colombia").strip()
    niche     = data.get("niche", "Empresa").strip()
    contexto  = data.get("contexto", "").strip()

    fuente_info = FUENTES.get(fuente, {"label": fuente, "prioridad": "normal"})
    ts          = datetime.now().strftime("%Y-%m-%d %H:%M")

    log.info(f"🔔 [{fuente_info['label']}] {nombre} | {email} | {url_sitio or 'sin web'}")

    # ── 1. Diagnóstico StoryBrand ─────────────────────────────────────────────
    log.info(f"  🔍 Analizando sitio: {url_sitio or 'N/A'}")
    pain = run_diagnostic(url_sitio)
    log.info(f"  🎯 Señal: [{pain['name']}] {pain['description']}")

    # ── 2. Organización en Pipedrive ──────────────────────────────────────────
    org_id = pd_post("organizations", {"name": nombre})
    log.info(f"  🏢 Org: {org_id}")

    # ── 3. Persona de contacto ────────────────────────────────────────────────
    person_payload = {"name": f"Contacto — {nombre}"}
    if org_id:   person_payload["org_id"] = org_id
    if email:    person_payload["email"]  = [{"value": email,    "label": "work", "primary": True}]
    if telefono: person_payload["phone"]  = [{"value": telefono, "label": "work", "primary": True}]
    person_id = pd_post("persons", person_payload)

    # ── 4. Deal en pipeline AI Web Factory ───────────────────────────────────
    deal_payload = {
        "title":       f"{nombre} | {fuente_info['label']}",
        "pipeline_id": PIPELINE_ID,
        "stage_id":    STAGES["cualificado"],
        "status":      "open",
    }
    if org_id:    deal_payload["org_id"]    = org_id
    if person_id: deal_payload["person_id"] = person_id
    deal_id = pd_post("deals", deal_payload)
    log.info(f"  📌 Deal AI Web Factory: {deal_id}")

    # ── 5. Nota HTML con diagnóstico ──────────────────────────────────────────
    if deal_id:
        digits = re.sub(r"[^\d]", "", telefono)
        wa_url = f"https://wa.me/57{digits}" if digits else ""

        nota = f"""
<b>🔔 FUENTE: {fuente_info['label']}</b> | <b>📅 {ts}</b><br><br>
<b>🎯 SEÑAL DE DOLOR (StoryBrand / Donald Miller):</b><br>
<b>{pain['description']}</b><br>
{pain['message']}<br><br>
<b>📍 Datos del prospecto:</b><br>
<b>Empresa:</b> {nombre}<br>
<b>Nicho:</b> {niche}<br>
<b>Email:</b> {email or '—'}<br>
<b>Teléfono:</b> {telefono or '—'}<br>
{f'<b>WhatsApp:</b> <a href="{wa_url}">{wa_url}</a><br>' if wa_url else ''}
<b>Sitio web:</b> {f'<a href="{url_sitio}">{url_sitio}</a>' if url_sitio else '—'}<br>
<b>Ciudad:</b> {ciudad}<br><br>
<b>💬 Contexto:</b> {contexto or 'Lead directo'}<br>
<b>🌐 Propuesta web:</b> {'✅ Sí — generar mockup' if pain['web_proposal'] else '❌ No aplica'}<br><br>
<i>Ingresado automáticamente — IDEUSS Intake API v1.0</i>
"""
        pd_post("notes", {"content": nota, "deal_id": deal_id})

        # Actividad de seguimiento
        due = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        pd_post("activities", {
            "subject":  f"Seguimiento — {nombre} [{fuente_info['label']}]",
            "type":     "call",
            "due_date": due,
            "due_time": "10:00",
            "duration": "00:20",
            "done":     0,
            "deal_id":  deal_id,
            "note":     f"Señal: {pain['description']} | Prioridad: {fuente_info['prioridad']}",
        })
        log.info(f"  📅 Actividad: {due}")

    # ── 6. Notificación Telegram ──────────────────────────────────────────────
    prioridad_emoji = (
        "🔴" if fuente_info["prioridad"] == "muy_alta" else
        "🟠" if fuente_info["prioridad"] == "alta"     else "🟡"
    )

    tg_send(f"""{prioridad_emoji} *Nuevo lead — {fuente_info['label']}*

🏢 *{nombre}* | {niche}
📍 {ciudad}
📞 {telefono or '—'}  |  ✉️ {email or '—'}
🌐 {url_sitio or 'Sin web'}

🎯 *Señal detectada:*
_{pain['description']}_

💬 _{contexto[:100] if contexto else 'Lead directo'}_

🔗 Pipeline: AI Web Factory → Cualificado
""")
    log.info(f"  📱 Telegram OK")

    return {
        "ok":       True,
        "deal_id":  deal_id,
        "org_id":   org_id,
        "pain":     pain["name"],
        "fuente":   fuente,
        "ts":       ts,
    }


# ═════════════════════════════════════════════════════════════════════════════
# SERVIDOR HTTP
# ═════════════════════════════════════════════════════════════════════════════


# =============================================================================
# PIPELINE FORMULARIO FABRICA WEB
# =============================================================================
def process_webform(data: dict) -> dict:
    nombre   = (data.get("nombre_del_negocio") or data.get("nombre") or
                data.get("company") or data.get("name") or "").strip()
    contacto = (data.get("nombre_de_contacto") or data.get("contacto") or "").strip()
    email    = (data.get("email_de_contacto") or data.get("email") or "").strip()
    telefono = (data.get("whatsapp_telefono") or data.get("phone") or
                data.get("telefono") or "").strip()
    url      = (data.get("url_del_sitio_web_actual") or data.get("url_sitio") or
                data.get("website") or data.get("url") or "").strip()
    ciudad   = (data.get("ciudad_y_pais") or data.get("ciudad") or "Colombia").strip()
    if not nombre:
        raise ValueError("Campo nombre requerido")
    from datetime import timedelta
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M")
    due = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    log.info(f"Webform: {nombre} | {email} | {url or 'sin web'}")
    pain = run_diagnostic(url) if url else {
        "name": "sin_web", "description": "Sin sitio web registrado",
        "message": "Oportunidad de empezar desde cero con un sitio de alta conversion."
    }
    log.info(f"  Senal: [{pain['name']}] {pain['description']}")
    org_id    = pd_post("organizations", {"name": nombre})
    person_pl = {"name": contacto or f"Contacto {nombre}"}
    if org_id:    person_pl["org_id"] = org_id
    if email:     person_pl["email"]  = [{"value": email,    "label": "work", "primary": True}]
    if telefono:  person_pl["phone"]  = [{"value": telefono, "label": "work", "primary": True}]
    person_id = pd_post("persons", person_pl)
    deal_pl = {
        "title":       f"{nombre} | Fabrica Web",
        "pipeline_id": PIPELINE_ID,
        "stage_id":    146,
        "status":      "open",
    }
    if org_id:    deal_pl["org_id"]    = org_id
    if person_id: deal_pl["person_id"] = person_id
    deal_id = pd_post("deals", deal_pl)
    nota = (
        f"<b>Formulario Fabrica Web — {ts}</b><br><br>"
        f"<b>Negocio:</b> {nombre}<br><b>Contacto:</b> {contacto or '—'}<br>"
        f"<b>Email:</b> {email or 'No indicado'}<br><b>Tel:</b> {telefono or 'No indicado'}<br>"
        f"<b>Ciudad:</b> {ciudad}<br><b>Web actual:</b> {url or 'Sin sitio'}<br><br>"
        f"<b>Diagnostico:</b> [{pain['name']}] {pain['description']}<br>"
        f"{pain.get('message','')}<br><br>"
        f"<i>Siguiente paso: evaluacion y diagnostico — agendar kick-off.</i>"
    )
    if deal_id:
        http_post(f"{PD_BASE}/notes?api_token={PIPEDRIVE_API_KEY}",
                  {"content": nota, "deal_id": deal_id})
        http_post(f"{PD_BASE}/activities?api_token={PIPEDRIVE_API_KEY}", {
            "subject": f"Evaluacion sitio web — {nombre}", "type": "call",
            "due_date": due, "due_time": "10:00", "duration": "00:30",
            "deal_id": deal_id, "done": 0,
            "note": f"Formulario Fabrica Web. Email:{email} Tel:{telefono} Web:{url or 'sin web'}",
        })
    tg_send(
        f"Nuevo formulario Fabrica Web\n\n"
        f"{nombre}\n{contacto or ''}\n{email or 'Sin email'}\n"
        f"{telefono or 'Sin tel'}\n{url or 'Sin web'}\n{ciudad}\n\n"
        f"Senal: [{pain['name']}] {pain['description']}\n"
        f"Deal en Contacto Establecido | Actividad: {due}"
    )
    return {"deal_id": deal_id, "org_id": org_id, "person_id": person_id, "pain": pain["name"]}

class Handler(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass  # Usar nuestro propio logger

    def send_json(self, code: int, body: dict):
        payload = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {
                "status":    "ok",
                "service":   "IDEUSS Lead Intake API",
                "version":   "1.0",
                "pipeline":  f"AI Web Factory (ID={PIPELINE_ID})",
                "timestamp": datetime.now().isoformat(),
                "schema":    SCHEMA,
            })
        elif self.path == "/":
            self.send_json(200, {
                "service": "IDEUSS Lead Intake API v1.0",
                "endpoints": {
                    "POST /api/lead": "Recibir nuevo lead",
                    "GET  /health":   "Estado del servicio",
                }
            })
        else:
            self.send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path not in ("/api/lead", "/api/draft", "/api/webform"):
            self.send_json(404, {"error": "Endpoints: POST /api/lead | POST /api/draft | POST /api/webform"})
            return

        # ── /api/webform — Webhook formulario Fabrica Web ──────────────
        if self.path == "/api/webform":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json(400, {"error": "JSON invalido"})
                return
            self.send_json(202, {"status": "accepted", "message": "Formulario Fabrica Web recibido"})
            def run_webform():
                try:
                    result = process_webform(data)
                    log.info(f"Webform OK: {result}")
                except Exception as e:
                    log.error(f"Error webform: {e}", exc_info=True)
            threading.Thread(target=run_webform, daemon=True).start()
            return

        # ── /api/draft — crear borrador Gmail con email recién conseguido ─────
        if self.path == "/api/draft":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json(400, {"error": "JSON inválido"})
                return
            missing = [f for f in ["nombre", "email"] if not data.get(f)]
            if missing:
                self.send_json(400, {"error": f"Campos requeridos: {missing}"})
                return
            self.send_json(202, {"status": "accepted", "message": "Creando borrador en background"})
            def run_draft():
                try:
                    result = create_draft_for_lead(data)
                    log.info(f"✅ Draft: {data.get('nombre')} → {result}")
                except Exception as e:
                    log.error(f"❌ Error draft: {e}")
            threading.Thread(target=run_draft, daemon=True).start()
            return

        # Leer body
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)

        try:
            data = json.loads(body)
        except Exception:
            self.send_json(400, {"error": "Body debe ser JSON válido"})
            return

        # Validar campos requeridos
        missing = [f for f in ["fuente", "nombre"] if not data.get(f)]
        if missing:
            self.send_json(400, {
                "error":  f"Campos requeridos faltantes: {missing}",
                "schema": SCHEMA,
            })
            return

        # Responder inmediatamente — procesar en background
        self.send_json(202, {
            "status":  "accepted",
            "message": "Lead recibido — procesando en background",
            "nombre":  data.get("nombre"),
            "fuente":  data.get("fuente"),
        })

        def run():
            try:
                result = process_lead(data)
                log.info(f"✅ Completado: {data.get('nombre')} → deal={result.get('deal_id')}")
            except Exception as e:
                log.error(f"❌ Error en process_lead: {e}")

        threading.Thread(target=run, daemon=True).start()



# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINT /api/draft — Crear borrador Gmail cuando se consigue el email
# ═════════════════════════════════════════════════════════════════════════════

def create_draft_for_lead(data: dict) -> dict:
    """
    Crea borrador de email de diagnóstico en Gmail cuando se consigue
    el email de un prospecto que antes no lo tenía.

    Campos requeridos: nombre, email, niche
    Campos opcionales: telefono, url_sitio, ciudad, deal_id, pain_name, pain_message
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import base64 as _b64

    nombre      = data.get("nombre", "").strip()
    email       = data.get("email", "").strip()
    niche       = data.get("niche", "Empresa").strip()
    url_sitio   = data.get("url_sitio", "")
    ciudad      = data.get("ciudad", "Colombia")
    deal_id     = data.get("deal_id", "")
    pain_name   = data.get("pain_name", "sin_cita_online")
    pain_msg    = data.get("pain_message", "")

    if not nombre or not email:
        return {"ok": False, "error": "nombre y email son requeridos"}

    # Si no viene señal de dolor, ejecutar diagnóstico
    if not pain_msg and url_sitio:
        pain = run_diagnostic(url_sitio)
        pain_name = pain["name"]
        pain_msg  = pain["message"]
    elif not pain_msg:
        pain_msg = (
            "Sus procesos de atención, seguimiento y marketing dependen de "
            "tareas manuales. La automatización IA puede recuperar 15+ horas semanales."
        )

    # Firma y configuración
    SENDER_NAME  = "Alejandro Torres"
    AGENCY_NAME  = "IDEUSS — Agencia IA y Automatización"
    BOOKING_URL  = "https://www.ideuss.com/agendar-reuniones/"
    BRIEF_URL    = "https://www.ideuss.com/brief-sitio-web/"
    MARIA_URL    = "https://wa.me/573158451170170"
    SENDER_EMAIL = "ventas@ideuss.com"
    SENDER_PHONE = "(57)(315)8451170"

    city_short = ciudad.split(",")[0].strip()
    subject    = f"{nombre}: detectamos algo en su negocio que le puede estar costando clientes"

    WEB_SIGNALS = {"sin_web","web_desactualizada","sin_cita_online","sin_reseñas_gestionadas"}
    brief_block = ""
    if pain_name in WEB_SIGNALS:
        brief_block = f"""
<div style="background:#f0f7ff;border-left:4px solid #1a73e8;padding:16px 20px;
border-radius:4px;margin:20px 0">
<p style="margin:0 0 8px"><strong>🎁 Diagnóstico gratuito de su presencia digital</strong></p>
<p style="margin:0 0 12px;color:#555;font-size:14px">
Completando este breve formulario (2 minutos) recibirá una propuesta personalizada
de sitio web automatizado con <strong>CRM, WhatsApp y ChatBot con IA</strong> — sin costo.
</p>
<a href="{BRIEF_URL}" style="background:#1a73e8;color:#fff;padding:10px 24px;
border-radius:6px;text-decoration:none;font-weight:bold;display:inline-block">
📋 Solicitar diagnóstico gratuito
</a>
</div>"""

    body_html = f"""<html><body style="font-family:Arial,sans-serif;color:#333;max-width:600px">
<p>Cordial saludo,</p>
<p>Mi nombre es <strong>{SENDER_NAME}</strong>, Director General de
<strong>{AGENCY_NAME}</strong>.</p>
<p>Revisamos el negocio <strong>{nombre}</strong> en {city_short} y encontramos:</p>
<blockquote style="border-left:4px solid #f0a500;padding:12px 20px;
background:#fffbf0;margin:16px 0;border-radius:4px">
🎯 <strong>{pain_name.upper().replace("_"," ")}</strong><br><br>
{pain_msg}
</blockquote>
<p>En <strong>{AGENCY_NAME}</strong> resolvemos exactamente esto:</p>
<ul>
  <li>✅ Automatizar captación y seguimiento de clientes (CRM inteligente)</li>
  <li>✅ Agendar citas online 24/7 sin intervención humana</li>
  <li>✅ ChatBot con IA que atiende WhatsApp y web 24/7</li>
  <li>✅ Conectar marketing, ventas y operación en un sistema</li>
</ul>
{brief_block}
<p>Consulta con nuestra agente <strong>MarIA</strong> experta en Automatización:<br>
👉 <a href="{MARIA_URL}">{MARIA_URL}</a></p>
<p style="margin:24px 0">
<a href="{BOOKING_URL}" style="background:#1a73e8;color:#fff;padding:14px 28px;
border-radius:8px;text-decoration:none;font-weight:bold;display:inline-block;font-size:16px">
📅 Agendar reunión gratuita (30 min)
</a>
</p>
<p style="color:#888;font-size:12px">
<a href="{BOOKING_URL}" style="color:#888">{BOOKING_URL}</a>
</p>
<hr style="border:none;border-top:1px solid #eee;margin:24px 0">
<p style="font-size:13px;color:#555">
<strong>{SENDER_NAME}</strong> | Director General<br>
<strong>{AGENCY_NAME}</strong><br>
📱 {SENDER_PHONE} | 🇺🇸 +1(786)579 0043<br>
✉️ {SENDER_EMAIL}<br>
🌐 www.IDEUSS.com | www.AutoPrint365.com
</p>
</body></html>"""

    # Crear borrador via Gmail API (google_api.py no disponible en intake-api)
    # Usar OAuth token desde variable de entorno si está disponible
    import os, base64 as _b64
    google_token_b64 = os.environ.get("GOOGLE_TOKEN_B64", "")
    if not google_token_b64:
        return {"ok": False, "error": "GOOGLE_TOKEN_B64 no configurada — borrador no creado"}

    try:
        import json as _json
        import google.oauth2.credentials
        import googleapiclient.discovery

        token_data = _json.loads(_b64.b64decode(google_token_b64).decode())
        creds = google.oauth2.credentials.Credentials(
            token         = token_data.get("token"),
            refresh_token = token_data.get("refresh_token"),
            token_uri     = token_data.get("token_uri"),
            client_id     = token_data.get("client_id"),
            client_secret = token_data.get("client_secret"),
        )
        gmail = googleapiclient.discovery.build("gmail", "v1", credentials=creds)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        msg["To"]      = email
        msg.attach(MIMEText(body_html, "html"))

        raw = _b64.urlsafe_b64encode(msg.as_bytes()).decode()
        draft = gmail.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw, "threadId": None}}
        ).execute()

        draft_id = draft.get("id", "")
        log.info(f"  📝 Borrador creado: {draft_id} → {email}")

        # Actualizar actividad en Pipedrive si viene deal_id
        if deal_id and PIPEDRIVE_API_KEY:
            pd_post("notes", {
                "content":  f"📧 Borrador de email preparado para {email} — revisión pendiente",
                "deal_id":  deal_id,
            })

        # Notificar Telegram
        tg_send(
            f"📝 *Borrador preparado* — {nombre}\n\n"
            f"✉️ Para: `{email}`\n"
            f"🎯 Señal: _{pain_name.replace('_',' ')}_\n"
            f"📋 Asunto: {subject[:60]}\n\n"
            f"Revisa Gmail → Borradores para enviar."
        )

        return {"ok": True, "draft_id": draft_id, "email": email, "subject": subject}

    except Exception as e:
        log.error(f"❌ Error creando borrador: {e}")
        return {"ok": False, "error": str(e)}


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not PIPEDRIVE_API_KEY:
        log.warning("⚠️  PIPEDRIVE_API_KEY no configurada — los leads no se guardarán en Pipedrive")
    if not TELEGRAM_BOT_TOKEN:
        log.warning("⚠️  TELEGRAM_BOT_TOKEN no configurada — no habrá notificaciones")

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    log.info(f"🚀 IDEUSS Lead Intake API — puerto {PORT}")
    log.info(f"   POST http://0.0.0.0:{PORT}/api/lead")
    log.info(f"   GET  http://0.0.0.0:{PORT}/health")
    log.info(f"   Pipeline: AI Web Factory ID={PIPELINE_ID}")
    log.info(f"   Telegram: {TELEGRAM_CHAT_ID}")
    log.info("   Esperando leads...\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("🛑 Servicio detenido")
