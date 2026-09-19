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
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

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


def fetch_pipedrive_person(person_id) -> dict | None:
    """Consulta una Person de Pipedrive por ID (webhooks v2 solo mandan el ID numérico)."""
    if not person_id or not PIPEDRIVE_API_KEY:
        return None
    try:
        url = f"https://api.pipedrive.com/v1/persons/{person_id}?api_token={PIPEDRIVE_API_KEY}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            res = json.loads(r.read())
            return res.get("data")
    except Exception as e:
        log.warning(f"fetch_pipedrive_person error: {e}")
        return None


def fetch_pipedrive_org(org_id) -> dict | None:
    """Consulta una Organization de Pipedrive por ID (webhooks v2 solo mandan el ID numérico)."""
    if not org_id or not PIPEDRIVE_API_KEY:
        return None
    try:
        url = f"https://api.pipedrive.com/v1/organizations/{org_id}?api_token={PIPEDRIVE_API_KEY}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as r:
            res = json.loads(r.read())
            return res.get("data")
    except Exception as e:
        log.warning(f"fetch_pipedrive_org error: {e}")
        return None


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


def find_existing_deal_for_email(email: str) -> dict | None:
    """Busca, dentro del pipeline AI Web Factory, un deal ya existente para este email.

    Evita crear una Organization/Person/Deal duplicada cuando el mismo prospecto
    vuelve a generar un lead (ej. corre el diagnóstico de procesos más de una vez,
    o llega por dos fuentes distintas con el mismo correo). Retorna None si no hay
    coincidencia (o si falla la búsqueda) — en ese caso el llamador crea todo nuevo,
    como ya hacía antes.
    """
    if not email or not PIPEDRIVE_API_KEY:
        return None
    try:
        search_url = (
            f"https://api.pipedrive.com/v1/persons/search"
            f"?term={urllib.parse.quote(email)}&fields=email&exact_match=true"
            f"&api_token={PIPEDRIVE_API_KEY}"
        )
        with urllib.request.urlopen(urllib.request.Request(search_url), context=SSL_CTX, timeout=10) as r:
            items = (json.loads(r.read()).get("data") or {}).get("items") or []
        if not items:
            return None
        person_id = items[0]["item"]["id"]

        deals_url = f"https://api.pipedrive.com/v1/persons/{person_id}/deals?api_token={PIPEDRIVE_API_KEY}"
        with urllib.request.urlopen(urllib.request.Request(deals_url), context=SSL_CTX, timeout=10) as r:
            deals = json.loads(r.read()).get("data") or []
        for d in deals:
            if d.get("pipeline_id") == PIPELINE_ID:
                org = d.get("org_id")
                return {
                    "deal_id":   d.get("id"),
                    "person_id": person_id,
                    "org_id":    org.get("value") if isinstance(org, dict) else org,
                }
        return None
    except Exception as e:
        log.warning(f"find_existing_deal_for_email error: {e}")
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

    # ── 2/3/4. Organización + Persona + Deal en pipeline AI Web Factory ───────
    # Buscar primero por email si ya existe un deal de este prospecto en el
    # pipeline — evita duplicar Org/Person/Deal si el mismo prospecto vuelve a
    # generar un lead (diagnóstico corrido más de una vez, u otra fuente con el
    # mismo correo).
    existing = find_existing_deal_for_email(email) if email else None
    if existing:
        org_id, person_id, deal_id = existing["org_id"], existing["person_id"], existing["deal_id"]
        log.info(f"  ♻️  Deal existente reutilizado (sin duplicar): {deal_id}")
    else:
        org_id = pd_post("organizations", {"name": nombre})
        log.info(f"  🏢 Org: {org_id}")

        person_payload = {"name": f"Contacto — {nombre}"}
        if org_id:   person_payload["org_id"] = org_id
        if email:    person_payload["email"]  = [{"value": email,    "label": "work", "primary": True}]
        if telefono: person_payload["phone"]  = [{"value": telefono, "label": "work", "primary": True}]
        person_id = pd_post("persons", person_payload)

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

    # ── 7. Borrador Gmail con diagnóstico + mockup (si hay email) ──────────────
    draft_result = {"ok": False}
    if email:
        try:
            draft_result = create_draft_for_lead({
                "nombre":       nombre,
                "email":        email,
                "niche":        niche,
                "url_sitio":    url_sitio,
                "ciudad":       ciudad,
                "deal_id":      deal_id,
                "pain_name":    pain["name"],
                "pain_message": pain.get("message", pain.get("description", "")),
            })
            if draft_result.get("ok"):
                log.info(f"  📝 Borrador Gmail creado: {draft_result.get('draft_id')}")
            else:
                log.warning(f"  ⚠️  Borrador Gmail no creado: {draft_result.get('error')}")
        except Exception as e:
            log.error(f"  ❌ Error creando borrador Gmail: {e}", exc_info=True)

    return {
        "ok":       True,
        "deal_id":  deal_id,
        "org_id":   org_id,
        "pain":     pain["name"],
        "fuente":   fuente,
        "ts":       ts,
        "draft_ok": draft_result.get("ok", False),
    }


# ═════════════════════════════════════════════════════════════════════════════
# SERVIDOR HTTP
# ═════════════════════════════════════════════════════════════════════════════


# =============================================================================
# OPENROUTER — Mockup avanzado StoryBrand (solo para brief completo)
# =============================================================================

def call_openrouter(model: str, system: str, user: str, max_tokens: int = 2000) -> str | None:
    """Llama a un modelo de OpenRouter y retorna el texto de la respuesta."""
    if not OPENROUTER_API_KEY:
        log.warning("OPENROUTER_API_KEY no configurada — sin copy/mockup avanzado")
        return None
    try:
        payload = json.dumps({
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            "max_tokens": max_tokens,
        }).encode()
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=payload,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type":  "application/json",
                "HTTP-Referer":  "https://ideuss.com",
                "X-Title":       "IDEUSS Mockup StoryBrand",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=45) as r:
            result = json.loads(r.read())
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        log.warning(f"OpenRouter error ({model}): {e}")
        return None


def generate_storybrand_copy(brief: dict) -> str | None:
    """
    Paso 1 — Genera el guión StoryBrand (Hero, Problema, Guía, Plan, CTA, Éxito/Fracaso)
    a partir de las respuestas reales del brief. Modelo gratis en etapa prospectiva.
    """
    nombre    = brief.get("empresa", "")
    nicho     = brief.get("actividadEconomica", "Empresa")
    buyer     = brief.get("buyerPersona", "")
    p_ext     = brief.get("problemaExterno", "")
    p_int     = brief.get("problemaInterno", "")
    p_fil     = brief.get("problemaFilosofico", "")
    experiencia  = brief.get("experiencia", "")
    testimonios  = brief.get("testimonios", "")
    diferencial  = brief.get("diferencial", "")
    paso1, paso2, paso3 = brief.get("paso1",""), brief.get("paso2",""), brief.get("paso3","")
    accion_p  = brief.get("accionPrincipal", "Agendar una llamada")
    accion_s  = brief.get("accionSecundaria", "")
    exito     = brief.get("exito", "")
    fracaso   = brief.get("fracaso", "")

    system = (
        "Actúa como un experto en Donald Miller's Building a StoryBrand. "
        "Responde SOLO con el texto estructurado en 6 secciones, sin explicaciones adicionales."
    )
    user = (
        f"Genera el texto para una landing page de {nombre} ({nicho}).\n\n"
        f"Cliente ideal: {buyer or 'no especificado'}\n"
        f"Problema externo: {p_ext or 'no especificado'}\n"
        f"Problema interno: {p_int or 'no especificado'}\n"
        f"Problema filosófico: {p_fil or 'no especificado'}\n"
        f"Experiencia/autoridad: {experiencia or 'no especificado'}\n"
        f"Testimonios: {testimonios or 'no especificado'}\n"
        f"Diferencial: {diferencial or 'no especificado'}\n"
        f"Plan: 1) {paso1 or '—'} 2) {paso2 or '—'} 3) {paso3 or '—'}\n"
        f"Acción principal: {accion_p}\n"
        f"Acción secundaria: {accion_s or 'no especificado'}\n"
        f"Éxito si resuelve: {exito or 'no especificado'}\n"
        f"Riesgo si no resuelve: {fracaso or 'no especificado'}\n\n"
        f"Estructura: 1. Hero (Titular claro + Subtitular + CTA), 2. El Problema "
        f"(Villano/Puntos de dolor), 3. Guía (Empatía y Autoridad), 4. El Plan (3 pasos simples), "
        f"5. Llamado a la Acción Directo y Transaccional, 6. Lo que está en juego (Éxito vs. Fracaso)."
    )
    # Etapa prospectiva: modelo gratis
    return call_openrouter("nvidia/nemotron-3.5-lightning:free", system, user, max_tokens=1200) or \
           call_openrouter("deepseek/deepseek-v4-flash-0731", system, user, max_tokens=1200) or \
           call_openrouter("deepseek/deepseek-chat", system, user, max_tokens=1200)


def generate_advanced_mockup_prompt(storybrand_copy: str, nombre: str, nicho: str) -> str | None:
    """
    Paso 2 — Convierte el guión StoryBrand en un prompt de imagen enriquecido
    y personalizado para FAL.ai (en vez de renderizar HTML real, ya que el
    contenedor de intake-api no tiene navegador headless instalado).
    Modelo gratis en etapa prospectiva. Resultado: mockup avanzado y
    personalizado con el copy real del cliente, no una plantilla genérica.
    """
    system = (
        "Eres un diseñador UX/UI experto que traduce copy StoryBrand en descripciones "
        "visuales detalladas para generación de imágenes con IA (FAL.ai / Flux). "
        "Responde SOLO con el prompt de imagen en inglés, una sola línea, sin explicaciones ni markdown."
    )
    user = (
        f"Toma el siguiente texto StoryBrand para '{nombre}' ({nicho}) y conviértelo en un prompt "
        f"detallado en INGLÉS para generar un mockup de sitio web realista con FAL.ai. El prompt debe "
        f"describir: header con logo y CTA en naranja (#f0a500), hero section con el titular y subtítulo "
        f"reales del copy, sección de problema/villano, sección de guía/autoridad con los testimonios "
        f"reales, tarjetas con el plan de 3 pasos reales, y CTA final. Diseño limpio, minimalista, "
        f"mucho espacio en blanco, tipografía sans-serif — estilo agencia de automatización premium.\n\n"
        f"TEXTO STORYBRAND:\n{storybrand_copy}"
    )
    prompt = call_openrouter("google/gemini-2.5-flash", system, user, max_tokens=800) or \
             call_openrouter("qwen/qwen-2.5-coder-32b-instruct", system, user, max_tokens=800)
    if prompt:
        prompt = prompt.strip().strip('"')
    return prompt


def generate_fal_mockup_from_prompt(prompt: str) -> str | None:
    """Genera la imagen con FAL.ai a partir de un prompt ya construido (mockup avanzado)."""
    fal_key = os.environ.get("FAL_KEY", "")
    if not fal_key or not prompt:
        return None
    try:
        payload = json.dumps({
            "prompt":      prompt,
            "image_size":  "portrait_4_3",
            "num_images":  1,
            "enable_safety_checker": False,
        }).encode()
        req = urllib.request.Request(
            "https://fal.run/fal-ai/flux/schnell",
            data=payload,
            headers={"Authorization": f"Key {fal_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as r:
            result = json.loads(r.read())
        images = result.get("images", [])
        if images:
            return images[0].get("url", "") or None
    except Exception as e:
        log.warning(f"FAL error (mockup avanzado): {e}")
    return None


def generate_advanced_mockup(brief: dict) -> str | None:
    """
    Orquesta el flujo completo de mockup avanzado (brief completo):
    Paso 1 (copy StoryBrand) → Paso 2 (prompt enriquecido) → FAL.ai (imagen).
    Si OpenRouter no está configurado o falla, retorna None (el caller debe
    hacer fallback a generate_fal_mockup genérico).
    """
    nombre = brief.get("empresa", "Empresa")
    nicho  = brief.get("actividadEconomica", "Empresa")

    copy_sb = generate_storybrand_copy(brief)
    if not copy_sb:
        log.warning("  No se pudo generar copy StoryBrand — fallback a mockup genérico")
        return None

    prompt = generate_advanced_mockup_prompt(copy_sb, nombre, nicho)
    if not prompt:
        log.warning("  No se pudo generar prompt avanzado — fallback a mockup genérico")
        return None

    url = generate_fal_mockup_from_prompt(prompt)
    if url:
        log.info(f"  🎨 Mockup AVANZADO (OpenRouter+FAL) generado: {url[:60]}...")
    return url


# =============================================================================
# MOCKUP DE PRODUCCIÓN — HTML/Tailwind real + screenshot (SOLO uso interno IDEUSS)
#
# Se dispara MANUALMENTE cuando el cliente confirma el pedido e inicia la fase
# de producción — no es parte del flujo automático de prospección/brief.
# Genera código HTML/Tailwind real y editable (a diferencia del mockup
# avanzado del brief, que solo genera una imagen vía FAL sin código real).
# El HTML se guarda en el servidor; SOLO IDEUSS puede descargarlo desde el
# link entregado por Telegram + nota de Pipedrive — el cliente nunca recibe
# el código, solo la vista previa (imagen) en su comunicación.
# =============================================================================

PRODUCTION_MOCKUPS_DIR = "/app/production_mockups"


def generate_production_html(storybrand_copy: str, nombre: str) -> str | None:
    """
    Genera el HTML/Tailwind real (editable) a partir del copy StoryBrand.
    Etapa de producción: usa modelos premium (mejor calidad, ya no gratis)
    porque el cliente ya confirmó y esto es la base real de su sitio.
    """
    system = (
        "Eres un desarrollador frontend senior experto en Tailwind CSS. "
        "Responde SOLO con el código HTML completo y válido — incluye "
        "<script src=\"https://cdn.tailwindcss.com\"></script> en el <head> — "
        "sin explicaciones, sin markdown, sin fences de código, listo para "
        "guardar directamente como archivo .html y abrir en un navegador."
    )
    user = (
        f"Toma el siguiente texto StoryBrand para '{nombre}' y conviértelo en una Landing Page "
        f"HTML completa, responsiva, estilizada con Tailwind CSS. Requisitos: "
        f"diseño limpio y moderno con identidad de marca IDEUSS (acentos en naranja #f0a500, "
        f"fondo blanco, tipografía sans-serif), header con logo placeholder y navegación, "
        f"hero section con el titular/subtítulo/CTA reales del copy, sección de problema con "
        f"iconos, sección de autoridad/testimonios, tarjetas para el plan de 3 pasos, sección "
        f"de éxito vs. riesgo, y CTA final destacado. Debe verse como un sitio real terminado, "
        f"no un boceto — usa contenido real del texto, sin placeholders tipo 'lorem ipsum'.\n\n"
        f"TEXTO STORYBRAND:\n{storybrand_copy}"
    )
    # Etapa de producción: modelos premium (cliente ya confirmó, calidad > costo)
    html = call_openrouter("anthropic/claude-sonnet-4.5", system, user, max_tokens=6000) or \
           call_openrouter("openai/gpt-4o", system, user, max_tokens=6000) or \
           call_openrouter("google/gemini-2.5-flash", system, user, max_tokens=6000)
    if html:
        html = html.strip()
        html = re.sub(r'^```(?:html)?\s*', '', html)
        html = re.sub(r'\s*```$', '', html)
    return html


def render_html_to_screenshot(html: str, output_path: str) -> bool:
    """
    Renderiza el HTML real en Chromium headless (Playwright) y guarda un
    screenshot .png — para tener una vista previa visual del sitio real,
    sin depender de FAL.ai ni de texto generado dentro de una imagen.
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.set_content(html, wait_until="networkidle", timeout=30000)
            page.screenshot(path=output_path, full_page=True)
            browser.close()
        return True
    except Exception as e:
        log.warning(f"Playwright screenshot error: {e}")
        return False


def generate_production_mockup(brief: dict, deal_id) -> dict:
    """
    Orquesta el flujo de producción: copy StoryBrand → HTML real (Tailwind,
    premium) → guarda .html en disco → screenshot .png para preview.
    Retorna dict con paths locales; el caller es responsable de notificar
    a IDEUSS (Telegram + nota Pipedrive) con el link de descarga interno,
    NUNCA se envía al cliente.
    """
    os.makedirs(PRODUCTION_MOCKUPS_DIR, exist_ok=True)
    nombre = brief.get("empresa", "Empresa")

    copy_sb = generate_storybrand_copy(brief)
    if not copy_sb:
        return {"ok": False, "error": "No se pudo generar copy StoryBrand"}

    html = generate_production_html(copy_sb, nombre)
    if not html:
        return {"ok": False, "error": "No se pudo generar HTML de producción"}

    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', nombre)[:40]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_filename = f"{safe_name}_{ts}.html"
    html_path = os.path.join(PRODUCTION_MOCKUPS_DIR, html_filename)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    png_filename = f"{safe_name}_{ts}.png"
    png_path = os.path.join(PRODUCTION_MOCKUPS_DIR, png_filename)
    screenshot_ok = render_html_to_screenshot(html, png_path)

    return {
        "ok": True,
        "html_filename": html_filename,
        "png_filename": png_filename if screenshot_ok else None,
        "download_url_html": f"https://intake.ideuss.com/production_mockups/{html_filename}",
        "download_url_png":  f"https://intake.ideuss.com/production_mockups/{png_filename}" if screenshot_ok else None,
    }


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
    existing_deal_id = data.get("deal_id")  # Si viene del webhook de Pipedrive, el deal YA existe
    if not nombre:
        raise ValueError("Campo nombre requerido")
    from datetime import timedelta
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M")
    due = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    log.info(f"Webform: {nombre} | {email} | {url or 'sin web'} | deal existente:{existing_deal_id}")
    pain = run_diagnostic(url) if url else {
        "name": "sin_web", "description": "Sin sitio web registrado",
        "message": "Oportunidad de empezar desde cero con un sitio de alta conversion."
    }
    log.info(f"  Senal: [{pain['name']}] {pain['description']}")

    if existing_deal_id:
        # El deal ya lo creó Pipedrive (formulario web) — solo actualizarlo, no duplicar
        deal_id = existing_deal_id
        org_id, person_id = None, None
        http_put(f"https://api.pipedrive.com/v1/deals/{deal_id}?api_token={PIPEDRIVE_API_KEY}", {"stage_id": 146})
    else:
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
        http_post(f"https://api.pipedrive.com/v1/notes?api_token={PIPEDRIVE_API_KEY}",
                  {"content": nota, "deal_id": deal_id})
        http_post(f"https://api.pipedrive.com/v1/activities?api_token={PIPEDRIVE_API_KEY}", {
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

    # ── Borrador Gmail con el diagnóstico ─────────────────────────────────────
    draft_result = {"ok": False}
    if email:
        try:
            draft_result = create_draft_for_lead({
                "nombre":       nombre,
                "email":        email,
                "niche":        "Empresa",
                "url_sitio":    url,
                "ciudad":       ciudad,
                "deal_id":      deal_id,
                "pain_name":    pain["name"],
                "pain_message": pain.get("message", pain.get("description", "")),
            })
            if draft_result.get("ok"):
                log.info(f"  Borrador Gmail creado: {draft_result.get('draft_id')}")
            else:
                log.warning(f"  Borrador Gmail no creado: {draft_result.get('error')}")
        except Exception as e:
            log.error(f"  Error creando borrador Gmail: {e}", exc_info=True)

    return {"deal_id": deal_id, "org_id": org_id, "person_id": person_id,
            "pain": pain["name"], "draft_ok": draft_result.get("ok", False)}

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
        elif self.path.startswith("/production_mockups/"):
            # Descarga interna del mockup de producción (HTML/PNG). Protegido
            # con token simple por query string (?token=...) — solo IDEUSS
            # recibe este link completo por Telegram/Pipedrive; nunca se
            # comparte con el cliente.
            self._serve_production_file()
        else:
            self.send_json(404, {"error": "Not found"})

    def _serve_production_file(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        token = (qs.get("token") or [""])[0]
        expected = os.environ.get("PRODUCTION_DOWNLOAD_TOKEN", "")
        if not expected or token != expected:
            self.send_json(403, {"error": "Token inválido o faltante"})
            return
        filename = os.path.basename(parsed.path)  # evita path traversal
        filepath = os.path.join(PRODUCTION_MOCKUPS_DIR, filename)
        if not os.path.isfile(filepath):
            self.send_json(404, {"error": "Archivo no encontrado"})
            return
        content_type = "text/html; charset=utf-8" if filename.endswith(".html") else "image/png"
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path not in ("/api/lead", "/api/draft", "/api/webform", "/api/brief-mockup", "/api/production-mockup"):
            self.send_json(404, {"error": "Endpoints: POST /api/lead | POST /api/draft | POST /api/webform | POST /api/brief-mockup | POST /api/production-mockup"})
            return

        # ── /api/production-mockup — HTML real de producción (SOLO uso interno) ──
        # Se llama MANUALMENTE (no automático) cuando el cliente confirma el
        # pedido. Requiere el mismo shape de brief que /api/brief-mockup, más
        # un deal_id de Pipedrive para notificar el resultado.
        if self.path == "/api/production-mockup":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json(400, {"error": "JSON inválido"})
                return
            missing = [f for f in ["empresa"] if not data.get(f)]
            if missing:
                self.send_json(400, {"error": f"Campos requeridos: {missing}"})
                return
            self.send_json(202, {"status": "accepted", "message": "Generando mockup de producción (HTML real) en background — puede tardar 2-3 minutos"})
            def run_production(brief=data):
                try:
                    deal_id = brief.get("deal_id")
                    log.info(f"🏗️  Producción HTML real: {brief.get('empresa')}")
                    result = generate_production_mockup(brief, deal_id)
                    token = os.environ.get("PRODUCTION_DOWNLOAD_TOKEN", "")
                    if result.get("ok"):
                        html_link = f"{result['download_url_html']}?token={token}" if token else result["download_url_html"]
                        png_link  = f"{result['download_url_png']}?token={token}" if (token and result.get('download_url_png')) else result.get("download_url_png")
                        msg = (
                            f"🏗️ Mockup de PRODUCCIÓN listo — {brief.get('empresa')}\n\n"
                            f"📄 Descargar HTML: {html_link}\n" +
                            (f"🖼️ Preview PNG: {png_link}\n" if png_link else "") +
                            f"\n⚠️ Uso interno IDEUSS — no compartir el link HTML con el cliente."
                        )
                        tg_send(msg)
                        if deal_id:
                            http_post(f"https://api.pipedrive.com/v1/notes?api_token={PIPEDRIVE_API_KEY}",
                                      {"content": msg.replace("\n", "<br>"), "deal_id": deal_id})
                        log.info(f"✅ Producción completada: {brief.get('empresa')} → {result['html_filename']}")
                    else:
                        log.error(f"❌ Error producción: {result.get('error')}")
                        tg_send(f"❌ Error generando mockup de producción para {brief.get('empresa')}: {result.get('error')}")
                except Exception as e:
                    log.error(f"❌ Error /api/production-mockup: {e}", exc_info=True)
            threading.Thread(target=run_production, daemon=True).start()
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

            # Webhook de Pipedrive envuelve el payload en meta + data (v2) o meta + current (v1)
            meta    = data.get("meta", {})
            current = data.get("data") or data.get("current") or data  # v2=data, v1=current, fallback=directo

            action     = meta.get("action", "")
            obj_type   = meta.get("object", "")
            deal_title = (current.get("title") or "") if isinstance(current, dict) else ""

            # Acciones válidas de creación: v1="added", v2="create"
            CREATE_ACTIONS = ("added", "create")

            # Solo procesar: create de deal con prefijo WebIA O payload directo del formulario
            is_pipedrive_wh = bool(meta.get("action"))
            if is_pipedrive_wh:
                if obj_type != "deal" or action not in CREATE_ACTIONS:
                    self.send_json(200, {"status": "ignored", "reason": f"{action}.{obj_type} no aplicable"})
                    return
                if not deal_title.startswith("WebIA"):
                    self.send_json(200, {"status": "ignored", "reason": "Deal sin prefijo WebIA"})
                    return
                # Extraer datos del deal de Pipedrive
                person = current.get("person_id") or {}
                org    = current.get("org_id") or {}
                # En v2 person_id/org_id pueden venir solo como ID numérico (no objeto) —
                # si es así, se consultan por separado.
                if isinstance(person, (int, str)) or not isinstance(person, dict):
                    person = fetch_pipedrive_person(person) or {}
                if isinstance(org, (int, str)) or not isinstance(org, dict):
                    org = fetch_pipedrive_org(org) or {}
                wf_data = {
                    "nombre":   (org.get("name") if isinstance(org, dict) else "") or deal_title.replace("WebIA | ","").replace("WebIA ",""),
                    "email":    (person.get("email", [{}])[0].get("value","") if isinstance(person, dict) and person.get("email") else ""),
                    "telefono": (person.get("phone", [{}])[0].get("value","") if isinstance(person, dict) and person.get("phone") else ""),
                    "deal_id":  current.get("id"),
                }
            else:
                # Payload directo (test o formulario externo)
                wf_data = current

            self.send_json(202, {"status": "accepted", "message": "Formulario Fabrica Web procesando"})
            def run_webform(d=wf_data):
                try:
                    result = process_webform(d)
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

        # ── /api/brief-mockup — mockup AVANZADO (OpenRouter + FAL) para el
        # brief completo de agente.ideuss.com/api/brief. Se llama por separado
        # (fire-and-forget) desde route.ts justo después de crear/actualizar
        # el deal en Pipedrive, pasando el brief completo + deal_id + email. ──
        if self.path == "/api/brief-mockup":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json(400, {"error": "JSON inválido"})
                return
            missing = [f for f in ["empresa", "email"] if not data.get(f)]
            if missing:
                self.send_json(400, {"error": f"Campos requeridos: {missing}"})
                return
            self.send_json(202, {"status": "accepted", "message": "Generando mockup avanzado en background"})
            def run_brief_mockup(brief=data):
                try:
                    log.info(f"🎨 Brief mockup avanzado: {brief.get('empresa')} | {brief.get('email')}")
                    advanced_url = generate_advanced_mockup(brief)
                    draft_result = create_draft_for_lead({
                        "nombre":              brief.get("empresa"),
                        "email":               brief.get("email"),
                        "niche":               brief.get("actividadEconomica", "Empresa"),
                        "url_sitio":           brief.get("sitioActualUrl", ""),
                        "ciudad":              brief.get("ciudadPais", "Colombia"),
                        "deal_id":             brief.get("deal_id"),
                        "pain_name":           "brief_completado",
                        "pain_message": (
                            f"Con base en su brief, identificamos que {brief.get('problemaExterno','su negocio')} "
                            f"es la barrera práctica que más le está costando — y ya tenemos una propuesta "
                            f"de sitio web lista para mostrarle."
                        ),
                        "advanced_mockup_url": advanced_url,
                    })
                    log.info(f"✅ Brief mockup completado: {brief.get('empresa')} → "
                             f"avanzado={'sí' if advanced_url else 'no (fallback genérico)'} draft={draft_result.get('ok')}")
                except Exception as e:
                    log.error(f"❌ Error brief-mockup: {e}", exc_info=True)
            threading.Thread(target=run_brief_mockup, daemon=True).start()
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

def generate_fal_mockup(name: str, niche: str, city: str) -> str | None:
    """
    Genera un mockup de sitio web con FAL.ai para incluir en el email.
    Retorna la URL pública de la imagen o None si falla.
    """
    fal_key = os.environ.get("FAL_KEY", "")
    if not fal_key:
        log.warning("FAL_KEY no configurada — sin mockup")
        return None

    niche_lower = (niche or "").lower()
    if "dental" in niche_lower:
        paleta = "white with medical blue (#1a73e8) accents and a subtle warm orange (#f0a500) CTA button"
        hero_img = "smiling patient in dental chair with confident doctor"
        headline = f"Tu Clínica Dental de Confianza en {city}"
    elif "veterinari" in niche_lower:
        paleta = "warm green (#2e7d32) and white with a warm orange (#f0a500) CTA button"
        hero_img = "happy pet owner with dog and friendly veterinarian"
        headline = f"Cuidamos a tu Mascota en {city}"
    elif "estética" in niche_lower or "gym" in niche_lower or "fitness" in niche_lower:
        paleta = "rose gold (#c2185b) and white with a warm orange (#f0a500) CTA button"
        hero_img = "fit person in modern gym with trainer"
        headline = f"Tu Centro de Bienestar en {city}"
    elif "spa" in niche_lower or "bienestar" in niche_lower:
        paleta = "soft gold (#f9a825) and white, consistent warm orange (#f0a500) CTA button"
        hero_img = "relaxed woman in luxury spa treatment"
        headline = f"Tu Spa y Centro de Bienestar en {city}"
    elif "óptica" in niche_lower or "optometría" in niche_lower:
        paleta = "light blue (#0288d1) and grey with a warm orange (#f0a500) CTA button"
        hero_img = "person trying modern glasses in bright optical store"
        headline = f"Tu Óptica de Confianza en {city}"
    elif "médic" in niche_lower or "clínica" in niche_lower:
        paleta = "medical blue (#1565c0) and white with a warm orange (#f0a500) CTA button"
        hero_img = "professional doctor with patient in modern clinic"
        headline = f"Tu Consulta Médica en {city}"
    else:
        paleta = "clean white background with warm orange (#f0a500) accents and CTA button — IDEUSS brand style"
        hero_img = "professional business team in modern office"
        headline = f"{name} — Tu Empresa en {city}"

    prompt = (
        f"Professional modern website mockup screenshot for '{name}' business in {city} Colombia. "
        f"Color scheme: {paleta}. Clean minimalist design, generous white space, sans-serif typography — "
        f"style consistent with a premium automation agency landing page. "
        f"Header: logo placeholder left, navigation center, 'RESERVAR CITA' CTA button right in warm orange. "
        f"Hero section: {hero_img}, headline '{headline}', subtitle about quality service. "
        f"Trust bar: 4.9 Google stars, number of clients, WhatsApp button, Online booking. "
        f"3 service cards with icons. Testimonials section with client photos. "
        f"WhatsApp floating button. Professional footer with contact info. "
        f"Realistic website screenshot, high quality, no watermarks."
    )

    try:
        payload = json.dumps({
            "prompt":      prompt,
            "image_size":  "portrait_4_3",
            "num_images":  1,
            "enable_safety_checker": False,
        }).encode()

        req = urllib.request.Request(
            "https://fal.run/fal-ai/flux/schnell",
            data=payload,
            headers={
                "Authorization": f"Key {fal_key}",
                "Content-Type":  "application/json",
            },
            method="POST"
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=60) as r:
            result = json.loads(r.read())

        images = result.get("images", [])
        if images:
            url = images[0].get("url", "")
            if url:
                log.info(f"  Mockup FAL generado: {url[:60]}...")
                return url
    except Exception as e:
        log.warning(f"FAL error: {e}")
    return None


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

    # ── Mockup: avanzado (brief con OpenRouter) o genérico (evaluación gratuita) ──
    mockup_block = ""
    mockup_url = data.get("advanced_mockup_url") or generate_fal_mockup(nombre, niche, city_short)
    is_advanced = bool(data.get("advanced_mockup_url"))
    if mockup_url:
        etiqueta = "Mockup avanzado de su nueva propuesta de sitio web" if is_advanced else \
                   f"Así podría verse el sitio web de <strong>{nombre}</strong>"
        pie = "Diseño personalizado basado en su brief — listo para refinar en la reunión." if is_advanced else \
              "Diseño conceptual generado por IDEUSS — personalizable con su identidad de marca."
        mockup_block = f"""
<div style="margin:24px 0;text-align:center">
<p style="font-weight:bold;color:#333;margin:0 0 12px">
  🖥️ {etiqueta}:
</p>
<img src="{mockup_url}" alt="Mockup {nombre}"
     style="width:100%;max-width:560px;border-radius:8px;
            box-shadow:0 4px 16px rgba(0,0,0,0.15);border:1px solid #e0e0e0"/>
<p style="margin:8px 0 0;font-size:12px;color:#888;font-style:italic">
  {pie}
</p>
</div>"""

    # ── Embudo de marketing (argumento CRM Twenty — Growth/Scale) ─────────────
    embudo_block = f"""
<div style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:10px;padding:20px 24px;margin:24px 0">
<p style="margin:0 0 12px;font-weight:bold;font-size:15px;color:#333">
  🎯 Lo que realmente incluye: un embudo de ventas completo
</p>
<p style="margin:0 0 14px;font-size:13px;color:#555;line-height:1.6">
  No es solo un sitio web — es un sistema que acompaña a cada visitante hasta que se convierte en cliente:
</p>
<table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:10px">
<tr>
  <td style="padding:8px 10px;background:#e8f0fe;border-radius:6px 0 0 6px;width:25%;text-align:center">
    <strong>🔍 Atracción</strong><br><span style="color:#666">Su sitio y redes<br>atraen visitantes</span>
  </td>
  <td style="padding:8px 10px;background:#fff3cd;width:25%;text-align:center">
    <strong>💬 Interés</strong><br><span style="color:#666">El agente IA<br>conversa y cualifica</span>
  </td>
  <td style="padding:8px 10px;background:#d1ecf1;width:25%;text-align:center">
    <strong>📋 Decisión</strong><br><span style="color:#666">Twenty CRM organiza<br>el seguimiento</span>
  </td>
  <td style="padding:8px 10px;background:#d4edda;border-radius:0 6px 6px 0;width:25%;text-align:center">
    <strong>🔁 Fidelización</strong><br><span style="color:#666">Seguimiento postventa<br>y recompra</span>
  </td>
</tr>
</table>
<p style="margin:0;font-size:12px;color:#777;line-height:1.6">
  Con <strong>Twenty CRM propio</strong> (incluido en los planes Growth y Scale), cada contacto que llega por
  WhatsApp o su sitio queda registrado y clasificado automáticamente — usted sabe en todo momento cuántos
  visitantes tiene, cuántos están interesados y cuántos están listos para comprar. Eso es lo que separa un
  sitio web de un sistema de ventas medible.
</p>
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
{mockup_block}
{embudo_block}
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

        _clean_b64 = google_token_b64.strip().replace("\n", "").replace("\r", "").replace(" ", "")
        try:
            _decoded_bytes = _b64.b64decode(_clean_b64)
        except Exception as e:
            raise ValueError(f"GOOGLE_TOKEN_B64 no es base64 válido ({len(_clean_b64)} chars): {e}")
        token_data = _json.loads(_decoded_bytes.decode("utf-8", errors="strict"))
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
