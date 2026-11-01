import base64
import logging
import mimetypes
import os
import pickle
import threading
import uuid
from email.message import EmailMessage
from pathlib import Path

from c_correos import empacadoras, fabricas_pro
from c_msg import msg_pro_correo, puesto
from flask import Flask, flash, redirect, render_template, send_from_directory, url_for
from flask_socketio import SocketIO
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# =========================================================
# CONFIGURACIÓN
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

TOKEN_FILE = BASE_DIR / "token_gmail.pickle"
SECRET_FILE = BASE_DIR / ".flask_secret"

ARCHIVOS = Path("D:/mi_pdf/CV/camaron_pdf")

PAUSA_ENTRE_CORREOS = 25
MAX_ADJUNTOS_BYTES = 20 * 1024 * 1024

htmlx = "app.html"

correos = empacadoras

titulo = "Dave Garcia | Tecnologo en Sistema"
asunto = f"{puesto} - {titulo}"
msg = msg_pro_correo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def obtener_clave_secreta():
    clave = os.environ.get("FLASK_SECRET_KEY")
    if clave:
        return clave

    # Guarda una clave estable para las sesiones de Flask.
    try:
        with SECRET_FILE.open("xb") as archivo:
            archivo.write(os.urandom(32))
    except FileExistsError:
        pass

    clave = SECRET_FILE.read_bytes()

    if len(clave) < 32:
        raise RuntimeError(f"El archivo {SECRET_FILE} debe contener al menos 32 bytes.")

    return clave


app = Flask(__name__, template_folder="templates")
app.config["SECRET_KEY"] = obtener_clave_secreta()

socketio = SocketIO(app, async_mode="threading", manage_session=False)

# Evita iniciar dos lotes simultáneos en este proceso.
envio_lock = threading.Lock()


# =========================================================
# CREDENCIALES DE GMAIL
# =========================================================


def crear_servicio_gmail():
    if not TOKEN_FILE.is_file():
        raise FileNotFoundError(f"No se encontró el token de Gmail: {TOKEN_FILE}")

    # Carga únicamente un archivo pickle propio y de confianza.
    with TOKEN_FILE.open("rb") as archivo:
        credenciales = pickle.load(archivo)

    if credenciales.expired and credenciales.refresh_token:
        credenciales.refresh(Request())

        temporal = TOKEN_FILE.with_suffix(".pickle.tmp")

        try:
            with temporal.open("wb") as archivo:
                pickle.dump(credenciales, archivo)

            os.replace(temporal, TOKEN_FILE)
        finally:
            temporal.unlink(missing_ok=True)

    if not credenciales.valid:
        raise RuntimeError("Las credenciales de Gmail no son válidas. Debes volver a autorizar la cuenta.")

    return build("gmail", "v1", credentials=credenciales, cache_discovery=False)


# =========================================================
# DESTINATARIOS Y ADJUNTOS
# =========================================================


def preparar_destinatarios():
    if isinstance(correos, str):
        raise ValueError("'correos' debe ser una lista de direcciones, no un texto.")

    resultado = []
    vistos = set()

    for valor in correos:
        if not isinstance(valor, str):
            raise ValueError(f"Destinatario inválido: {valor!r}")

        correo = valor.strip()

        # Validación básica; Gmail hará la validación definitiva.
        if not correo or any(caracter.isspace() for caracter in correo) or correo.count("@") != 1 or not all(correo.split("@")):
            raise ValueError(f"Correo inválido: {valor!r}")

        clave = correo.casefold()

        if clave not in vistos:
            vistos.add(clave)
            resultado.append(correo)

    if not resultado:
        raise ValueError("No hay destinatarios para enviar.")

    return tuple(resultado)


def obtener_adjuntos():
    if not ARCHIVOS.is_dir():
        raise FileNotFoundError(f"No existe la carpeta de adjuntos: {ARCHIVOS}")

    rutas = sorted(
        (ruta for ruta in ARCHIVOS.iterdir() if ruta.is_file()),
        key=lambda ruta: ruta.name.casefold(),
    )

    if not rutas:
        raise ValueError("La carpeta está vacía. Se canceló el envío para evitar correos sin adjuntos.")

    return rutas


def cargar_adjuntos():
    adjuntos = []
    total_bytes = 0

    for ruta in obtener_adjuntos():
        limite_restante = MAX_ADJUNTOS_BYTES - total_bytes

        with ruta.open("rb") as archivo:
            contenido = archivo.read(limite_restante + 1)

        total_bytes += len(contenido)

        if total_bytes > MAX_ADJUNTOS_BYTES:
            raise ValueError("Los adjuntos superan el límite configurado de 20 MiB.")

        if not contenido:
            raise ValueError(f"El archivo está vacío: {ruta.name}")

        tipo, _ = mimetypes.guess_type(ruta.name)
        tipo = tipo or "application/octet-stream"
        tipo_principal, subtipo = tipo.split("/", 1)

        adjuntos.append((ruta.name, contenido, tipo_principal, subtipo))

    return tuple(adjuntos)


def crear_mensaje(destinatario, asunto_envio, html, adjuntos):
    mensaje = EmailMessage()
    mensaje["To"] = destinatario
    mensaje["Subject"] = asunto_envio

    mensaje.set_content("Este mensaje contiene una versión HTML y archivos adjuntos.")
    mensaje.add_alternative(html, subtype="html")

    for nombre, contenido, tipo_principal, subtipo in adjuntos:
        mensaje.add_attachment(
            contenido,
            maintype=tipo_principal,
            subtype=subtipo,
            filename=nombre,
        )

    raw = base64.urlsafe_b64encode(mensaje.as_bytes()).decode("ascii")

    return {"raw": raw}


# =========================================================
# EVENTOS Y ENVÍO EN SEGUNDO PLANO
# =========================================================


def emitir_evento(nombre, datos):
    try:
        socketio.emit(nombre, datos)
    except Exception:
        logger.exception("No se pudo emitir el evento %s", nombre)


def procesar_envio(lote, destinatarios, asunto_envio, html, adjuntos):
    servicio = None
    enviados = 0
    fallidos = 0
    error_general = None
    total = len(destinatarios)

    try:
        emitir_evento("process_status", {"status": "started", "job_id": lote, "total": total})

        # Cada tarea utiliza su propio cliente de Gmail.
        servicio = crear_servicio_gmail()

        for indice, destinatario in enumerate(destinatarios, start=1):
            try:
                cuerpo = crear_mensaje(destinatario, asunto_envio, html, adjuntos)

                # Sin reintentos automáticos para reducir el riesgo
                # de duplicados ante una respuesta de red incierta.
                (servicio.users().messages().send(userId="me", body=cuerpo).execute(num_retries=0))

                enviados += 1
                estado = "success"

                logger.info("Correo enviado a %s", destinatario)

            except Exception:
                fallidos += 1
                estado = "failure"
                logger.exception("Envío no confirmado para %s", destinatario)

            emitir_evento("progress_update", {"job_id": lote, "recipient": destinatario, "sent": enviados, "failed": fallidos, "current": indice, "total": total, "progress": round(indice * 100 / total, 2), "status": estado})

            # No espera después del último destinatario.
            if indice < total:
                socketio.sleep(PAUSA_ENTRE_CORREOS)

    except Exception as error:
        error_general = str(error)
        logger.exception("Se interrumpió el lote %s", lote)

    finally:
        try:
            if servicio is not None:
                servicio.close()
        except Exception:
            logger.exception("No se pudo cerrar el cliente de Gmail")

        envio_lock.release()

        emitir_evento("process_status", {"status": "error" if error_general else "finished", "job_id": lote, "total": total, "sent": enviados, "failed": fallidos, "pending": total - enviados - fallidos, "error": error_general})


@app.route("/uploads/<path:filename>")
def uploads(filename):
    return send_from_directory(str(ARCHIVOS), filename)


@app.route("/")
def index():
    try:
        archivos_para_vista = [{"filename": ruta.name, "filepath": str(ruta)} for ruta in obtener_adjuntos()]

    except (OSError, ValueError) as error:
        archivos_para_vista = []
        flash(str(error), "warning")

    return render_template(htmlx, files=archivos_para_vista, emails=correos, asuntox=asunto)


@app.route("/send_email", methods=["POST"])
def send_email():
    if not envio_lock.acquire(blocking=False):
        flash("Ya hay un envío en curso. Espera a que termine.", "warning")
        return redirect(url_for("index"))

    try:
        destinatarios = preparar_destinatarios()

        # Lee los archivos una sola vez para todo el lote.
        adjuntos = cargar_adjuntos()

        if not TOKEN_FILE.is_file():
            raise FileNotFoundError(f"No se encontró el token de Gmail: {TOKEN_FILE}")

        lote = uuid.uuid4().hex

        socketio.start_background_task(procesar_envio, lote, destinatarios, asunto, msg, adjuntos)

    except Exception as error:
        envio_lock.release()

        logger.exception("No se pudo iniciar el envío")

        flash(f"No se inició el envío: {error}", "danger")
        return redirect(url_for("index"))

    flash(f"Envío iniciado para {len(destinatarios)} destinatarios. Adjuntos: {len(adjuntos)}.", "info")

    return redirect(url_for("index"))


if __name__ == "__main__":
    socketio.run(app, host="127.0.0.1", port=5000, debug=False, use_reloader=False)
