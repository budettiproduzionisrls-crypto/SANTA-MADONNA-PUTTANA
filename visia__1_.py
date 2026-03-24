"""
Script per esportare automaticamente il palinsesto da Visia (MySQL) a WordPress
VERSIONE OTTIMIZZATA: Cache in RAM, retry FTP/HTTP, context manager DB
Data: 2025
"""

import mysql.connector
from contextlib import contextmanager
from datetime import datetime, timedelta
from ftplib import FTP_TLS
import xml.etree.ElementTree as ET
import xml.dom.minidom
import logging
import time
import os
import re
import requests
import json

# ==================== CONFIGURAZIONE ====================

DB_CONFIG = {
    'host': '192.168.1.236',
    'user': 'visia_reader',
    'password': 'visia2025',
    'database': 'openlogic',
    'charset': 'utf8mb4'
}

FTP_HOST = "ftp.liratv.it"
FTP_USER = "2976010@aruba.it"
FTP_PASS = "Liratv1956@Ciak"
FTP_REMOTE_PATH = "/www.liratv.it/wp-content/xlmvisia/palinsesto.xml"

WP_SYNC_URL = "https://www.liratv.it/wp-content/xlmvisia/sync-visia.php"
WP_SYNC_TOKEN = "visia_secret_token_2025_xyz"

CHANNEL_ID = "LiraTV"
CHANNEL_NAME = "Lira TV"
ID_CANALE = 16
FPS = 25
ARCHIVE_DAYS = 7

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'export_epg_log.txt')
BACKUP_FILE = os.path.join(SCRIPT_DIR, 'palinsesto_backup.xml')
CACHE_DIR = os.path.join(SCRIPT_DIR, 'playlist_cache')
LAST_DATE_FILE = os.path.join(SCRIPT_DIR, 'last_processed_date.txt')

# ==================== CACHE IN MEMORIA ====================
# I giorni passati vengono caricati una sola volta e tenuti in RAM.
# Il giorno corrente viene sempre riletto (può cambiare durante la giornata).
_memory_cache = {}  # { date_yyyymmdd: [programs, ...] }

# ==================== SETUP LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
    logging.info(f"Creata directory cache: {CACHE_DIR}")

# ==================== FUNZIONI UTILITY ====================

def frames_to_seconds(frames):
    return frames / FPS

def get_today_date_yyyymmdd():
    return int(datetime.now().strftime('%Y%m%d'))

def get_last_processed_date():
    if os.path.exists(LAST_DATE_FILE):
        try:
            with open(LAST_DATE_FILE, 'r') as f:
                return int(f.read().strip())
        except Exception:
            return None
    return None

def save_last_processed_date(date_yyyymmdd):
    with open(LAST_DATE_FILE, 'w') as f:
        f.write(str(date_yyyymmdd))

def get_cache_filename(date_yyyymmdd):
    return os.path.join(CACHE_DIR, f"playlist_{date_yyyymmdd}.json")

def save_playlist_to_cache(date_yyyymmdd, programs):
    """Salva la playlist su disco E aggiorna la cache in memoria."""
    cache_file = get_cache_filename(date_yyyymmdd)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(programs, f, ensure_ascii=False, indent=2)
        _memory_cache[date_yyyymmdd] = programs
        logging.info(f"✅ Playlist salvata in cache: {cache_file}")
        return True
    except Exception as e:
        logging.error(f"❌ Errore salvataggio cache: {e}")
        return False

def load_playlist_from_cache(date_yyyymmdd, is_today=False):
    """
    Carica la playlist con priorità:
    1. RAM (solo per giorni passati)
    2. Disco JSON
    3. None (forza lettura DB)
    """
    # I giorni passati possono stare in RAM indefinitamente
    if not is_today and date_yyyymmdd in _memory_cache:
        programs = _memory_cache[date_yyyymmdd]
        logging.info(f"⚡ Cache RAM: {date_yyyymmdd} ({len(programs)} programmi)")
        return programs

    # Prova dal disco
    cache_file = get_cache_filename(date_yyyymmdd)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                programs = json.load(f)
            if not is_today:
                _memory_cache[date_yyyymmdd] = programs  # Promuovi in RAM
            logging.info(f"📦 Cache disco: {date_yyyymmdd} ({len(programs)} programmi)")
            return programs
        except Exception as e:
            logging.error(f"❌ Errore caricamento cache: {e}")
    return None

def cleanup_old_cache_files():
    """Cancella file cache più vecchi di ARCHIVE_DAYS giorni."""
    cutoff = datetime.now() - timedelta(days=ARCHIVE_DAYS)
    cutoff_int = int(cutoff.strftime('%Y%m%d'))
    removed = 0
    try:
        for fname in os.listdir(CACHE_DIR):
            if fname.startswith('playlist_') and fname.endswith('.json'):
                try:
                    date_int = int(fname.replace('playlist_', '').replace('.json', ''))
                    if date_int < cutoff_int:
                        os.remove(os.path.join(CACHE_DIR, fname))
                        _memory_cache.pop(date_int, None)
                        removed += 1
                except ValueError:
                    pass
        if removed:
            logging.info(f"🧹 Rimossi {removed} file cache vecchi")
    except Exception as e:
        logging.warning(f"⚠️  Pulizia cache fallita: {e}")

# ==================== FUNZIONI DATABASE ====================

@contextmanager
def db_connection():
    """Context manager per la connessione MySQL — garantisce chiusura anche in caso di errore."""
    conn = None
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            port=3306,
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset=DB_CONFIG['charset'],
            connection_timeout=10,
            use_pure=True
        )
        logging.info("✅ Connessione database MySQL riuscita")
        yield conn
    except mysql.connector.Error as e:
        logging.error(f"❌ Errore connessione database: {e}")
        yield None
    finally:
        if conn and conn.is_connected():
            conn.close()

def read_palinsesto_from_db(date_yyyymmdd=None):
    """Legge il palinsesto dal database per una data specifica."""
    if date_yyyymmdd is None:
        date_yyyymmdd = get_today_date_yyyymmdd()

    with db_connection() as conn:
        if not conn:
            return []

        try:
            cursor = conn.cursor(dictionary=True)
            logging.info(f"🔍 Ricerca palinsesto per data: {date_yyyymmdd}")

            query = """
            SELECT
                p.ID, p.Orario, p.Durata, p.DurataElaborazione,
                p.FrameIN, p.FrameOUT, p.Tipo_Oggetto, p.Stato, p.Ordine,
                prog.Titolo AS Programma_Titolo,
                prog.Descrizione AS Programma_Descrizione,
                prog.DescrizioneEpg, prog.TramaEpg, prog.EtaEpg,
                m.Titolo AS Marcatura_Titolo, m.Puntata, m.Tempo,
                cat.Codice AS Codice_Ministeriale
            FROM tbl_palinsesti p
            INNER JOIN tbl_programmi prog ON p.ID_Programma = prog.ID
            LEFT JOIN tbl_marcature m ON p.ID_Oggetto = m.ID AND p.Tipo_Oggetto = 1
            LEFT JOIN tbl_categorieministeriali cat ON prog.ID_Categoria = cat.ID
            WHERE p.ID_Canale = %s AND p.Data = %s AND prog.Eliminato = 0
            ORDER BY p.Ordine ASC, p.Orario ASC
            """

            cursor.execute(query, (ID_CANALE, date_yyyymmdd))
            results = cursor.fetchall()
            logging.info(f"📺 Trovati {len(results)} programmi")

            programs = []
            for row in results:
                title = row['Marcatura_Titolo'] or row['Programma_Titolo']
                description = row['DescrizioneEpg'] or row['Programma_Descrizione'] or title
                codice = row['Codice_Ministeriale'] or "NULLO"

                programs.append({
                    'orario_frame': row['Orario'],
                    'title': title,
                    'description': description,
                    'trama': row['TramaEpg'] or '',
                    'rating': codice,
                    'eta': row['EtaEpg'] or 0,
                    'duration_seconds': 0,
                    'puntata': row['Puntata'],
                    'tempo': row['Tempo'],
                    'durata_elaborazione': row['DurataElaborazione'],
                    'durata': row['Durata'],
                    'frame_in': row['FrameIN'],
                    'frame_out': row['FrameOUT']
                })

            # Calcola durate
            for i, program in enumerate(programs):
                dur = 0
                if program['durata_elaborazione'] and program['durata_elaborazione'] > 0:
                    dur = int(frames_to_seconds(program['durata_elaborazione']))
                elif program['frame_out'] and program['frame_in'] and program['frame_out'] > program['frame_in']:
                    dur = int(frames_to_seconds(program['frame_out'] - program['frame_in']))
                elif program['durata'] and program['durata'] > 0:
                    dur = int(frames_to_seconds(program['durata']))
                elif i < len(programs) - 1:
                    dur = int(frames_to_seconds(programs[i + 1]['orario_frame'] - program['orario_frame']))
                else:
                    dur = 1800
                program['duration_seconds'] = dur

            # Rimuovi campi temporanei
            for p in programs:
                for k in ('durata_elaborazione', 'durata', 'frame_in', 'frame_out'):
                    del p[k]

            cursor.close()

            # Filtra promo e accorpa televendite consecutive
            programs = filter_and_merge_programs(programs)

            return programs

        except mysql.connector.Error as e:
            logging.error(f"❌ Errore query database: {e}")
            return []

# ==================== FILTRO E ACCORPAMENTO ====================

def is_promo(title: str) -> bool:
    """Restituisce True se il titolo contiene la parola 'promo' (case-insensitive)."""
    return bool(re.search(r'\bpromo\b', title, re.IGNORECASE))

def is_televendita(title: str) -> bool:
    """Restituisce True se il titolo contiene 'televendita/e' (case-insensitive)."""
    return bool(re.search(r'\btelevendit[ae]\b', title, re.IGNORECASE))

def filter_and_merge_programs(programs: list) -> list:
    """
    1. Rimuove tutti i programmi il cui titolo contiene 'promo'.
    2. Accorpa le televendite consecutive in un unico slot chiamato 'Televendite'
       con durata sommata e orario di inizio della prima.
    Restituisce la lista trasformata.
    """
    # --- STEP 1: filtra promo ---
    filtered = [p for p in programs if not is_promo(p['title'])]
    removed_promo = len(programs) - len(filtered)
    if removed_promo:
        logging.info(f"🚫 Rimossi {removed_promo} programmi 'promo'")

    # --- STEP 2: accorpa televendite consecutive ---
    # Un programma corto (<= 60s) circondato da televendite viene inglobato nel blocco
    def is_short(p):
        return p['duration_seconds'] <= 60

    merged = []
    i = 0
    merged_groups = 0
    while i < len(filtered):
        prog = filtered[i]
        if is_televendita(prog['title']):
            # Raccogli tutte le televendite consecutive, inglobando programmi corti in mezzo
            group_duration = prog['duration_seconds']
            j = i + 1
            while j < len(filtered):
                nxt = filtered[j]
                if is_televendita(nxt['title']):
                    group_duration += nxt['duration_seconds']
                    j += 1
                elif is_short(nxt) and j + 1 < len(filtered) and is_televendita(filtered[j + 1]['title']):
                    # Programma corto in mezzo a televendite: inglobalo
                    logging.info(f"📦 Inglobato programma corto '{nxt['title']}' ({nxt['duration_seconds']}s) nel blocco televendite")
                    group_duration += nxt['duration_seconds']
                    j += 1
                else:
                    break
            count = j - i
            if count > 1:
                merged_groups += 1
                logging.info(f"📦 Accorpate {count} elementi → 'Televendite' ({group_duration}s)")
            merged.append({
                **prog,
                'title':            'Televendite',
                'description':      'Televendite',
                'trama':            '',
                'duration_seconds': group_duration,
            })
            i = j
        else:
            merged.append(prog)
            i += 1

    if merged_groups:
        logging.info(f"✅ Accorpamento: {merged_groups} gruppi televendite uniti")

    # --- STEP 3: accorpa programmi consecutivi con lo stesso titolo ---
    # Segmenti/marcature dello stesso programma vengono uniti in un unico slot
    if merged:
        accorpati = [merged[0]]
        merged_programs = 0
        for p in merged[1:]:
            last = accorpati[-1]
            if p['title'] == last['title']:
                # Stesso programma: somma durata, mantieni orario di inizio del primo
                last['duration_seconds'] += p['duration_seconds']
                # Mantieni la trama più lunga tra le due
                if len(p.get('trama', '') or '') > len(last.get('trama', '') or ''):
                    last['trama'] = p['trama']
                merged_programs += 1
            else:
                accorpati.append(p)
        if merged_programs:
            logging.info(f"✅ Accorpamento programmi: {merged_programs} segmenti uniti")
        merged = accorpati

    logging.info(f"📊 Programmi dopo filtro/accorpamento: {len(merged)} (erano {len(programs)})")
    return merged

# ==================== TROVA PROGRAMMA CORRENTE ====================

def get_current_programme():
    """
    Trova il programma in onda ora.
    Usa la cache (disco o RAM) invece di rileggere sempre il DB.
    Gestisce programmi corti (<50 sec) con logica smart.
    """
    current_date = get_today_date_yyyymmdd()

    # Usa cache disco per oggi (è infragiornaliero, non promuovere in RAM)
    programs = load_playlist_from_cache(current_date, is_today=True)
    if not programs:
        logging.info("Cache oggi non disponibile, leggo dal DB...")
        programs = read_palinsesto_from_db(current_date)
        if programs:
            save_playlist_to_cache(current_date, programs)

    if not programs:
        return None

    now = datetime.now()
    current_seconds = now.hour * 3600 + now.minute * 60 + now.second

    real_current_index = None
    for i, prog in enumerate(programs):
        start_s = int(frames_to_seconds(prog['orario_frame']))
        if start_s <= current_seconds < start_s + prog['duration_seconds']:
            real_current_index = i
            break

    if real_current_index is None:
        return None

    current_prog = programs[real_current_index]

    if current_prog['duration_seconds'] >= 50:
        current_prog['start_seconds'] = int(frames_to_seconds(current_prog['orario_frame']))
        current_prog['end_seconds'] = current_prog['start_seconds'] + current_prog['duration_seconds']
        return current_prog

    # Programma corto: logica smart
    logging.info(f"⚠️  Programma corto: {current_prog['title']} ({current_prog['duration_seconds']}s)")
    start_s = int(frames_to_seconds(current_prog['orario_frame']))
    progress = (current_seconds - start_s) / current_prog['duration_seconds'] * 100

    if progress < 50:
        candidates = range(real_current_index - 1, -1, -1)
    else:
        candidates = range(real_current_index + 1, len(programs))

    for i in candidates:
        if programs[i]['duration_seconds'] >= 50:
            p = programs[i]
            p['start_seconds'] = int(frames_to_seconds(p['orario_frame']))
            p['end_seconds'] = p['start_seconds'] + p['duration_seconds']
            return p

    # Fallback
    current_prog['start_seconds'] = start_s
    current_prog['end_seconds'] = start_s + current_prog['duration_seconds']
    return current_prog

# ==================== FUNZIONI XML ====================

def generate_multiday_xml(days_back=7):
    """
    Genera XML multi-giorno.
    I giorni passati vengono letti dalla cache RAM/disco.
    Solo oggi viene sempre riletto freschi.
    """
    logging.info(f"📝 Generazione XML multi-giorno ({days_back} giorni)")

    tv = ET.Element('tv')
    tv.set('generator-info-name', 'Visia MySQL Export - Multi Day Archive')

    channel = ET.SubElement(tv, 'channel')
    channel.set('id', CHANNEL_ID)
    ET.SubElement(channel, 'display-name').text = CHANNEL_NAME

    today = datetime.now().date()
    today_int = get_today_date_yyyymmdd()
    total_programs = 0

    for days_ago in reversed(range(days_back)):
        target_date = today - timedelta(days=days_ago)
        date_int = int(target_date.strftime('%Y%m%d'))
        is_today = (date_int == today_int)

        logging.info(f"📅 {target_date.strftime('%d/%m/%Y')} ({'oggi' if is_today else 'passato'})")

        programs = load_playlist_from_cache(date_int, is_today=is_today)

        if not programs:
            logging.info("   💾 Leggo dal database...")
            programs = read_palinsesto_from_db(date_int)
            if programs:
                save_playlist_to_cache(date_int, programs)

        if not programs:
            logging.warning("   ⚠️  Nessun programma trovato")
            continue

        logging.info(f"   ✅ {len(programs)} programmi")
        total_programs += len(programs)

        for program in programs:
            orario_s = int(frames_to_seconds(program['orario_frame']))
            start_dt = datetime.combine(target_date, datetime.min.time()) + timedelta(seconds=orario_s)
            end_dt = start_dt + timedelta(seconds=program['duration_seconds'])

            programme = ET.SubElement(tv, 'programme')
            programme.set('start', start_dt.strftime('%Y%m%d%H%M%S +0100'))
            programme.set('stop', end_dt.strftime('%Y%m%d%H%M%S +0100'))
            programme.set('channel', CHANNEL_ID)

            ET.SubElement(programme, 'title', lang='it').text = program['description']
            ET.SubElement(programme, 'desc', lang='it').text = program['trama'] or program['description']

            if program.get('puntata') and program.get('tempo'):
                ET.SubElement(programme, 'sub-title', lang='it').text = (
                    f"Puntata {program['puntata']} - Tempo {program['tempo']}"
                )

            rating_el = ET.SubElement(programme, 'rating', system='IT')
            ET.SubElement(rating_el, 'value').text = program['rating']

            if program.get('eta', 0) > 0:
                sr = ET.SubElement(programme, 'star-rating')
                ET.SubElement(sr, 'value').text = f"{program['eta']}+"

    logging.info(f"📊 Totale programmi nel XML: {total_programs}")

    xml_str = ET.tostring(tv, encoding='utf-8', xml_declaration=True)
    dom = xml.dom.minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ", encoding='utf-8')

# ==================== FTP CON RETRY ====================

def upload_to_ftp(xml_content, max_retries=3, retry_delay=3):
    """Carica il file XML su FTP con retry automatico (backoff lineare)."""
    from io import BytesIO

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"🌐 FTP tentativo {attempt}/{max_retries}...")
            ftp = FTP_TLS(FTP_HOST)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.prot_p()

            remote_dir = os.path.dirname(FTP_REMOTE_PATH)
            remote_file = os.path.basename(FTP_REMOTE_PATH)
            if remote_dir:
                ftp.cwd(remote_dir)

            file_obj = BytesIO(xml_content)
            file_obj.seek(0)
            ftp.storbinary(f'STOR {remote_file}', file_obj)
            ftp.quit()

            logging.info(f"✅ File caricato: {FTP_REMOTE_PATH}")
            return True

        except Exception as e:
            logging.warning(f"⚠️  FTP tentativo {attempt} fallito: {e}")
            if attempt < max_retries:
                wait = retry_delay * attempt
                logging.info(f"   Riprovo tra {wait}s...")
                time.sleep(wait)

    logging.error("❌ Upload FTP fallito dopo tutti i tentativi")
    return False

# ==================== WORDPRESS SYNC CON RETRY ====================

def trigger_wordpress_sync(mode='full', max_retries=3, retry_delay=5):
    """Chiama lo script WordPress con retry e timeout ridotto."""
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"🔄 WordPress sync (mode={mode}) tentativo {attempt}/{max_retries}...")
            url = f"{WP_SYNC_URL}?token={WP_SYNC_TOKEN}&mode={mode}"
            response = requests.get(url, timeout=120)

            logging.info(f"📊 Status: {response.status_code}")

            if response.status_code == 200:
                if "SINCRONIZZAZIONE COMPLETATA" in response.text:
                    logging.info("✅ WordPress sync completato")
                    return True
                else:
                    logging.warning("⚠️ HTTP 200 ma sync non eseguito!")
                    logging.warning(f"Risposta: {response.text[:300]}")
            else:
                logging.error(f"❌ HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            logging.warning(f"⚠️  Timeout tentativo {attempt}")
        except Exception as e:
            logging.warning(f"⚠️  Errore tentativo {attempt}: {e}")

        if attempt < max_retries:
            wait = retry_delay * attempt
            logging.info(f"   Riprovo tra {wait}s...")
            time.sleep(wait)

    logging.error("❌ WordPress sync fallito dopo tutti i tentativi")
    return False

# ==================== LOGICA PRINCIPALE ====================

def export_cycle_full():
    """Ciclo completo: legge DB, aggiorna cache, genera XML, carica FTP, sync WP."""
    logging.info("=" * 60)
    logging.info("🚀 AVVIO CICLO EXPORT PALINSESTO")
    logging.info("=" * 60)

    current_date = get_today_date_yyyymmdd()
    last_date = get_last_processed_date()

    if last_date != current_date:
        logging.info(f"📅 Cambio giorno: {last_date} → {current_date}")
        # Invalida cache RAM del giorno precedente (non più "oggi")
        _memory_cache.pop(last_date, None)

    # Leggi oggi dal DB e aggiorna cache (sempre, non solo al cambio giorno)
    programs = read_palinsesto_from_db(current_date)
    if not programs:
        logging.error("❌ Nessun programma trovato per oggi")
        return False

    logging.info(f"✅ Letti {len(programs)} programmi per oggi")
    save_playlist_to_cache(current_date, programs)

    if last_date != current_date:
        save_last_processed_date(current_date)
        cleanup_old_cache_files()

    xml_content = generate_multiday_xml(days_back=ARCHIVE_DAYS)

    try:
        with open(BACKUP_FILE, 'wb') as f:
            f.write(xml_content)
        logging.info(f"💾 Backup salvato: {BACKUP_FILE}")
    except Exception as e:
        logging.warning(f"⚠️  Impossibile salvare backup: {e}")

    if not upload_to_ftp(xml_content):
        logging.error("❌ Upload FTP fallito")
        return False

    if not trigger_wordpress_sync(mode='full'):
        logging.warning("⚠️  WordPress sync fallito, ma XML caricato")

    logging.info("✅ CICLO COMPLETATO!")
    logging.info("=" * 60)
    return True

def export_cycle_today():
    """Export veloce: solo oggi, usato al cambio programma."""
    logging.info("=" * 60)
    logging.info("📺 EXPORT OGGI - Cambio programma")
    logging.info("=" * 60)

    # Aggiorna cache oggi
    current_date = get_today_date_yyyymmdd()
    programs = read_palinsesto_from_db(current_date)
    if programs:
        save_playlist_to_cache(current_date, programs)

    xml_content = generate_multiday_xml(days_back=ARCHIVE_DAYS)

    if not upload_to_ftp(xml_content):
        logging.error("❌ Upload FTP fallito")
        return False

    if not trigger_wordpress_sync(mode='today'):
        logging.warning("⚠️  WordPress sync fallito, ma XML caricato")

    logging.info("✅ EXPORT OGGI COMPLETATO!")
    logging.info("=" * 60)
    return True

def export_cycle_batch():
    """Export batch: genera XML completo e sincronizza 1 giorno alla volta."""
    logging.info("=" * 60)
    logging.info("📦 EXPORT BATCH - 7 giorni separati")
    logging.info("=" * 60)

    xml_content = generate_multiday_xml(days_back=ARCHIVE_DAYS)

    if not upload_to_ftp(xml_content):
        logging.error("❌ Upload FTP fallito")
        return False

    success_count = 0
    failed_dates = []

    for days_ago in range(ARCHIVE_DAYS - 1, -1, -1):
        target_date = datetime.now() - timedelta(days=days_ago)
        date_yyyymmdd = target_date.strftime('%Y%m%d')
        date_display = target_date.strftime('%d/%m/%Y')

        logging.info(f"📅 Sync {date_display} ({date_yyyymmdd})...")

        try:
            url = f"{WP_SYNC_URL}?token={WP_SYNC_TOKEN}&date={date_yyyymmdd}"
            response = requests.get(url, timeout=120)

            if response.status_code == 200 and "SINCRONIZZAZIONE COMPLETATA" in response.text:
                logging.info(f"   ✅ {date_display} importato")
                success_count += 1
                time.sleep(2)
            else:
                logging.error(f"   ❌ {date_display} fallito: HTTP {response.status_code}")
                failed_dates.append(date_display)

        except Exception as e:
            logging.error(f"   ❌ {date_display} errore: {e}")
            failed_dates.append(date_display)

    logging.info("=" * 60)
    logging.info(f"📊 BATCH: ✅ {success_count}/{ARCHIVE_DAYS} importati")
    if failed_dates:
        logging.info(f"   ❌ Falliti: {', '.join(failed_dates)}")
    logging.info("=" * 60)

    return success_count > 0

def main():
    logging.info("=" * 60)
    logging.info("🚀 AVVIO SCRIPT VISIA EPG - MODALITÀ SMART OTTIMIZZATA")
    logging.info(f"📚 Giorni archivio: {ARCHIVE_DAYS}")
    logging.info(f"💾 Directory cache: {CACHE_DIR}")
    logging.info(f"⚡ Cache RAM attiva per giorni passati")
    logging.info("🌙 Mezzanotte: Sync completa 7 giorni")
    logging.info("📺 Giorno: Sync a cambio programma")
    logging.info("=" * 60)

    logging.info("🔄 SYNC FORZATO AVVIO")
    export_cycle_batch()

    while True:
        try:
            now = datetime.now()

            if now.hour == 0:
                logging.info("\n🌙 ORARIO MEZZANOTTE - Avvio sync completa")
                export_cycle_batch()

                next_run = now.replace(hour=1, minute=0, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - datetime.now()).total_seconds()
                logging.info(f"💤 Prossima esecuzione alle 01:00 (tra {int(wait_seconds)}s)")
                time.sleep(wait_seconds)

            else:
                current_prog = get_current_programme()

                if current_prog:
                    end_seconds = current_prog['end_seconds']
                    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=end_seconds)
                    end_time += timedelta(seconds=5)

                    if end_time > now:
                        wait_seconds = (end_time - datetime.now()).total_seconds()
                        logging.info(f"\n📺 In onda: {current_prog['title']}")
                        logging.info(f"⏰ Fine alle: {end_time.strftime('%H:%M:%S')} (tra {int(wait_seconds)}s)")
                        time.sleep(wait_seconds)

                    export_cycle_today()

                else:
                    logging.warning("⚠️  Nessun programma corrente, riprovo tra 5 minuti")
                    time.sleep(300)

        except KeyboardInterrupt:
            logging.info("\n⛔ Script interrotto (Ctrl+C)")
            break
        except Exception as e:
            logging.error(f"\n❌ ERRORE CRITICO: {e}")
            logging.info("🔄 Riprovo tra 1 minuto...")
            time.sleep(60)

if __name__ == "__main__":
    main()
