import csv
import os
import subprocess
import requests
import re
import logging
from typing import List, Dict
from dataclasses import dataclass
from contextlib import contextmanager
import ipaddress
from pathlib import Path
import time

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EventoConfig:
    nombre: str
    titulo: str
    puerto: int
    tracker: str
    source: str
    host: str
    bitrate: int
    token: str

    def _es_ip_valida(self, ip: str) -> bool:
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    def _es_dominio_valido(self, dominio: str) -> bool:
        patron = r'^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
        return bool(re.match(patron, dominio))

    def validar(self) -> List[str]:
        errores = []
        if not self.nombre or not re.match(r'^[a-zA-Z0-9_-]+$', self.nombre):
            errores.append("Nombre inválido: use solo letras, números, guiones y guiones bajos")
        if not (1024 <= self.puerto <= 65535):
            errores.append("Puerto inválido: debe estar entre 1024 y 65535")
        if not self._es_ip_valida(self.host) and not self._es_dominio_valido(self.host):
            errores.append("Host inválido: debe ser una IP válida o un nombre de dominio")
        if not (0 <= self.bitrate <= 10000000):
            errores.append("Bitrate inválido")
        if not self.titulo.strip():
            errores.append("El título no puede estar vacío")
        if not self.source.strip():
            errores.append("La fuente no puede estar vacía")
        return errores


class EventoManager:
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        # En la definición de cabeceras
        self.cabeceras = ["name", "title", "port", "service_access_token", "tracker", "source", "host",
                  "bitrate", "content_id"]
        self._inicializar_csv()

    def _inicializar_csv(self):
        if not self.csv_path.exists():
            with self._abrir_csv('w') as file:
                writer = csv.writer(file)
                writer.writerow(self.cabeceras)

    @contextmanager
    def _abrir_csv(self, modo='r'):
        file = None
        try:
            file = open(self.csv_path, mode=modo, newline='', encoding='utf-8')
            yield file
        finally:
            if file:
                file.close()

    def _guardar_evento(self, evento: List[str]):
        # Verificar que el número de campos coincida con las cabeceras
        if len(evento) != len(self.cabeceras):
            raise ValueError(f"Número incorrecto de campos: esperado {len(self.cabeceras)}, recibido {len(evento)}")
            
        with self._abrir_csv('a') as file:
            writer = csv.writer(file)
            writer.writerow(evento)

    def _leer_eventos_csv(self) -> List[Dict[str, str]]:
        eventos = []
        with self._abrir_csv('r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                eventos.append(row)
        return eventos

    def limpiar_archivos_temporales(self, nombre_volumen: str) -> int:
        try:
            import platform
            if platform.system() == "Windows":
                ruta_base = Path(f"\\\\wsl.localhost\\docker-desktop\\mnt\\docker-desktop-disk\\data\\docker\\volumes\\{nombre_volumen}\\_data")
            elif platform.system() == "Linux":
                ruta_base = Path(f"/var/lib/docker/volumes/{nombre_volumen}/_data")
            else:
                logger.warning(f"Sistema operativo no soportado: {platform.system()}")
                return 0

            if not ruta_base.exists():
                logger.info(f"El directorio {ruta_base} no existe")
                return 0

            extensiones_permitidas = {".acelive", ".sauth"}
            archivos_eliminados = 0
            for archivo in ruta_base.iterdir():
                if archivo.is_file() and archivo.suffix not in extensiones_permitidas:
                    try:
                        archivo.unlink()
                        archivos_eliminados += 1
                        logger.debug(f"Archivo eliminado: {archivo}")
                    except Exception as e:
                        logger.error(f"Error al eliminar {archivo}: {e}")
            
            logger.info(f"Se eliminaron {archivos_eliminados} archivos temporales")
            return archivos_eliminados
        except Exception as e:
            logger.error(f"Error en limpieza de archivos temporales: {e}")
            return 0

    def ejecutar_docker(self, config: EventoConfig) -> None:
        comando = self._construir_comando_docker(config)
    
        # Imprimir el comando antes de ejecutarlo
        comando_str = " ".join(comando)
        print(f"Comando a ejecutar: {comando_str}")
        logger.info(f"Ejecutando comando: {comando_str}")

        try:
            resultado = subprocess.run(comando, check=True, capture_output=True, text=True)
            logger.info(f"Evento '{config.nombre}' iniciado correctamente")
            logger.debug(f"Salida del comando: {resultado.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al iniciar evento '{config.nombre}': {e.stderr}")
            raise RuntimeError(f"Error al iniciar Docker: {e.stderr}")


    def _construir_comando_docker(self, config: EventoConfig) -> List[str]:
        return [
            "docker", "run", "-d", "--restart", "unless-stopped",
            "--name", config.nombre,
            "-p", f"{config.puerto}:{config.puerto}/tcp",
            "-p", f"{config.puerto}:{config.puerto}/udp",
            "-v", f"acestreamengine_{config.nombre}:/data",
            "lob666/acestreamengine",
            "--port", str(config.puerto),
            "--tracker", config.tracker,
            "--stream-source",
            "--name", config.nombre,
            "--title", config.titulo,
            "--publish-dir", "/data",
            "--cache-dir", "/data",
            "--skip-internal-tracker",
            "--quality", "HD",
            "--category", "amateur",
            "--service-access-token", config.token,
            "--service-remote-access",
            "--log-debug", "1",
            "--max-peers", "6",
            "--max-upload-slots", "6",
            "--source-read-timeout", "15",
            "--source-reconnect-interval", "1",
            "--host", config.host,
            "--source", config.source,
            "--bitrate", str(config.bitrate)
        ]

    def obtener_monitor(self, contenedor_id: str, puerto: int, intentos: int = 10) -> Dict:
        tiempo_espera_inicial = 15  # segundos
        tiempo_entre_intentos = 5   # segundos
        
        logger.info(f"Esperando {tiempo_espera_inicial} segundos para el inicio del servicio...")
        time.sleep(tiempo_espera_inicial)
        
        for intento in range(intentos):
            try:
                response = requests.get(
                    f"http://localhost:{puerto}/app/{puerto}/monitor",
                    timeout=10
                )
                
                if response.status_code == 200:
                    try:
                        monitor_data = response.json()
                        content_id = monitor_data.get('content_id')
                        download_hash = monitor_data.get('download_hash')
                        
                        if content_id and content_id != 'No encontrado':
                            return {
                                'content_id': content_id,
                                'download_hash': download_hash
                            }
                        else:
                            logger.warning(f"Content ID vacío o no encontrado en el intento {intento + 1}")
                    except ValueError as e:
                        logger.warning(f"Error al decodificar JSON en el intento {intento + 1}: {e}")
                else:
                    logger.warning(f"Estado de respuesta inesperado ({response.status_code}) en el intento {intento + 1}")
                    
            except requests.RequestException as e:
                logger.warning(f"Reintentando obtener monitor ({intento + 1}/{intentos}): {e}")
            
            logger.info(f"Esperando {tiempo_entre_intentos} segundos antes del siguiente intento...")
            time.sleep(tiempo_entre_intentos)
        
        raise RuntimeError("No se pudo obtener el monitor después de varios intentos")

    def _actualizar_content_id(self, nombre: str, content_id: str):
        eventos = self._leer_eventos_csv()
        
        with self._abrir_csv('w') as file:
            writer = csv.writer(file)
            writer.writerow(self.cabeceras)
            for evento in eventos:
                if evento['name'] == nombre:
                    evento['content_id'] = content_id
                writer.writerow([evento.get(campo, '') for campo in self.cabeceras])

    def verificar_y_limpiar_contenedor(self, nombre: str) -> None:
        try:
            resultado = subprocess.run(
                ["docker", "ps", "-a", "--filter", f"name=^{nombre}$", "--format", "{{.ID}}"],
                capture_output=True,
                text=True,
                check=True
            )
            if resultado.stdout.strip():
                contenedor_id = resultado.stdout.strip()
                logger.info(f"Contenedor existente encontrado: {nombre} ({contenedor_id})")
                self.parar_contenedor(contenedor_id)
                self.borrar_contenedor(contenedor_id)
                logger.info(f"Contenedor {nombre} eliminado exitosamente")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al ejecutar comando Docker: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error al limpiar contenedor {nombre}: {e}")
            raise

    def parar_contenedor(self, contenedor_id: str) -> None:
        try:
            resultado = subprocess.run(
                ["docker", "stop", contenedor_id],
                check=True,
                capture_output=True,
                text=True
            )
            logger.debug(f"Contenedor detenido: {resultado.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al detener contenedor: {e.stderr}")
            raise RuntimeError(f"Error al detener contenedor: {e.stderr}")

    def borrar_contenedor(self, contenedor_id: str) -> None:
        try:
            resultado = subprocess.run(
                ["docker", "rm", contenedor_id],
                check=True,
                capture_output=True,
                text=True
            )
            logger.debug(f"Contenedor eliminado: {resultado.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al eliminar contenedor: {e.stderr}")
            raise RuntimeError(f"Error al eliminar contenedor: {e.stderr}")

    def crear_desde_csv(self, indices: List[int]) -> None:
        eventos = self._leer_eventos_csv()
        for idx in indices:
            if not (0 <= idx < len(eventos)):
                raise ValueError(f"Índice {idx + 1} fuera de rango")
            
            evento = eventos[idx]
            try:
                config = EventoConfig(
                    nombre=evento['name'],
                    titulo=evento['title'],
                    puerto=int(evento['port']),
                    tracker=evento['tracker'],
                    source=evento['source'],
                    host=evento['host'],
                    bitrate=int(evento['bitrate']),
                    token=evento['service_access_token']
                )
                
                errores = config.validar()
                if errores:
                    logger.error(f"Errores de validación para {config.nombre}: {errores}")
                    continue
                
                logger.info(f"Creando contenedor para evento: {config.nombre}")
                self.verificar_y_limpiar_contenedor(config.nombre)
                
                nombre_volumen = f"acestreamengine_{config.nombre}"
                archivos_eliminados = self.limpiar_archivos_temporales(nombre_volumen)
                logger.info(f"Archivos temporales eliminados: {archivos_eliminados}")
                
                self.ejecutar_docker(config)
                logger.info(f"Contenedor creado exitosamente para {config.nombre}")
                
                time.sleep(10)
                try:
                    resultado = subprocess.run(
                        ["docker", "ps", "--filter", f"name=^{config.nombre}$", "--format", "{{.ID}}"],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    contenedor_id = resultado.stdout.strip()
                    info = self.obtener_monitor(contenedor_id, config.puerto)
                    content_id = info.get('content_id', 'No encontrado')
                    download_hash = info.get('download_hash', 'No encontrado')
                    print(f"Content ID para {config.nombre}: {content_id}")
                    print(f"Hash para {config.nombre}: {download_hash}")
                    self._actualizar_content_id(config.nombre, content_id)
                except Exception as e:
                    logger.error(f"Error al obtener Content ID para {config.nombre}: {e}")
                    print(f"Error al obtener Content ID para {config.nombre}: {e}")
                    
            except Exception as e:
                logger.error(f"Error al procesar evento {evento.get('name', 'desconocido')}: {e}")
                print(f"Error al procesar evento {evento.get('name', 'desconocido')}: {e}")

    def mostrar_eventos_csv(self) -> List[Dict[str, str]]:
        eventos = self._leer_eventos_csv()
        if not eventos:
            print("\nNo hay eventos guardados en el CSV.")
            return []
            
        print("\nEventos guardados en CSV:")
        for i, evento in enumerate(eventos, 1):
            print(f"{i}. Nombre: {evento['name']} - {evento['title']} - {evento.get('content_id', 'Sin ID')}")
        return eventos

    def solicitar_datos_evento(self) -> EventoConfig:
        return EventoConfig(
            nombre=input("Nombre del evento (nuevo_unico): ").strip(),
            titulo=input("Título del canal: ").strip(),
            puerto=int(input("Puerto (unico)[8642]: ").strip() or "8642"),
            tracker=input("Tracker URL https://newtrackon.com/list [Enter default]: ").strip() or "udp://tracker.opentrackr.org:1337/announce",
            source=input("Fuente del contenido: ").strip(),
            host=input("IP pública: ").strip(),
            bitrate=int(input("Bitrate [Enter 697587]: ").strip() or "697587"),
            token=input("Token de acceso: ").strip() or "12345"
        )


def main():
    manager = EventoManager("eventos.csv")
    while True:
        try:
            print("\n=== Gestor de Eventos ===")
            print("1. Crear nuevo evento")
            print("2. Ver eventos guardados")
            print("3. Crear contenedores desde CSV")
            print("4. Salir")
            opcion = input("\nElija una opción: ").strip()

            if opcion == "1":
                config = manager.solicitar_datos_evento()
                errores = config.validar()
                if errores:
                    print("\nErrores encontrados:")
                    for error in errores:
                        print(f"- {error}")
                    continue
                try:
                    manager.verificar_y_limpiar_contenedor(config.nombre)
                    manager._guardar_evento([
                        config.nombre,
                        config.titulo,
                        str(config.puerto),
                        config.token,  # service_access_token
                        config.tracker,
                        config.source,
                        config.host,
                        str(config.bitrate),
                        ""  # Empty content_id initially                       
                    ])
                    # manager.ejecutar_docker(config)
                    print(f"\nEvento '{config.nombre}' creado y guardado exitosamente")
                except Exception as e:
                    print(f"\nError al crear evento: {e}")

            elif opcion == "2":
                manager.mostrar_eventos_csv()

            elif opcion == "3":
                eventos = manager.mostrar_eventos_csv()
                if not eventos:
                    print("No hay eventos guardados en el CSV.")
                else:
                    indices_str = input("\nIngrese los números de los eventos a crear (separados por comas): ").strip()
                    try:
                        indices = [int(idx.strip()) - 1 for idx in indices_str.split(',')]
                        manager.crear_desde_csv(indices)
                        print("\nContenedores creados exitosamente")
                    except Exception as e:
                        print(f"\nError al crear contenedores: {e}")

            elif opcion == "4":
                print("\n¡Hasta luego!")
                break

            else:
                print("\nOpción inválida")
        except Exception as e:
            print(f"\nError inesperado: {e}")
            logger.error(f"Error inesperado: {e}")


if __name__ == "__main__":
    main()