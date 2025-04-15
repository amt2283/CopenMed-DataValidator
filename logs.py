import logging
import os
from datetime import datetime

# modulo 5
class GestionLogs:
    def __init__(self, nombre_logger="verificador_medico", nivel=logging.INFO, ruta_logs="logs"):
        """
        Inicializa el gestor de logs
        
        Args:
            nombre_logger: Nombre del logger
            nivel: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            ruta_logs: Carpeta donde se guardarán los archivos de log
        """
        self.nombre_logger = nombre_logger
        
        # Crear directorio de logs si no existe
        if not os.path.exists(ruta_logs):
            os.makedirs(ruta_logs)
        
        # Configurar el logger
        self.logger = logging.getLogger(nombre_logger)
        self.logger.setLevel(nivel)
        
        # Evitar duplicación de handlers
        if not self.logger.handlers:
            # Formato del log
            formato = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            # Handler para consola
            self.console_handler = logging.StreamHandler()
            self.console_handler.setFormatter(formato)
            self.logger.addHandler(self.console_handler)
            
            # Handler para archivo
            fecha_actual = datetime.now().strftime("%Y%m%d")
            self.file_handler = logging.FileHandler(
                f"{ruta_logs}/{nombre_logger}_{fecha_actual}.log", 
                encoding='utf-8'
            )
            self.file_handler.setFormatter(formato)
            self.logger.addHandler(self.file_handler)
    
    def set_nivel(self, nivel):
        """
        Establece el nivel de logs o los desactiva completamente
        
        Args:
            nivel: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL) o "NONE" para desactivar
        """
        if nivel == "NONE":
            # Desactivar todos los handlers
            for handler in self.logger.handlers[:]:
                handler.setLevel(logging.CRITICAL + 1)  # Un nivel imposible de alcanzar
            self.logger.setLevel(logging.CRITICAL + 1)
        else:
            # Restaurar nivel normal en handlers
            nivel_num = getattr(logging, nivel) if isinstance(nivel, str) else nivel
            for handler in self.logger.handlers[:]:
                handler.setLevel(nivel_num)
            self.logger.setLevel(nivel_num)
            
    def debug(self, mensaje):
        """Registra un mensaje de nivel DEBUG"""
        self.logger.debug(mensaje)
    
    def info(self, mensaje):
        """Registra un mensaje de nivel INFO"""
        self.logger.info(mensaje)
    
    def warning(self, mensaje):
        """Registra un mensaje de nivel WARNING"""
        self.logger.warning(mensaje)
    
    def error(self, mensaje):
        """Registra un mensaje de nivel ERROR"""
        self.logger.error(mensaje)
    
    def critical(self, mensaje):
        """Registra un mensaje de nivel CRITICAL"""
        self.logger.critical(mensaje)