from datetime import datetime
import json
import os

class GestionCheckpoint:
    def __init__(self, ruta_del_archivo="checkpoint.json"):
        """
        Iniciador del gestor del archivo de checkpoint
        
        Argumentos:
            ruta_del_archivo: ruta del archivo donde se guardan los checkpoints
        """
        self.ruta_del_archivo = ruta_del_archivo
        self.checkpoint = self.cargar_checkpoint()
    
    def cargar_checkpoint(self):
        """
        Cargar el archivo checkpoint si es que existe 
        """
        if os.path.exists(self.ruta_del_archivo):
            try: 
                with open(self.ruta_del_archivo, "r") as archivo:
                    return json.load(archivo)
            except json.JSONDecodeError:
                print("Error al cargar el archivo de checkpoint")
                return self.crear_checkpoint()
            except Exception as e:
                print(f"Error: {e}")
                return self.crear_checkpoint()
        else:
            return self.crear_checkpoint()
    
    def crear_checkpoint(self):
        """
        Crea un nuevo archivo de checkpoint vacío
        """
        return {
            "ultimo_id_procesado": None,
            "ids_procesados": [],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_procesados": 0
        }
    
    def guardar_checkpoint(self):
        """
        Guarda el checkpoint en un archivo json
        """
        with open(self.ruta_del_archivo, "w") as archivo:
            json.dump(self.checkpoint, archivo, indent=4)
    
    def actualizar_checkpoint(self, ultimo_id_procesado, total_procesados):
        """
        Actualiza el checkpoint con el último id procesado y el total de registros procesados
        """
        self.checkpoint["ultimo_id_procesado"] = ultimo_id_procesado
        self.checkpoint["total_procesados"] = total_procesados
        self.checkpoint["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.guardar_checkpoint()
    
    def obtener_ultimo_id_procesado(self):
        """
        Obtiene el último id procesado del checkpoint
        """
        return self.checkpoint["ultimo_id_procesado"]
    
    def obtener_total_procesados(self):
        """
        Obtiene el total de registros procesados del checkpoint
        """
        return self.checkpoint["total_procesados"]
    
    def obtener_ids_procesados(self):
        """
        Obtiene los ids de los registros procesados del checkpoint
        """
        return self.checkpoint.get("ids_procesados", [])
    
    def obtener_info(self, clave=None):
        """
        Obtiene información del checkpoint
        
        Args:
            clave: Si se proporciona, devuelve solo el valor de esa clave
        
        Returns:
            El checkpoint completo o el valor de la clave especificada
        """
        if clave is not None:
            return self.checkpoint.get(clave)
        return self.checkpoint
    
    def agregar_id_procesado(self, id_elemento):
        """
        Agrega un id a la lista de procesados sin guardar el checkpoint
        
        Args:
            id_elemento: ID del elemento a agregar 
            
        Returns:
            True si se agregó el id, False si ya existía
        """
        if "ids_procesados" not in self.checkpoint:
            self.checkpoint["ids_procesados"] = []
            
        id_str = str(id_elemento)
        if id_str not in self.checkpoint["ids_procesados"]:
            self.checkpoint["ids_procesados"].append(id_str)
            self.checkpoint["ultimo_id_procesado"] = id_str
            return True
        else:
            return False
    
    def es_procesado(self, id_elemento):
        """
        Verifica si el id ya había sido procesado
        
        Args:
            id_elemento: ID del elemento a verificar
            
        Returns:
            True si el id ya había sido procesado, False en caso contrario
        """
        return str(id_elemento) in self.checkpoint.get("ids_procesados", [])
    
    def reiniciar(self):
        """
        Reinicia el checkpoint al estado inicial
        """
        self.checkpoint = self.crear_checkpoint()
        self.guardar_checkpoint()
        return True