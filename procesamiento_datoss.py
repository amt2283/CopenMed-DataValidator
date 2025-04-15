import os
import json
import time
import requests
from datetime import datetime

import pandas as pd

# modulo 4 despues de checkpoint_manager.py
from checkpoint_manager import CheckpointManager
from config import CONFIG

class VerificadorRelaciones:
    def __init__(self, config=CONFIG):
        """
        Inicializa el verificador de relaciones médicas utilizando Ollama

        Args:
            config: Diccionario con configuración del procesamiento
        """
        self.config = config
        self.modelo = config.get("modelo", "deepseek")  # Usar el modelo de la configuración
        self.host = "http://localhost:11434"  # Host de Ollama local
        self.batch_size = config.get("batch_size", 32)
        self.max_procesar = config.get("max_procesar", 5)
        self.checkpoint_manager = CheckpointManager(path=config.get("ruta_checkpoint", "checkpoint.json"))
        
        self._verificar_modelo()
        
    def _verificar_modelo(self):
        try:
            response = requests.post(
                f"{self.host}/api/generate",
                json={"model": self.modelo, "prompt": "Test"}
            )
            if response.status_code != 200:
                raise Exception(f"Modelo {self.modelo} no disponible en Ollama")
            print(f"✅ Modelo {self.modelo} conectado correctamente")
        except Exception as e:
            print(f"❌ Error: {e}")
            exit(1)
    
    # La función verificar_relacion se mantiene sin cambios
    def verificar_relacion(self, id_relacion, entidad1, tipo_relacion, entidad2):
        prompt = f"""
        Como sistema experto en medicina, evalúa si esta relación es médicamente válida:
        
        ID: {id_relacion}
        Entidad 1: {entidad1}
        Relación: {tipo_relacion}
        Entidad 2: {entidad2}
        
        Si "{entidad1}" implica "{entidad2}" es médicamente correcto, responde solamente "VÁLIDO".
        Si NO es correcto, responde "INVÁLIDO" y explica brevemente por qué.
        
        Basa tu respuesta únicamente en conocimiento médico establecido.
        """
        
        try:
            time.sleep(0.5)
            response = requests.post(
                f"{self.host}/api/generate",
                json={
                    "model": self.modelo,
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0.1
                }
            )
            if response.status_code == 200:
                respuesta = response.json()["response"]
                if "VÁLIDO" in respuesta.upper() and "INVÁLIDO" not in respuesta.upper():
                    validez = "válido"
                    justificacion = ""
                else:
                    validez = "inválido"
                    partes = respuesta.upper().split("INVÁLIDO", 1)
                    justificacion = partes[1].strip() if len(partes) > 1 else respuesta
                return {
                    "id": id_relacion,
                    "entidad1": entidad1,
                    "relacion": tipo_relacion,
                    "entidad2": entidad2,
                    "validez": validez,
                    "justificacion": justificacion if validez == "inválido" else ""
                }
            else:
                print(f"❌ Error en API para ID {id_relacion}: {response.status_code}")
                return {"id": id_relacion, "validez": "error", "justificacion": f"Error API: {response.status_code}"}
        except Exception as e:
            print(f"❌ Excepción al procesar ID {id_relacion}: {str(e)}")
            return {"id": id_relacion, "validez": "error", "justificacion": f"Error: {str(e)}"}

    def procesar_datos(self, datos):
        """
        Procesa los datos y utiliza la información del checkpoint para evitar reprocesar relaciones.
        Se detecta de forma automática el nombre de la columna de identificación y el campo del elemento relacionado,
        en función del formato del DataFrame (nuevos datos TXT con 7 columnas o formato original de 4 columnas).

        Args:
            datos (DataFrame o lista): Los datos a procesar.

        Returns:
            tuple: (resultados, total_relaciones_procesadas)
        """
        if not isinstance(datos, pd.DataFrame):
            datos = pd.DataFrame(datos)
        
        # Detectar de forma automática el nombre de la columna para el identificador.
        # Si se usó el formato original se esperará "ID", de lo contrario se usa "Linea".
        id_col = "ID" if "ID" in datos.columns else ("Linea" if "Linea" in datos.columns else None)
        if id_col is None:
            raise Exception("No se encontró la columna de identificación en los datos.")
            
        # Para el elemento relacionado: preferir "Elemento Relacionado" y si no, "ElementoRelacionado"
        elem_col = "Elemento Relacionado" if "Elemento Relacionado" in datos.columns else (
                    "ElementoRelacionado" if "ElementoRelacionado" in datos.columns else None)
        if elem_col is None:
            raise Exception("No se encontró la columna del elemento relacionado en los datos.")
            
        # Otras columnas que se esperan están en ambos formatos:
        for col in ["Entidad", "Relación"]:
            if col not in datos.columns:
                raise Exception(f"No se encontró la columna '{col}' en los datos.")
        
        ids_procesados = set(self.checkpoint_manager.checkpoint.get("ids_procesados", []))
        datos_filtrados = datos[~datos[id_col].astype(str).isin(ids_procesados)]
        
        if self.max_procesar and len(datos_filtrados) > self.max_procesar:
            print(f"ℹ️ Limitando a {self.max_procesar} relaciones por ejecución")
            datos_filtrados = datos_filtrados.head(self.max_procesar)
        
        if len(datos_filtrados) == 0:
            print("ℹ️ No hay nuevas relaciones para procesar")
            return [], 0
        
        print(f"🔍 Procesando {len(datos_filtrados)} relaciones...")
        resultados = []
        nuevos_ids_procesados = list(ids_procesados)
        
        for i in range(0, len(datos_filtrados), self.batch_size):
            lote = datos_filtrados.iloc[i:i+self.batch_size]
            for _, fila in lote.iterrows():
                id_rel = str(fila[id_col])
                entidad = fila["Entidad"]
                relacion = fila["Relación"]
                elemento = fila[elem_col]
                print(f"📊 Verificando: {id_rel} - {entidad} -> {elemento}")
                
                resultado = self.verificar_relacion(id_rel, entidad, relacion, elemento)
                if resultado["validez"] in ["inválido", "error"]:
                    resultados.append(resultado)
                
                nuevos_ids_procesados.append(id_rel)
                
                if len(nuevos_ids_procesados) % 10 == 0:
                    self.checkpoint_manager.update_checkpoint(id_rel, nuevos_ids_procesados)
        
        if datos_filtrados.shape[0] > 0:
            ultimo_id = str(datos_filtrados.iloc[-1][id_col])
            self.checkpoint_manager.update_checkpoint(ultimo_id, nuevos_ids_procesados)
        
        return resultados, len(datos_filtrados)
    
    def generar_reporte(self, relaciones_invalidas, total_procesado):
        """
        Genera un reporte con las relaciones inválidas encontradas
        """
        if not relaciones_invalidas:
            print("\n✅ No se encontraron relaciones inválidas")
            return
            
        print(f"\n❌ Se encontraron {len(relaciones_invalidas)} relaciones inválidas de {total_procesado} verificadas")
        print("\n==== REPORTE DE RELACIONES INVÁLIDAS ====")
        
        for resultado in relaciones_invalidas:
            print(f"\nID: {resultado['id']}")
            print(f"Relación: {resultado['entidad1']} {resultado.get('relacion', '')} {resultado['entidad2']}")
            print(f"Problema: {resultado['justificacion']}")
            print("-" * 50)
        
        # Guardar reporte en archivo
        with open(f"reporte_relaciones_invalidas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w', encoding='utf-8') as f:
            json.dump(relaciones_invalidas, f, ensure_ascii=False, indent=2)


# Función para cargar datos desde diferentes formatos
def cargar_datos(ruta_o_datos):
    """Carga datos desde CSV, JSON o DataFrame"""
    if isinstance(ruta_o_datos, pd.DataFrame):
        return ruta_o_datos
    elif isinstance(ruta_o_datos, list):
        return pd.DataFrame(ruta_o_datos)
    elif isinstance(ruta_o_datos, str):
        if ruta_o_datos.endswith('.csv'):
            return pd.read_csv(ruta_o_datos)
        elif ruta_o_datos.endswith('.json'):
            with open(ruta_o_datos, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))
    raise ValueError("Formato de datos no soportado")


# Código de prueba
if __name__ == "__main__":
    # Inicializar el verificador con la configuración
    verificador = VerificadorRelaciones()
    
    # Datos de ejemplo (esto podría venir de un archivo)
    datos_ejemplo = [
        {
            "ID": "44303",
            "Entidad": "Deposiciones/heces grasosas o aceitosas",
            "Relación": "Symptom1 implies Symptom2",
            "Elemento Relacionado": "Diarrea"
        },
        {
            "ID": "44304",
            "Entidad": "Fiebre alta",
            "Relación": "Symptom1 implies Symptom2",
            "Elemento Relacionado": "Aumento de apetito"  # Esta relación debería ser inválida
        }
    ]
    
    # Cargar datos (adaptar según cómo se reciben los datos)
    datos = cargar_datos(datos_ejemplo)
    
    # Procesar los datos
    relaciones_invalidas, total = verificador.procesar_datos(datos)
    
    # Generar reporte
    verificador.generar_reporte(relaciones_invalidas, total)
