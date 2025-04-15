from venv import logger
import pandas as pd
import argparse
import sys
import json
import os
import logging
from datetime import datetime

# Importar los módulos de la aplicación
from procesamiento_datoss import VerificadorRelaciones
from checkpoint import GestionCheckpoint
from logs import GestionLogs
from config import CONFIG
from gestion_de_datos import DataLoader

def cargar_datos(ruta_archivo):
    logger.info(f"Cargando datos desde {ruta_archivo}")
    try:
        file_extension = ruta_archivo.split('.')[-1].lower()
        if file_extension in ['xlsx', 'xls']:
            # Para Excel, usamos DataLoader con encabezado personalizado
            custom_headers = ['A']
            loader = DataLoader(ruta_archivo, has_header=False, custom_headers=custom_headers)
            # Ajustar sheet_name según corresponda
            return loader.load_csv_or_excel(sheet_name="Hoja 1", remove_garbage=True)
        elif file_extension == 'csv':
            return pd.read_csv(ruta_archivo)
        elif file_extension == 'json':
            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))
        elif file_extension == 'txt':
            # Utilizamos el DataLoader modificado para TXT
            loader = DataLoader(ruta_archivo, chunk_size=CONFIG.get("batch_size", 10000), has_header=False)
            return loader.load_csv_or_excel(remove_garbage=True)
        else:
            logger.error(f"Formato de archivo no soportado: {ruta_archivo}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error al cargar el archivo {ruta_archivo}: {e}")
        sys.exit(1)

def guardar_resultados(resultados, prefijo="reporte_"):
    """
    Guarda los resultados en archivos CSV y JSON

    Args:
        resultados: Lista de diccionarios con los resultados
        prefijo: Prefijo para el nombre de los archivos
    """
    if not resultados:
        logger.info("No hay resultados para guardar")
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_base = f"{prefijo}{timestamp}"
    
    # Guardar como JSON
    ruta_json = f"{nombre_base}.json"
    with open(ruta_json, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    logger.info(f"Resultados guardados en JSON: {ruta_json}")
    
    # Se puede agregar guardado en CSV si se requiere.

def obtener_config_interactivo():
    """
    Muestra un menú interactivo para ingresar opciones y las guarda en un archivo JSON
    para usarlas en próximas ejecuciones.

    Returns:
        dict: Opciones ingresadas.
    """
    config_filename = "opciones_config.json"
    config_defaults = {}
    if os.path.exists(config_filename):
        respuesta = input("Se encontraron opciones guardadas. ¿Desea cargarlas? (s/n): ").strip().lower()
        if respuesta == 's':
            with open(config_filename, 'r', encoding='utf-8') as f:
                config_defaults = json.load(f)
            print("Opciones cargadas.")
    
    ruta = input(f"Ingrese la ruta del archivo de datos [{config_defaults.get('datos', '')}]: ") or config_defaults.get("datos", "")
    max_rel = input(f"Ingrese el número máximo de relaciones a procesar (limita la cantidad de datos a analizar) [{config_defaults.get('max', '')}]: ") or config_defaults.get("max", "")
    batch = input(f"Ingrese el tamaño del lote para procesamiento (cuántos registros procesar a la vez, útil para archivos grandes) [{config_defaults.get('batch', '')}]: ") or config_defaults.get("batch", "")
    reset = input(f"¿Desea reiniciar el checkpoint? (s/n) (Si elige 's', comenzará el procesamiento desde el principio) [{config_defaults.get('reset', 'n')}]: ") or config_defaults.get("reset", "n")
    mostrar = input(f"¿Desea mostrar resultados en consola? (s/n) [{config_defaults.get('mostrar', 'n')}]: ") or config_defaults.get("mostrar", "n")
    nivel_logs = input(f"Ingrese el nivel de logs (NONE=sin mensajes, ERROR=solo errores, INFO=todos los mensajes) [{config_defaults.get('nivel_logs', 'INFO')}]: ") or config_defaults.get("nivel_logs", "INFO")
    
    # Construir diccionario de opciones
    interactive_args = {
       "datos": ruta,
       "max": int(max_rel) if max_rel and max_rel.isdigit() else None,
       "batch": int(batch) if batch and batch.isdigit() else None,
       "reset": True if reset.lower() == 's' else False,
       "mostrar": True if mostrar.lower() == 's' else False,
       "nivel_logs": nivel_logs.upper()
    }
    # Guardar las opciones para la próxima ejecución
    with open(config_filename, 'w', encoding='utf-8') as f:
        json.dump(interactive_args, f, indent=2)
    return interactive_args

def main():
    """Función principal que ejecuta el verificador de relaciones médicas"""
    # Inicializar el gestor de logs
    gestor_logs = GestionLogs()
    logger = gestor_logs.logger
    
    # Verificar si se pasó algún argumento en la línea de comandos
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description="Verificador de relaciones médicas")
        parser.add_argument("--datos", required=True, help="Ruta al archivo de datos (CSV, JSON, Excel o TXT)")
        parser.add_argument("--max", type=int, help="Máximo número de relaciones a procesar")
        parser.add_argument("--batch", type=int, help="Tamaño del lote para procesamiento")
        parser.add_argument("--mostrar", action="store_true", help="Mostrar resultados por consola")
        parser.add_argument("--reset", action="store_true", help="Reiniciar el checkpoint")
        parser.add_argument("--nivel_logs", choices=["NONE", "ERROR", "INFO"], help="Nivel de logs a mostrar")
        args = parser.parse_args()
        opciones = {
            "datos": args.datos,
            "max": args.max,
            "batch": args.batch,
            "mostrar": args.mostrar,
            "reset": args.reset,
            "nivel_logs": args.nivel_logs if args.nivel_logs else "INFO"
        }
    else:
        # Modo interactivo: se solicita al usuario ingresar las opciones por terminal
        print("Modo interactivo de configuración:")
        opciones = obtener_config_interactivo()

    # Configurar el nivel de logs según la opción
    nivel = opciones.get("nivel_logs", "INFO")
    # Usar el nuevo método set_nivel
    gestor_logs.set_nivel(nivel)
    
    # Actualizar configuración principal con los argumentos seleccionados
    config = CONFIG.copy()
    if opciones.get("max"):
        config["max_procesar"] = opciones["max"]
    if opciones.get("batch"):
        config["batch_size"] = opciones["batch"]
    
    # Inicializar gestor de checkpoint
    checkpoint_manager = GestionCheckpoint()
    
    # Reiniciar checkpoint si se solicita
    if opciones.get("reset"):
        if nivel != "NONE":  # Solo mostrar log si no está en modo silencioso
            logger.info("Reiniciando checkpoint")
        checkpoint_manager.reiniciar()
    
    # Cargar datos usando la función que soporta CSV, JSON, Excel y TXT
    datos = cargar_datos(opciones["datos"])
    
    # En caso de que DataLoader para TXT retorne un generador, se consolidan los chunks
    if not isinstance(datos, pd.DataFrame):
        lista_df = []
        contador = 0
        for chunk in datos:
            lista_df.append(chunk)
            contador += len(chunk)
            if opciones.get("max") and contador >= opciones["max"]:
                break
        datos = pd.concat(lista_df, ignore_index=True)
    
    if nivel != "NONE":
        logger.info(f"Datos cargados: {len(datos)} relaciones")
    
    # Inicializar y ejecutar el verificador
    try:
        verificador = VerificadorRelaciones(config)
        
        # Mostrar información del checkpoint
        ultimo_id = checkpoint_manager.obtener_ultimo_id_procesado()
        total_prev = checkpoint_manager.obtener_total_procesados()
        if ultimo_id and nivel != "NONE":
            logger.info(f"Continuando desde ID: {ultimo_id} (Total procesados: {total_prev})")
        
        # Procesar datos
        if nivel != "NONE":
            logger.info("Iniciando procesamiento...")
        relaciones_invalidas, total = verificador.procesar_datos(datos)
        
        # Mostrar y guardar resultados
        if nivel != "NONE":
            if relaciones_invalidas:
                logger.info(f"Se encontraron {len(relaciones_invalidas)} relaciones inválidas de {total} verificadas")
            else:
                logger.info(f"No se encontraron relaciones inválidas en las {total} relaciones verificadas")
        
        if relaciones_invalidas:
            guardar_resultados(relaciones_invalidas, prefijo="relaciones_invalidas_")
        
        if opciones.get("mostrar"):
            print("\nVista previa de relaciones inválidas:")
            if relaciones_invalidas:
                for registro in relaciones_invalidas[:5]:
                    print(registro)
            else:
                print("No se encontraron relaciones inválidas.")
                
        if nivel != "NONE":
            logger.info("Procesamiento completado con éxito")
        
    except Exception as e:
        # Los errores críticos siempre deben mostrarse, incluso con NONE
        logger.critical(f"Error durante la ejecución: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
