# CopenMed-DataValidator
## Instalación

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/amt2283/CopenMed-DataValidator.git
Instalar dependencias: Se requiere Python 3.8+ y la librería pandas. Instale la dependencia ejecutando:

bash
Copiar
pip install pandas requests
Requisitos adicionales: Asegúrese de que el servicio Ollama se encuentre activo en http://localhost:11434.

Uso
El sistema puede ejecutarse tanto mediante argumentos en la línea de comandos como en modo interactivo.
Para ejecutar el proyecto:

bash
Copiar
python main.py --datos ruta/al/archivo.csv --batch 16 --max 50 --mostrar --nivel_logs INFO
Si no se proveen argumentos, se solicitarán las opciones de manera interactiva.

Módulos Principales
1. DataLoader (gestion_de_datos.py)
Función:
Carga datos desde archivos CSV, Excel y TXT. Permite el procesamiento en “chunks” para optimizar el uso de memoria y limpia datos basura.

Métodos clave:

load_csv_or_excel()

_process_txt_file()

2. Configuración (config.py)
Función:
Define los parámetros de configuración del sistema, tales como el modelo a utilizar, tamaño del lote (batch_size), ruta del checkpoint y límite de procesamiento.

Ejemplo de contenido:

python
Copiar
CONFIG = {  
    "modelo": "deepseek-r1:8b",  
    "batch_size": 32,  
    "ruta_checkpoint": "checkpoint.json",  
    "max_procesar": 5  
}
3. Gestión de Checkpoints
El proyecto incluye dos módulos para gestionar el estado del procesamiento:

CheckpointManager:

Función:
Maneja de forma básica la carga, actualización y guardado del checkpoint.

Métodos clave:
load_checkpoint(), update_checkpoint(), reset_checkpoint().

GestionCheckpoint:

Función:
Proporciona una gestión más completa del estado del procesamiento, con métodos para consultar el último ID, total de registros procesados, agregar IDs, entre otros.

Métodos clave:
cargar_checkpoint(), crear_checkpoint(), guardar_checkpoint(), actualizar_checkpoint(), obtener_ultimo_id_procesado(), obtener_total_procesados(), agregar_id_procesado(), es_procesado(), reiniciar().

4. VerificadorRelaciones (procesamiento_datoss.py)
Función:
Se comunica con el servicio Ollama para validar la validez médica de cada relación. Procesa los datos en lotes, actualiza el checkpoint y genera un reporte de las relaciones inválidas.

Métodos clave:

_verificar_modelo()

verificar_relacion()

procesar_datos()

generar_reporte()

5. Gestión de Logs (logs.py)
Función:
Registra eventos, errores y advertencias del sistema tanto en consola como en archivos diarios.

Métodos clave:
debug(), info(), warning(), error(), critical(), y set_nivel() para ajustar el nivel de logging.

6. Módulo Principal (main.py)
Función:
Es el punto de entrada del sistema. Integra todos los módulos anteriores para:

Leer la configuración y argumentos (o solicitar opciones de manera interactiva).

Cargar y consolidar datos desde archivos.

Ejecutar la verificación de relaciones, gestionar checkpoints y generar reportes.

Registrar el proceso mediante logs.

Ejemplos de Código
Reiniciar el Checkpoint (GestionCheckpoint)
python
Copiar
from checkpoint import GestionCheckpoint

manager = GestionCheckpoint()
manager.reiniciar()  # Reinicia el checkpoint al estado inicial.
Ejecutar el Verificador en Modo Interactivo
bash
Copiar
python main.py
Ejecución con Argumentos
bash
Copiar
python main.py --datos datos.csv --batch 16 --max 50 --mostrar --nivel_logs INFO
Errores Comunes
MemoryError:
Reducir el valor de batch_size en config.py.

Ollama no responde:
Verificar que el servicio esté activo en http://localhost:11434.

Encabezados incorrectos:
Utilizar el parámetro has_header=False en DataLoader si el archivo carece de ellos.
