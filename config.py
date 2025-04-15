CONFIG = {  
    "modelo": "deepseek-r1:8b",  # Modelo a seleccionar
    
    "batch_size": 32,  # Cantidad de datos a procesar por ejecucion python  cambiar batch_size 1 para evitar errores de memoria
    "ruta_checkpoint": "checkpoint.json",  
    "max_procesar": 5,  # Límite de textos por ejecución (opcional)
}  