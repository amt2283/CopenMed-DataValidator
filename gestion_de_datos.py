import pandas as pd
import json
import os
import re
from typing import List, Union, Optional, Iterable
import logging

# Configuración del logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, 
                 file_path: str, 
                 chunk_size: int = 10000, 
                 has_header: bool = True, 
                 custom_headers: Optional[List[str]] = None):
        """
        Inicializa el DataLoader con la ruta del archivo, tamaño de chunk,
        y la opción de definir encabezados personalizados.
        
        Args:
            file_path (str): Ruta completa del archivo a cargar.
            chunk_size (int): Tamaño de los chunks para lectura de archivos grandes.
            has_header (bool): Indica si el archivo contiene encabezados.
            custom_headers (list, opcional): Lista de encabezados a asignar si no existen.
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.has_header = has_header
        self.custom_headers = custom_headers
        
        if not os.path.exists(file_path):
            logger.error(f"El archivo {file_path} no existe.")
            raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
        
        # Cargar la estructura solo si el archivo es JSON
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.json':
            self.data_structure = self.load_data_structure()
        else:
            self.data_structure = {}
            
        logger.info(f"Inicializando DataLoader para el archivo: {os.path.basename(file_path)}")
        logger.info(f"Ruta completa: {file_path}")

    def load_data_structure(self) -> dict:
        """
        Carga la estructura de datos desde un archivo JSON.
        
        Returns:
            dict: Estructura de datos cargada.
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error al cargar la estructura de datos: {e}")
            return {}

    def load_csv_or_excel(self, 
                          columns_to_keep: Optional[List[str]] = None, 
                          remove_garbage: bool = False,
                          sheet_name: Optional[str] = None
                          ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Carga datos de archivos CSV, Excel o TXT con opciones de validación.
        
        Args:
            columns_to_keep (list, opcional): Lista de columnas a mantener.
            remove_garbage (bool): Activar limpieza de datos basura.
            sheet_name (str, opcional): Nombre de la hoja en archivos Excel.
        
        Returns:
            DataFrame o generador de chunks procesados.
        """
        file_extension = os.path.splitext(self.file_path)[1].lower()
        
        try:
            if file_extension == '.csv':
                reader = pd.read_csv
                if not self.has_header:
                    full_df = reader(self.file_path, header=None)
                    if self.custom_headers is not None:
                        full_df.columns = self.custom_headers
                else:
                    full_df = reader(self.file_path)
            
            elif file_extension in ('.xlsx', '.xls'):
                reader = pd.read_excel
                if not self.has_header:
                    full_df = reader(self.file_path, sheet_name=sheet_name, header=None)
                    if self.custom_headers is not None:
                        full_df.columns = self.custom_headers
                else:
                    full_df = reader(self.file_path, sheet_name=sheet_name)
            
            elif file_extension == '.txt':
                full_df_or_generator = self._process_txt_file(remove_garbage)
                # Si se obtiene un DataFrame (archivo pequeño) podemos asignar encabezados personalizados
                if isinstance(full_df_or_generator, pd.DataFrame):
                    # Si se requieren encabezados personalizados, se validan las columnas extraídas
                    if not self.has_header and self.custom_headers is not None:
                        if len(self.custom_headers) == full_df_or_generator.shape[1]:
                            full_df_or_generator.columns = self.custom_headers
                        else:
                            logger.warning(f"Encabezados personalizados ({len(self.custom_headers)}) no coinciden con "
                                           f"el número de columnas extraídas ({full_df_or_generator.shape[1]})")
                    processed_df = self._process_dataframe(full_df_or_generator, columns_to_keep, remove_garbage)
                    logger.info(f"Filas después del procesamiento: {processed_df.shape[0]}")
                    logger.info(f"Columnas después del procesamiento: {processed_df.shape[1]}")
                    return processed_df
                else:
                    # Se devuelve el generador de chunks para archivos TXT grandes
                    return full_df_or_generator
            else:
                raise ValueError(f"Formato de archivo no soportado: {file_extension}")
            
            # Para CSV y Excel, si el DataFrame es pequeño se procesa completo
            if self.chunk_size > len(full_df):
                processed_df = self._process_dataframe(full_df, columns_to_keep, remove_garbage)
                logger.info(f"Filas después del procesamiento: {processed_df.shape[0]}")
                logger.info(f"Columnas después del procesamiento: {processed_df.shape[1]}")
                return processed_df
            
            # Para archivos CSV se habilita la lectura por chunks
            if file_extension == '.csv':
                logger.info("Modo de lectura por chunks activado")
                chunk_iter = reader(self.file_path, chunksize=self.chunk_size)
                return self._process_chunks(chunk_iter, columns_to_keep, remove_garbage)
            else:
                processed_df = self._process_dataframe(full_df, columns_to_keep, remove_garbage)
                logger.info(f"Filas después del procesamiento: {processed_df.shape[0]}")
                logger.info(f"Columnas después del procesamiento: {processed_df.shape[1]}")
                return processed_df
            
        except Exception as e:
            logger.error(f"Error al cargar el archivo: {e}")
            return pd.DataFrame()

    def _process_txt_file(self, remove_garbage: bool = False) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Procesa el archivo TXT línea por línea utilizando _parse_data_entry y retorna
        o bien un DataFrame completo o un generador de DataFrames en función del tamaño.
        
        Args:
            remove_garbage (bool): Si se deben reportar líneas mal formateadas.
        
        Returns:
            DataFrame o generador de DataFrames.
        """
        data = []
        problematic_lines = []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                for i, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue  # Ignorar líneas vacías
                    
                    parsed = self._parse_data_entry(line)
                    # Verificamos si al menos los dos primeros campos (identificador y primer código) tienen valor
                    if parsed and parsed[0] is not None and parsed[1] is not None:
                        data.append(parsed)
                    else:
                        problematic_lines.append((i, line))
                        if remove_garbage:
                            logger.warning(f"Línea mal formateada ({i}): {line}")
            
            if not data:
                logger.error("No se pudo procesar ninguna línea del archivo TXT")
                return pd.DataFrame()
            
            if problematic_lines:
                logger.warning(f"Total de líneas problemáticas: {len(problematic_lines)} de un total aproximado de {i} líneas")
            
            # Determinar el número de columnas según el primer registro
            num_cols = len(data[0])
            if num_cols == 7:
                columns = ['Linea', 'Codigo1', 'Entidad', 'Relación', 'Codigo2', 'ElementoRelacionado', 'Score']
            else:
                # Formato original (4 columnas)
                columns = ['ID', 'Entidad', 'Relación', 'Elemento Relacionado']
            
            # Si se excede el chunk_size, se retorna un generador de DataFrames
            if len(data) > self.chunk_size:
                def generator():
                    for j in range(0, len(data), self.chunk_size):
                        chunk = data[j:j+self.chunk_size]
                        df_chunk = pd.DataFrame(chunk, columns=columns)
                        yield self._process_dataframe(df_chunk, None, remove_garbage)
                return generator()
            else:
                df = pd.DataFrame(data, columns=columns)
                return self._process_dataframe(df, None, remove_garbage)
            
        except Exception as e:
            logger.error(f"Error al procesar archivo TXT: {e}")
            return pd.DataFrame()

    def _process_chunks(self, 
                        reader: Union[pd.io.parsers.TextFileReader, pd.ExcelFile],
                        columns_to_keep: Optional[List[str]] = None, 
                        remove_garbage: bool = False
                        ) -> Iterable[pd.DataFrame]:
        """
        Procesa chunks de datos aplicando validaciones.
        
        Args:
            reader: Iterador de chunks de datos.
            columns_to_keep: Columnas a mantener.
            remove_garbage: Activar limpieza de datos basura.
        
        Yields:
            DataFrames procesados.
        """
        for chunk in reader:
            yield self._process_dataframe(chunk, columns_to_keep, remove_garbage)

    def _process_dataframe(self, 
                           df: pd.DataFrame, 
                           columns_to_keep: Optional[List[str]] = None, 
                           remove_garbage: bool = False
                           ) -> pd.DataFrame:
        """
        Procesa un DataFrame aplicando validaciones y parseando la columna "A" si existe.
        
        Args:
            df: DataFrame a procesar.
            columns_to_keep: Columnas a mantener.
            remove_garbage: Activar limpieza de datos basura.
        
        Returns:
            DataFrame procesado.
        """
        if columns_to_keep:
            df = df[columns_to_keep]
        if remove_garbage:
            df = self._remove_garbage_data(df)
        if 'A' in df.columns:
            logger.info("Parseando estructura de datos de la columna 'A'...")
            try:
                parsed_columns = df['A'].apply(lambda x: pd.Series(self._parse_data_entry(x)))
                # Se asumen 4 columnas para el parseo de la columna "A"
                parsed_columns.columns = ['ID', 'Entidad', 'Relación', 'Elemento Relacionado']
                df = pd.concat([df, parsed_columns], axis=1)
                df.drop('A', axis=1, inplace=True, errors='ignore')
            except Exception as e:
                logger.warning(f"Error al parsear la columna 'A': {e}")
        return df

    def _remove_garbage_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpieza básica de datos basura.
        Implementación: eliminar filas con valores nulos.
        """
        return df.dropna()

    def _parse_data_entry(self, entry: str) -> tuple:
        """
        Parsea cada entrada en un archivo TXT o en la columna "A".
        
        Maneja tres formatos:
        1. Formato extendido con paréntesis anidados:
           "235571: (16172 (ICD-11 : BlockL1-7A2( Hypersomnolence disorders)), The prevalence of..., 641 (Población general)) 0.000500,"
        
        2. Formato extendido estándar:
           "167: (11 (Alergia alimentaria), Disease is in the domain of Specialty, 98 (Alergología)) 1.000000,"
           
        3. Formato original (4 campos):
           "44310: (Tumores neuroendocrinos gastrointestinales (TNEG) y de páncreas,Group can be observed in Anatomy,Páncreas)"
        
        Args:
            entry (str): Entrada a parsear.
        
        Returns:
            tuple: 7 elementos para los formatos extendidos o 4 para el formato original.
        """
        try:
            # 1. Intentar con formato extendido con paréntesis anidados
            initial_match = re.match(r"^(\d+):\s*\((.*)\)\s*([\d.]+)(?:,)?$", entry)
            if initial_match:
                line_num = initial_match.group(1)
                inner_content = initial_match.group(2)
                score = initial_match.group(3)
                
                # Extraer primer código y su texto (se permite anidamiento en el contenido)
                code1_match = re.match(r"^\s*(\d+)\s*\(([^)]+(?:\([^)]*\)[^)]*)*)\)\s*,", inner_content)
                if code1_match:
                    code1 = code1_match.group(1)
                    entity = code1_match.group(2).strip()
                    
                    # Posición después del primer bloque para capturar el resto
                    pos_after_first = code1_match.end()
                    remaining = inner_content[pos_after_first:].strip()
                    
                    # Extraer la relación y el segundo código con su elemento asociado
                    relation_match = re.match(r"^([^,]+)\s*,\s*(\d+)\s*\(([^)]+(?:\([^)]*\)[^)]*)*)\)\s*$", remaining)
                    if relation_match:
                        relation = relation_match.group(1).strip()
                        code2 = relation_match.group(2)
                        related_element = relation_match.group(3).strip()
                        return (line_num, code1, entity, relation, code2, related_element, score)
            
            # 2. Intentar con el formato extendido estándar
            pattern_ext = r"^(\d+):\s*\(\s*(\d+)\s*\(([^)]+)\)\s*,\s*([^,]+)\s*,\s*(\d+)\s*\(([^)]+)\)\s*\)\s*([\d.]+)(?:,)?$"
            match_ext = re.match(pattern_ext, entry)
            if match_ext:
                return (match_ext.group(1),   # Línea
                        match_ext.group(2),   # Código1
                        match_ext.group(3),   # Entidad
                        match_ext.group(4),   # Relación
                        match_ext.group(5),   # Código2
                        match_ext.group(6),   # Elemento Relacionado
                        match_ext.group(7))   # Score
            
            # 3. Intentar con el formato original (4 campos)
            pattern_orig = r"^(\d+):\s*\((.*)\)$"
            match_orig = re.match(pattern_orig, entry, flags=re.DOTALL)
            if match_orig:
                id_num = match_orig.group(1)
                content = [elem.strip() for elem in match_orig.group(2).split(',', maxsplit=2)]
                content += [None] * (3 - len(content))
                return (id_num, content[0], content[1], content[2])
            
            # Si ningún patrón coincide, se registra la advertencia
            logger.warning(f"No se pudo parsear la línea: {entry}")
            return tuple([None] * 7)
            
        except Exception as e:
            logger.warning(f"Error parseando entrada: {entry} | {str(e)}")
            return tuple([None] * 7)

# Ejemplo de uso actualizado
if __name__ == "__main__":
    try:
        # Ejemplo con archivo TXT
        txt_path = r"C:\Users\AMT22\Downloads\allrelations.txt"
        loader = DataLoader(txt_path, chunk_size=10, has_header=False)
        txt_data = loader.load_csv_or_excel(remove_garbage=True)
        
        if hasattr(txt_data, '__iter__') and not isinstance(txt_data, pd.DataFrame):
            print("\nModo de chunks para archivo TXT:")
            for chunk in txt_data:
                print(chunk.head())
                break  # Se muestra solo el primer chunk para la demo
        else:
            print("\nVista previa de datos TXT:")
            print(txt_data.head())
    
    except Exception as e:
        logger.error(f"Error: {e}")
