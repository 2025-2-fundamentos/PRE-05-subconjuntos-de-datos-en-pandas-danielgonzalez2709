#!/usr/bin/env python3
"""
Extracción eficiente de columnas específicas usando pandas.
Proyecto: PRE-05 subconjuntos de datos en pandas
"""

import pandas as pd
import os
from pathlib import Path


def extract_specific_columns():
    """
    Extrae columnas específicas del dataset de eventos de camiones
    y las guarda en un archivo CSV optimizado.
    
    Utiliza las mejores prácticas de pandas para eficiencia:
    - Lectura optimizada con tipos de datos específicos
    - Selección eficiente de columnas
    - Escritura optimizada
    """
    
    # Definir rutas usando pathlib para compatibilidad multiplataforma
    input_file = Path("files/input/truck_event_text_partition.csv")
    output_file = Path("files/output/specific-columns.csv")
    
    # Crear directorio de salida si no existe
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Columnas específicas a extraer (subconjunto relevante para análisis)
    columns_to_extract = [
        'driverId',
        'truckId', 
        'eventTime',
        'eventType',
        'longitude',
        'latitude',
        'driverName',
        'routeName',
        'eventDate'
    ]
    
    try:
        # Lectura optimizada: solo las columnas necesarias y tipos específicos
        dtype_mapping = {
            'driverId': 'int32',
            'truckId': 'int32',
            'eventTime': 'string',
            'eventType': 'category',  # Eficiente para datos categóricos
            'longitude': 'float32',   # Precisión suficiente, menos memoria
            'latitude': 'float32',
            'driverName': 'string',
            'routeName': 'string',
            'eventDate': 'string'
        }
        
        print("Leyendo archivo de entrada...")
        df = pd.read_csv(
            input_file,
            usecols=columns_to_extract,  # Solo leer columnas necesarias
            dtype=dtype_mapping,         # Tipos optimizados
            engine='c'                   # Motor C para mayor velocidad
        )
        
        print(f"Datos leídos: {len(df):,} filas, {len(df.columns)} columnas")
        print(f"Memoria utilizada: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Información básica del subconjunto
        print("\nColumnas extraídas:")
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")
        
        # Guardar con optimizaciones
        print("\nGuardando archivo de salida...")
        df.to_csv(
            output_file,
            index=False,           # No incluir índice
            float_format='%.6f'    # Formato optimizado para coordenadas
        )
        
        print(f"✅ Archivo guardado exitosamente: {output_file}")
        print(f"📊 Resumen: {len(df)} registros, {len(df.columns)} columnas específicas")
        
        # Mostrar muestra de los datos
        print("\n📋 Muestra de los datos extraídos:")
        print(df.head(3).to_string())
        
        return True
        
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {input_file}")
        return False
    except Exception as e:
        print(f"❌ Error procesando datos: {e}")
        return False


if __name__ == "__main__":
    success = extract_specific_columns()
    exit(0 if success else 1)
