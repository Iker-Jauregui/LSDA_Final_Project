import requests
import json
import time
import pandas as pd
import argparse
import os  # Nuevo import necesario para gestionar directorios
from datetime import datetime

BASE_URL = "https://data.police.uk/api"

def parse_date(date_string):
    """Convierte string YYYY-MM a objeto datetime"""
    try:
        return datetime.strptime(date_string, "%Y-%m")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Formato de fecha inválido: {date_string}. Use YYYY-MM")

def fetch_available_dates(start_date=None, end_date=None):
    """Obtiene las fechas y fuerzas disponibles desde la API, filtradas por rango"""
    print("Obtaining available dates and forces...")
    response = requests.get(f"{BASE_URL}/crimes-street-dates")
    available_data = response.json() if response.status_code == 200 else []

    # Filtrar por rango de fechas si se especifica
    if start_date or end_date:
        filtered_data = []
        for month_data in available_data:
            month_date = datetime.strptime(month_data['date'], "%Y-%m")

            # Verificar si está dentro del rango
            if start_date and month_date < start_date:
                continue
            if end_date and month_date > end_date:
                continue

            filtered_data.append(month_data)

        available_data = filtered_data

        if start_date and end_date:
            print(f"Filtering data between {start_date.strftime('%Y-%m')} and {end_date.strftime('%Y-%m')}")
        elif start_date:
            print(f"Filtering data from {start_date.strftime('%Y-%m')} onwards")
        elif end_date:
            print(f"Filtering data until {end_date.strftime('%Y-%m')}")

    print(f"Found data for {len(available_data)} months\n")
    return available_data

def fetch_stops_data(available_data):
    """Descarga todos los registros de stop-and-search"""
    all_records = []
    total_records = 0
    total_requests = 0

    for month_data in available_data:
        date = month_data['date']
        forces = month_data.get('stop-and-search', [])

        if not forces:
            print(f"Date {date}: No stop-and-search data available")
            continue

        print(f"\n{'='*60}")
        print(f"Processing date: {date}")
        print(f"Forces with data: {len(forces)}")
        print(f"{'='*60}")

        for force_id in forces:
            print(f"  {force_id}...", end=" ")

            try:
                response = requests.get(
                    f"{BASE_URL}/stops-force",
                    params={'force': force_id, 'date': date}
                )
                total_requests += 1

                if response.status_code == 200:
                    stops = response.json()
                    if stops:
                        for record in stops:
                            record['date'] = date
                            record['force_id'] = force_id
                            all_records.append(record)

                        total_records += len(stops)
                        print(f"✓ {len(stops)} records")
                    else:
                        print("○ No records")
                else:
                    print(f"✗ Error {response.status_code}")

            except Exception as e:
                print(f"✗ Exception: {e}")

            time.sleep(0.3)

    return all_records, total_records, total_requests

def save_data(all_records, output_base, save_json, save_csv, save_parquet):
    """Guarda los datos en los formatos solicitados"""
    print(f"\n{'='*60}")
    print("Processing and saving data...")
    print(f"{'='*60}")

    saved_files = []

    if save_json:
        output_file = f"{output_base}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, indent=2, ensure_ascii=False)
        print(f"✓ JSON saved: {output_file}")
        saved_files.append(output_file)

    if save_csv or save_parquet:
        df = pd.json_normalize(all_records)

        if save_csv:
            output_file = f"{output_base}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✓ CSV saved: {output_file} ({len(df)} rows, {len(df.columns)} columns)")
            saved_files.append(output_file)

        if save_parquet:
            output_file = f"{output_base}.parquet"
            df.to_parquet(output_file, index=False, engine='pyarrow')
            print(f"✓ Parquet saved: {output_file}")
            saved_files.append(output_file)

    return saved_files

def print_summary(available_data, total_requests, total_records, saved_files):
    """Imprime el resumen final"""
    print(f"\n{'='*60}")
    print("FINAL SUMMARY:")
    print(f"{'='*60}")
    print(f"Processed dates: {len(available_data)}")
    print(f"Total requests made: {total_requests}")
    print(f"Total records obtained: {total_records}")
    print(f"\nGenerated files:")
    for file in saved_files:
        print(f"  - {file}")
    print(f"{'='*60}")

def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(
        description='Descarga datos de stop-and-search de la API de police.uk',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Descargar todos los datos disponibles en formato Parquet
  python script.py

  # Descargar solo datos de 2024 en CSV
  python script.py --format csv --start 2024-01 --end 2024-12
        """
    )
    parser.add_argument(
        '--format',
        choices=['json', 'csv', 'parquet', 'all'],
        default='parquet',
        help='Formato de salida: json, csv, parquet o all (por defecto: parquet)'
    )
    # ELIMINADO: argumento --output ya que el nombre es automático ahora
    parser.add_argument(
        '--start',
        type=str,
        metavar='YYYY-MM',
        help='Fecha de inicio (formato: YYYY-MM, ej: 2023-01)'
    )
    parser.add_argument(
        '--end',
        type=str,
        metavar='YYYY-MM',
        help='Fecha de fin (formato: YYYY-MM, ej: 2024-12)'
    )

    args = parser.parse_args()

    # Validar y parsear fechas
    start_date = parse_date(args.start) if args.start else None
    end_date = parse_date(args.end) if args.end else None

    # Validar que start no sea posterior a end
    if start_date and end_date and start_date > end_date:
        parser.error("La fecha de inicio no puede ser posterior a la fecha de fin")

    # Determinar qué formatos guardar
    save_json = args.format in ['json', 'all']
    save_csv = args.format in ['csv', 'all']
    save_parquet = args.format in ['parquet', 'all']

    # Proceso principal
    available_data = fetch_available_dates(start_date, end_date)

    if not available_data:
        print("No hay datos disponibles para el rango de fechas especificado")
        return

    all_records, total_records, total_requests = fetch_stops_data(available_data)

    if not all_records:
        print("No se obtuvieron registros")
        return

    # --- NUEVA LÓGICA DE NOMBRE Y RUTA ---
    
    # 1. Definir el directorio de salida
    output_dir = os.path.join("data", "stop_search_data")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"\nCreated directory: {output_dir}")

    # 2. Extraer fechas reales de los datos obtenidos para el nombre del archivo
    # Ordenamos las fechas disponibles para asegurar cuál es la inicial y la final
    dates_found = sorted([d['date'] for d in available_data])
    file_start_date = dates_found[0]
    file_final_date = dates_found[-1]

    # 3. Construir el nombre base: ssd_{start_date}-{final_date}
    filename = f"ssd_{file_start_date}-{file_final_date}"
    output_path = os.path.join(output_dir, filename)

    # -------------------------------------

    saved_files = save_data(all_records, output_path, save_json, save_csv, save_parquet)
    print_summary(available_data, total_requests, total_records, saved_files)

if __name__ == "__main__":
    main()