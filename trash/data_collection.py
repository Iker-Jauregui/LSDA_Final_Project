import requests
import json
import time
import pandas as pd

# Configuración
BASE_URL = "https://data.police.uk/api"
OUTPUT_JSON = "stop_search_data.json"
OUTPUT_CSV = "stop_search_data.csv"
OUTPUT_PARQUET = "stop_search_data.parquet"

# Obtain datasets available by month and force
print("Obtaining available dates and forces...")
response = requests.get(f"{BASE_URL}/crimes-street-dates")
available_data = response.json() if response.status_code == 200 else []

print(f"Found data for {len(available_data)} months\n")
# Lista para almacenar todos los registros unificados
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
                    # Agregar metadatos de fecha y fuerza a cada registro
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
        
        # Pause to avoid overwhelming the API
        time.sleep(0.3)

print(f"\n{'='*60}")
print("Processing and saving data...")
print(f"{'='*60}")

# Save complete JSON
with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(all_records, f, indent=2, ensure_ascii=False)
print(f"✓ JSON saved: {OUTPUT_JSON}")

# Convert to DataFrame and normalize nested data
df = pd.json_normalize(all_records)

# Save CSV
df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
print(f"✓ CSV saved: {OUTPUT_CSV} ({len(df)} rows, {len(df.columns)} columns)")

# Save Parquet
# df.to_parquet(OUTPUT_PARQUET, index=False, engine='pyarrow')
# print(f"✓ Parquet saved: {OUTPUT_PARQUET}")

# Final summary
print(f"\n{'='*60}")
print("FINAL SUMMARY:")
print(f"{'='*60}")
print(f"Processed dates: {len(available_data)}")
print(f"Total requests made: {total_requests}")
print(f"Total records obtained: {total_records}")
print(f"\nGenerated files:")
print(f"  - JSON:    {OUTPUT_JSON}")
print(f"  - CSV:     {OUTPUT_CSV}")
print(f"  - Parquet: {OUTPUT_PARQUET}")
print(f"{'='*60}")