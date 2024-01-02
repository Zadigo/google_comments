import csv
from google_comments import MEDIA_PATH

def write_csv_file(filename, data):
    with open(MEDIA_PATH / f'{filename}.csv', mode='w', newline='\n', encoding='utf-8') as f:
        writer = csv.writer(f)
        for item in data:
            if not isinstance(item, list):
                writer.writerow([item])
                continue
            writer.writerow(item)

