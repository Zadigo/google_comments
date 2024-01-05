import csv
import json

from google_comments import MEDIA_PATH
from google_comments.utilities import encoders


def write_csv_file(filename, data):
    with open(MEDIA_PATH / f'{filename}.csv', mode='w', newline='\n', encoding='utf-8') as f:
        writer = csv.writer(f)
        for item in data:
            if not isinstance(item, list):
                writer.writerow([item])
                continue
            writer.writerow(item)


def write_json_file(path, data):
    with open(path, mode='w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, cls=encoders.DefaultJsonEncoder)
