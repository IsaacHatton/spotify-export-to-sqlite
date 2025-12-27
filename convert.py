import zipfile
import sqlite3
import json
import sys

conn = sqlite3.connect('music.db')
cursor = conn.cursor()

def read_zip_in_memory(zip_path):
    contents = []
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.filelist:
            if not file_info.is_dir():
                filename = file_info.filename
                content = zip_ref.read(filename)
                
                contents.append({
                    'filename': filename,
                    'content': content.decode('utf-8', errors='ignore')  # Decode to string
                })
    
    return contents

cursor.execute('DROP TABLE IF EXISTS streaming_history')
cursor.execute('DROP TABLE IF EXISTS search_queries')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS streaming_history (  
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        end_time TEXT,
        artist_name TEXT,
        track_name TEXT,
        ms_played INTEGER
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_queries (
        platform TEXT,
        search_time TEXT,
        search_query TEXT,
        search_interaction_uris TEXT
    )
''')

# From file path
files = read_zip_in_memory(sys.argv[1])
for file in files:
    if file['filename'].startswith('Spotify Account Data/StreamingHistory_music'):
        streaming_data = json.loads(file['content'])
        for entry in streaming_data:
            cursor.execute('''
                INSERT INTO streaming_history (end_time, artist_name, track_name, ms_played)
                VALUES (?, ?, ?, ?)
            ''', (entry['endTime'], entry['artistName'], entry['trackName'], entry['msPlayed']))
    elif file['filename'].startswith('Spotify Account Data/SearchQueries'):
        search_data = json.loads(file['content'])
        for entry in search_data:
            cursor.execute('''
                INSERT INTO search_queries (platform, search_time, search_query, search_interaction_uris)
                VALUES (?, ?, ?, ?)
            ''', (entry['platform'], entry['searchTime'], entry['searchQuery'], ','.join(entry['searchInteractionURIs'])))


conn.commit()
conn.close()