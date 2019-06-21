import sys
import os
import re
import hashlib
import sqlite3

def walk_dir(root):
    allowed_types = ['jpg', '.jpeg', '.cr2', '.mp4']
    reg_expr = '.*('+'|'.join(allowed_types)+')$'

    for dir_name, sub_dir_list, file_list in os.walk(root):
        for file_name in file_list:
            path = dir_name+'/'+file_name
            match = re.search(reg_expr, path, re.IGNORECASE)
            if not match:
                continue

            yield os.path.abspath(path)

def create_tables(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY,
    sha1 TEXT NOT NULL UNIQUE
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS file (
        id INTEGER PRIMARY KEY,
        media_id INTEGER NOT NULL,
        path TEXT NOT NULL UNIQUE
        )""")

    cursor.execute("CREATE INDEX IF NOT EXISTS media_id ON file (media_id)")

def insert_files(cursor, root):
    counts = {'files': 0, 'media_inserts': 0, 'file_inserts': 0}
    for path in walk_dir(root):
        with open(path, 'rb') as myfile:
            data = myfile.read()
            sha1 = hashlib.sha1(data).hexdigest()
            cursor.execute("SELECT * FROM media WHERE sha1 = ?", [sha1])
            media = cursor.fetchone()
            if media is None:
                cursor.execute("INSERT INTO media(sha1) VALUES(?)", [sha1])
                counts['media_inserts'] += 1
                media_id = cursor.lastrowid
            else:
                media_id = media[0]

            cursor.execute("SELECT * FROM file WHERE path = ?", [path])
            file = cursor.fetchone()
            if file is None:
                cursor.execute("INSERT INTO file(media_id, path) VALUES(?,?)", [
                            media_id, path])
                counts['file_inserts'] += 1

            counts['files'] += 1

    return counts


if __name__ == "__main__":

    root = 'test/images'
    if len(sys.argv) > 1:
        root = sys.argv[1]
    
    if not os.path.exists(root):
        print("Path Not Found")
        exit(1)
    if not os.path.isdir(root):
        print("Path is not Directory")
        exit(1)

    conn = sqlite3.connect('media.db')

    # handle transactions manually
    conn.isolation_level = None

    cursor = conn.cursor()

    cursor.execute('BEGIN')
    create_tables(cursor)
    counts = insert_files(cursor, root)
    conn.execute('COMMIT')

    conn.close()

    print(counts)
