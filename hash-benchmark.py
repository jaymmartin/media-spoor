import os
import sqlite3
import hashlib
from time import time


def hashFiles(hasher):
    allowed_types = ['jpg', '.jpeg', '.cr2', '.mp4']
    reStr = '|'.join(allowed_types)
    rootDir = 'images'
    hashes = []
    h = {}
    files = 0
    for dirName, subdirList, fileList in os.walk(rootDir):
        for fname in fileList:
            path = dirName+'/'+fname
            if not (path.endswith('.jpg') or path.endswith('.jpeg') or path.endswith('.cr2')):
                continue

            with open(path, 'rb') as myfile:
                data = myfile.read()
                hash = hasher(data).hexdigest()
                hashes.append(hash)
                if hash not in h:
                    h[hash] = []
                h[hash].append(path)
                files += 1

    return hashes, files


for hasher in [hashlib.md5, hashlib.sha1, hashlib.sha256, hashlib.sha512, hashlib.sha3_256, hashlib.blake2b, hashlib.blake2s]:
    h = hasher()
    start = time()
    files = 0
    for i in range(10):
        results = hashFiles(hasher)
        files += results[1]
    end = time()
    print(h.name, files/(end-start))
