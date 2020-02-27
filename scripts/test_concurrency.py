#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor

import urllib.request
import hashlib

def get_url():
    data = urllib.request.urlopen('http://0.0.0.0:5000/s/21st%20Century%20Preparatory%20School')
    hash = hashlib.sha256()
    hash.update(data.read())
    return hash.digest()

COUNT=10
with ThreadPoolExecutor(max_workers=COUNT) as executor:
    threads = [ executor.submit(get_url) for i in range(COUNT) ]
    for thread in threads:
        print(thread.result())
