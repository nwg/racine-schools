#!/usr/bin/env python3

from urllib import request
import os
import sys
from shutil import copyfileobj
import zipfile

DOWNLOAD_DIR=os.path.join(os.path.dirname(sys.argv[0]), 'data/downloaded')

PAIRS=(
    ('https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReportDownload?selectedYear=2019&hiringLea=&workingLea=005830&position=&assignmentArea=&licenseType=&firstNameContains=&lastNameContains=&criteria=Filter%20Criteria:%20Selected%20Year:%202018%20-%202019;%20Selected%20Hiring%20Agencies:%20none;%20Selected%20Working%20Agencies:%204620%20-%20Racine%20Unified%20School%20District;%20Selected%20Assignment%20Position:%20--%20All%20Positions%20--;%20Selected%20Assignment%20Area:%20--%20All%20Areas%20--;%20Selected%20First%20Name:%20none;%20Selected%20Last%20Name:%20none;%20Selected%20License%20Type:%20--%20All%20License%20Types%20--', 'all-staff-2018-2019.csv'),
    ('https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReportDownload?selectedYear=2018&hiringLea=&workingLea=005830&position=&assignmentArea=&licenseType=&firstNameContains=&lastNameContains=&criteria=Filter%20Criteria:%20Selected%20Year:%202017%20-%202018;%20Selected%20Hiring%20Agencies:%20none;%20Selected%20Working%20Agencies:%204620%20-%20Racine%20Unified%20School%20District;%20Selected%20Assignment%20Position:%20--%20All%20Positions%20--;%20Selected%20Assignment%20Area:%20--%20All%20Areas%20--;%20Selected%20First%20Name:%20none;%20Selected%20Last%20Name:%20none;%20Selected%20License%20Type:%20--%20All%20License%20Types%20--', 'all-staff-2017-2018.csv'),
    ('https://publicstaffreports.dpi.wi.gov/PubStaffReport/Public/PublicReport/AllStaffReportDownload?selectedYear=2017&hiringLea=&workingLea=005830&position=&assignmentArea=&licenseType=&firstNameContains=&lastNameContains=&criteria=Filter%20Criteria:%20Selected%20Year:%202016%20-%202017;%20Selected%20Hiring%20Agencies:%20none;%20Selected%20Working%20Agencies:%204620%20-%20Racine%20Unified%20School%20District;%20Selected%20Assignment%20Position:%20--%20All%20Positions%20--;%20Selected%20Assignment%20Area:%20--%20All%20Areas%20--;%20Selected%20First%20Name:%20none;%20Selected%20Last%20Name:%20none;%20Selected%20License%20Type:%20--%20All%20License%20Types%20--', 'all-staff-2016-2017.csv')
)

ZIPPED_PAIRS = (
    (
        'https://dpi.wi.gov/sites/default/files/imce/cst/exe/AllStaff2016rev11-01-16.zip', 
        'all-staff-2015-2016.xlsx.zip',
        {'AllStaff2016rev11-01-16.xlsx': 'all-staff-2015-2016.xlsx'}
    ),
    (
        'https://dpi.wi.gov/sites/default/files/imce/cst/exe/AllStaff2015rev10-31-2016.zip', 
        'all-staff-2014-2015.xlsx.zip',
        {'AllStaff2015rev10-31-2016.xlsx': 'all-staff-2014-2015.xlsx'}
    ),
    (
        'https://nces.ed.gov/surveys/pss/zip/pss1718_pu_csv.zip',
        'pss-2017-2018.zip',
        {'pss1718_pu.csv': 'pss-2017-2018.csv'}
    ),
    (
        'https://nces.ed.gov/surveys/pss/zip/pss1516_pu_csv.zip',
        'pss-2015-2016.zip',
        {'pss1516_pu.csv': 'pss-2015-2016.csv'}
    ),
)

def fix_bad_zip(zipFile):
    f = open(zipFile, 'r+b')
    data = f.read()
    pos = data.find(b'\x50\x4b\x05\x06') # End of central directory signature
    if (pos > 0):
        self._log("Trancating file at location " + str(pos + 22)+ ".")
        f.seek(pos + 22)   # size of 'ZIP end of central directory record'
        f.truncate()
        f.close()
    else:
        raise ValueError('bad file')

def getpath(filename):
    return os.path.join(DOWNLOAD_DIR, filename)

os.mkdir(DOWNLOAD_DIR)

for url, filename in PAIRS:
    path = getpath(filename)
    if os.path.exists(path):
        print(f'Skipping {path} -- already exists')
        continue

    data = request.urlopen(url)
    with open(path, 'wb') as f:
        copyfileobj(data, f)

for url, filename, objects in ZIPPED_PAIRS:
    path = getpath(filename)
    if all(os.path.exists(getpath(outpath)) for internal, outpath in objects.items()):
        print(f'Skipping {path} -- all paths already exist')
        continue

    if not os.path.exists(path):
        print(f'downloading {path}')

        data = request.urlopen(url)
        with open(path, 'wb') as f:
            copyfileobj(data, f)

    with zipfile.ZipFile(path, 'r') as zip:
        for filename in zip.namelist():
            if filename in objects:
                zip.extract(filename, DOWNLOAD_DIR)
                os.rename(getpath(filename), getpath(objects[filename]))

def allpaths():
    for _, filename in PAIRS:
        yield getpath(filename)
    for _, _, objects in ZIPPED_PAIRS:
        yield from objects.values()

for filename in allpaths():
    path = getpath(filename)
    root, ext = os.path.splitext(path)
    csv = root + '.csv'
    if ext == '.xlsx' and not os.path.exists(csv):
        print(f'Please use excel to convert file {path} to {csv}')

