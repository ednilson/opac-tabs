import configparser
import csv
import logging
import os
import time
import zipfile

from mongoengine import connect
from pymongo import ReadPreference
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from opac_schema.v1 import models


NOW = time.strftime('%Y%m%d_%H%M')

config = configparser.ConfigParser()
config.read('config.ini')
dirout = config._sections['DIRPATH']['diroutput']


def connect_mongodb():
    try:
        # reads config
        mdb = config._sections['MONGO-OPAC']

        # reads reference
        rp = ReadPreference.SECONDARY if mdb['readpreference'] == 'secondary' else ReadPreference.PRIMARY

        connect(db=mdb['dbname'],
                username=mdb['username'],
                password=mdb['password'], 
                host='mongodb://{hostnames}'.format(hostnames=mdb['hostnames']),
                port=int(mdb['port']),
                replicaSet=mdb['replicaset'],
                read_preference=rp,
                )
    except (ServerSelectionTimeoutError, ConnectionFailure) as et:
        logging.info('timeout, connect failure')
        logging.exception(et)


def get_data(item):
    
    # aka
    aka = ''
    if item.scielo_pids and item.scielo_pids.get("other"):
        aka = set(item.scielo_pids.get("other"))
        try:
            aka.remove(item._id)
        except KeyError:
            pass
        try:
            aka.remove(item.pid)
        except KeyError:
            pass
        aka = ';'.join(aka)
    
    # languages
    languages = set()
    for item_ in item.pdfs:
        languages.add(item_["lang"].strip().lower())
    for item_ in item.htmls:
        languages.add(item_["lang"].strip().lower())

    # languages [pt, es, en]
    doc_pt = 1 if 'pt' in languages else 0
    doc_es = 1 if 'es' in languages else 0
    doc_en = 1 if 'en' in languages else 0

    # other languages
    xlang = set(languages)

    for l in ("pt", "en", "es"):
        try:
            xlang.remove(l)
        except Exception as e:
            pass
    doc_other_lang = 1 if xlang else 0
    
    # Data
    data_dict = dict(
        pid_v3 = item._id,
        pid_v2 = item.pid,
        aka = aka,
        type = item.type.strip().lower(),
        doi = item.doi,
        languages = ';'.join(languages),
        document_pt = doc_pt,
        document_es = doc_es,
        document_en = doc_en,
        document_other_languages = doc_other_lang,       
        )
    
    return data_dict


def main():
    # MongoDB Connection
    connect_mongodb()
    
    # File names output
    csvfilename = os.path.join(dirout, 'opac-tabs-{now}.csv'.format(now=NOW))
    zipfilename = os.path.join(dirout, 'opac-tabs-{now}.zip'.format(now=NOW))
    
    # Create output directory
    if not os.path.exists(dirout):
        os.mkdir(dirout)
    
    # Get and writes data in CSV file
    fieldnames = ['pid_v3', 'pid_v2', 'aka', 'type', 'doi', 'languages',
                  'document_pt', 'document_es', 'document_en',
                  'document_other_languages',
                  ]

    with open(csvfilename, mode="w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        query = models.Article.objects.filter(is_public=True)
        total = str(query.count())
        logging.info('total records: ', total)
        
        for item in query:
            try:
                writer.writerow(get_data(item))
            except Exception as e:
                logging.info(item._id)
                logging.exception(e)

    # ZipFile
    try:
        if os.path.isfile(csvfilename):
            zf = zipfile.ZipFile(zipfilename, mode='x')
            zf.write(csvfilename, compress_type=zipfile.ZIP_DEFLATED)
            zf.close()
            
            # remove CSV file
            os.remove(csvfilename)
    except Exception as e:
        logging.info(csvfilename, zipfilename)
        logging.exception(e)

    # Remove old zip files keeping the 3 most recent
    ld = [fzip for fzip in os.listdir(dirout) if fzip.startswith('opac-tabs-') and fzip.endswith('.zip')]
    ld.sort()
    if len(ld) > 3:
        try:
            for file_to_remove in ld[:-3]:
                os.remove(os.path.join(dirout, file_to_remove))
        except Exception as e:
            logging.info(str(ld))
            logging.exception(e)

if __name__ == '__main__':
    main()