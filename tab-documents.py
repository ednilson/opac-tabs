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


CONFIG = configparser.ConfigParser()
CONFIG.read('config.ini')
TIMENOW = time.strftime('%Y%m%d_%H%M')


def connect_mongodb():
    try:
        # reads config
        mdb = CONFIG._sections['MONGO-OPAC']

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
    if item.scielo_pids and item.scielo_pids.get('other'):
        aka = set(item.scielo_pids.get('other'))
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
    for lang in item.pdfs:
        if lang['lang'] != '':
            languages.add(lang['lang'].strip().lower())
    for lang in item.htmls:
        if lang['lang'] != '':
            languages.add(lang['lang'].strip().lower())

    # languages [pt, es, en]
    doc_pt = 1 if 'pt' in languages else 0
    doc_es = 1 if 'es' in languages else 0
    doc_en = 1 if 'en' in languages else 0

    # other languages
    xlang = languages.copy()
    for l in ('pt', 'en', 'es'):
        try:
            xlang.remove(l)
        except Exception as e:
            pass
    doc_other_lang = 1 if xlang else 0

    # publication date
    pdate = ''
    pyear = ''
    if item.publication_date:
        pdate = item.publication_date
        pyear = item.publication_date[0:4]

    dois = []
    if item.doi_with_lang:
        dois = [item.doi]
        doilang = [d.doi for d in item.doi_with_lang]
        dois.extend(doilang)
        dois = list(set(dois))
        # print(dois)
    else:
        dois = [item.doi]
    
    # Data
    data_dict = dict(
        journal_acronym = item.journal.acronym,
        journal_id = item.journal.id,
        journal_title = item.journal.title,
        issue_label = item.issue.label,
        xml_kernel = item.xml,
        pdf_files = [f['filename'] for f in item.pdfs if f['type'] == 'pdf'],
        num_pdfs = len(item.pdfs),
        publication_date = pdate,
        publication_year = pyear,
        pid_v3 = item._id,
        pid_v2 = item.pid,
        aka = aka,
        type = item.type.strip().lower(),
        doi = item.doi,
        doi_lang = [d.doi for d in item.doi_with_lang],
        dois = dois,
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
    
    # Directory and file names output
    dirout = CONFIG._sections['DIRPATH']['diroutput']
    csvfilename = os.path.join(dirout, 'opac-tabs-documents-{now}.csv'.format(now=TIMENOW))
    zipfilename = os.path.join(dirout, 'opac-tabs-documents-{now}.zip'.format(now=TIMENOW))
    
    # Create output directory
    if not os.path.exists(dirout):
        os.mkdir(dirout)
    
    # Get and writes data in CSV file
    fieldnames = ['journal_acronym', 'journal_id','journal_title', 'issue_label', 'xml_kernel', 'pdf_files', 'num_pdfs',
                  'publication_date', 'publication_year',
                  'pid_v3', 'pid_v2', 'aka', 'type', 'doi', 'doi_lang', 'dois', 'languages',
                  'document_pt', 'document_es', 'document_en',
                  'document_other_languages',
                  ]

    with open(csvfilename, mode='w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # query = models.Article.objects.filter(is_public=True, _id='MPZdS9X4FRxC6cBhxjxpbZF')
        # query = models.Article.objects.filter(is_public=True, _id='ZWDBHmJpMvsfnH38JvrPwCJ')
        query = models.Article.objects.no_cache().filter(is_public=True).batch_size(10)
       
        total = '%s: %s' % ('total records: ', query.count())
        print(total)
        logging.info(total)

        for item in query:
            try:
                print(item.id, item.pid, item.journal)
                logging.info(item._id)
                writer.writerow(get_data(item))
            except Exception as e:
                logging.info(item._id)
                logging.exception(e)
        
    csvfile.close()

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
    ld = [fzip for fzip in os.listdir(dirout) if fzip.startswith('opac-tabs-documents-') and fzip.endswith('.zip')]
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
