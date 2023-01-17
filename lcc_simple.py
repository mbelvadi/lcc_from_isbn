#  Copyright 2023 Melissa Belvadi Open Source License CC-BY-NC-SA granted
# Input a file with a single column of ISBNs - ASSUMES NO COLUMN HEADER ROW
# and output a file with four tsv columns:
# the original ISBN from the input file,
# the Library of Congress Call Number if found from OCLC Classify, Harvard, Stanford, or Yale
# the cleaned-up ISBN
# the source of the LCC (OCLC, Harvard, Stanford, Yale)
# User provides:
# - the input filename
# If no LCC was found from any of the available sources, the data is written with the LCC column blank and source=NOTFOUND
#    as well as writing a line to the logfile.
# If any ISBN is missing or is invalid (can't be fixed with isbnlib cleanup attempts), the line is reported to the error log.
# The program checks for a local sqlite3 database file first called isbn_lc.db and creates it empty if it
#   doesn't find it, and if it does, will search it first so the program won't keep hitting the remote servers
#   every time for call numbers the user has previously found.
#  It will then save any that it gets from the remote servers into that file for checking the next time.
#  It looks for that sqlite file in the same working directory as the data file.

import os
import re
import logging
import csv
import isbnlib
import requests
from requests import get
from requests.utils import requote_uri
import xml.dom.pulldom
import xml.dom.minidom
import xml.sax.saxutils
import json
from isbnlib.registry import bibformatters
from bs4 import BeautifulSoup
import sruthi
import threading
import sys
import sqlite3


UA = 'isbnlib (gzip)'
myheaders = {'User-Agent': UA}

base = 'http://classify.oclc.org/classify2/Classify?'
summaryBase = '&summary=true'
summaryInd = 'false'

UA_harv = 'noreply@nobody.com'
SERVICE1 = 'harv'
jsondata = bibformatters['json']

sqlitefile = 'isbn_lc.db'
table_columns = {"ISBN": "text", "LC": "text", "LCSource": "text"}

LOC_Base = 'http://lx2.loc.gov:210/LCDB?'

def sqlite_search(dbfile,isbn):
    try:
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
    except:
        sqlite_create(dbfile)
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
        infologger.error(f'Unable to open sqlite file: {dbfile}')
    select_field_list_string = "DISTINCT ISBN, LC, LCSource"
    from_string = "main"
    where_string = f"ISBN like '{isbn}';"
    sql_string = "SELECT {} from {} WHERE {}".format(select_field_list_string, from_string, where_string)
    try:
        c.execute(sql_string)
    except:
        infologger.debug(f'{isbn} not found in {dbfile}')
        conn.close()
        return None
    else:
        entry = c.fetchall()
    conn.close()
    if entry is None or len(entry) == 0:
        return None
    else:
        return entry[0]

def sqlite_create(dbfile):
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    sql_tablecreate(c,'main',**table_columns)
    conn.commit()
    conn.close()
    return None

def sql_tablecreate(sqlc, table_name, **kwargs):
    """ Parameters: cursor, table name, **kwargs are dict of fieldnames and data types """
    create_string = "CREATE TABLE IF NOT EXISTS {} (".format(table_name)
    for fieldname, sql_type in kwargs.items():
        create_string = create_string + fieldname + " " + sql_type + ","
    final_string = create_string.rstrip(",") + ")"
    infologger.debug("Tablecreate finalstring: {}".format(final_string))
    try:
        sqlc.execute("{}".format(final_string))
        return True
    except sqlite3.Error as es:
        infologger.debug("Tablecreate Error: {}".format(es))
        return False
    except Exception as es2:
        infologger.debug("Tablecreate Exception: {}".format(es2))
        return False

def sql_tableinsert(sqlcconn,table_name, **kwargs):
    """Given a cursor and table name and dict list of column names and values, insert a single row with the provided data"""
    insert_string = "INSERT OR IGNORE INTO {} VALUES (".format(table_name)
    format_string = ''
    try:
        _ = len(kwargs.values())
    except Exception as etf:
        infologger.error("TbleInsert no fields in kwargs Exception: {}".format(etf))
        return False
    for fieldvalue in kwargs.values():
        if fieldvalue.find("'") != -1:
            fieldvalue = re.sub("'","''",fieldvalue)
        format_string = format_string + "'" + fieldvalue + "',"
    format_string = format_string.rstrip(",") + ')'
    final_string = insert_string + format_string
    try:
        sqlcconn.execute(final_string)
        sqlcconn.execute("SELECT * from {} limit 5".format(table_name))
        return True
    except sqlite3.Error as eti:
        infologger.error("TableInsert Error: {} {}".format(eti,final_string))
        return False
    except Exception as eti2:
        infologger.error("TbleInsert Exception: {}".format(eti2))
        return False


def alma_search(inst_code,isbn):
    a_lcc = None
    alma_extra = 'alma.isbn='
    records = sruthi.searchretrieve('http://eu00.alma.exlibrisgroup.com/view/sru/'+inst_code, query=alma_extra + isbn)
    record = records[0]
    infologger.debug(f'record: {list(record)}')
    for x in record['datafield']:
        infologger.debug(list(x))
        if x['tag'] == '050':
            infologger.debug(f"xsubfieldkeys: {x['subfield'][0]['text']}")
            try:
                a_lcc = (x['subfield'][0]['text'])
            except:
                continue
            break
    return a_lcc


def loc_search(isbn,sru_base):
    loc_lcc = None
    try:
        records = sruthi.searchretrieve(sru_base, query=isbn)
    except:
        return None
    if len(list(records)) < 1:
        return None
    record = records[0]
    try:
        _ = record['datafield']
    except:
        return None
    for x in record['datafield']:
        if x['tag'] == '050':
            try:
                loc_lcc = (x['subfield']['text'])
            except:
                return None
            else:
                break
    return loc_lcc


def get_blacklight(base_url1, institution, paramvalue=''):
    calltext = None
    URL_TITLE = f'{base_url1}/catalog?q={paramvalue}&search_field=all_fields&commit=Search'
    infologger.warning(f'{institution} URL:\t{URL_TITLE}')
    try:
        response = get(URL_TITLE)
    except:
        infologger.exception(f'{institution} Error:\t{paramvalue}')
    else:
        soup = BeautifulSoup(response.text, 'html.parser')
        tags = soup.find_all('a')
        for tag in tags:
            try:
                check_link = tag.attrs['href']
            except:
                return None
            if check_link.find('/catalog/') >= 0:
                detailURL = f'{base_url1}{check_link}/librarian_view'
                infologger.debug(f'{institution} detailURL:\t{detailURL}')
                try:
                    response_det = get(detailURL)
                except:
                    continue
                soup_det = BeautifulSoup(response_det.text, 'html.parser')
                try:
                    fields = soup_det.findAll('div', 'field')
                except:
                    return None
                """ Find 050 callnumbers """
                for marcfield in fields:
                    try:
                        tag_ind = marcfield.find('div', attrs={'class': 'tag_ind'})
                    except:
                        return None
                    if tag_ind is None:
                        continue
                    tag_ind_text = str(tag_ind.text)
                    tag_ind_text_050 = re.search(r'.*050.*', tag_ind_text)
                    if tag_ind_text_050:
                        infologger.debug(f'{institution} 050tag: {tag_ind_text_050.group(0)}')
                        subfields = marcfield.find('div', attrs={'class': 'subfields'})
                        if subfields is None:
                            continue
                        find_sub = re.sub(r'\n', '', subfields.text)
                        regex = r'([a-z])\|'
                        find_sub = re.sub(regex, r'|\g<1>', find_sub)
                        sub_list = find_sub.split('|')
                        calltext = None
                        for loop in sub_list:  # first time, just looking for first subfield a
                            if loop != '':
                                if loop.startswith('a'):
                                    infologger.debug(f'{institution} 050 a element:{loop[2:]}')
                                    calltext = loop[2:].strip()
                                    break  # want only one callnumber subfield a
    return calltext


def get_oclc_data(parmtype="isbn", parmvalue=""):
    lcc_value = None
    try:
        nexturl = base + parmtype+"=" + requote_uri(parmvalue)+"&summary=true"
        infologger.debug("OCLC URL: {} ".format(nexturl))
    except Exception as ue:
        infologger.error("OCLC URL encode failed: {}".format(ue))
        return None
    else:
        try:
            r = requests.get(nexturl, headers=myheaders)
            if not r.ok:
                infologger.error("OCLC Request returned http error: {}".format(r.status_code))
                return None
        except Exception as e:
            infologger.error("OCLC URL request failed: {}".format(e))
            return None
        else:
            wq = r.text
        xdoc = xml.dom.minidom.parseString(wq)
    response = xdoc.getElementsByTagName('response')[0]
    respCode = response.attributes["code"].value
    if respCode == '0' or respCode == '2':
        recommendations = xdoc.getElementsByTagName('recommendations')[0]
        if recommendations:
            if len(xdoc.getElementsByTagName('lcc')) > 0:
                local_lcc = recommendations.getElementsByTagName('lcc')[0]
                if local_lcc:
                    for mostPopular in local_lcc.getElementsByTagName('mostPopular'):
                        nsfa = mostPopular.attributes["nsfa"].value
                        lcc_value = nsfa
    elif respCode == '4':
        works = xdoc.getElementsByTagName('works')[0]
        infologger.debug('Works found: ' + str(len(works.getElementsByTagName('work'))))
        for work in works.getElementsByTagName('work'):
            try:
                m_wi = work.attributes["wi"].value
            except:
                continue
            else:
                try:
                    schemes = work.attributes["schemes"].value
                except:
                    continue
                if 'LCC' in schemes:
                    infologger.debug(f'going to try to get lcc using wi {m_wi}')
                    lcc_value = get_oclc_data('wi',m_wi)
                    break
    elif respCode != '102':
        infologger.error("OCLC reporting odd error {}, check by hand: {}".format(respCode,nexturl))
    if lcc_value:
        return lcc_value
    else:
        return None


def validate_json(data):
    if str(data) == "":
        infologger.error("validate_json: returns False because no data in passed string: {}".format(str(data)))
        return False
    return True


def json_query(isbn, service=SERVICE1, user_agent=UA_harv):
    SERVICE_URL = f'https://api.lib.harvard.edu/v2/items.json?identifier={isbn}'
    j_myheaders = {'User-Agent': user_agent}
    infologger.debug(f'HARVARD URL: {SERVICE_URL}')
    try:
        r = requests.get(SERVICE_URL, headers=j_myheaders)
        if not r.ok:
            infologger.error("{} Request returned http error: {}".format(service, r.status_code))
            return None
    except Exception as e:
        infologger.exception("URL request failed: {}".format(e))
        return None
    else:
        try:
            wq = r.json()
        except:
            infologger.exception("Failed to return json: {}".format(r.text[0:100]))
            return None
        else:
            if isinstance(wq, list):  # wiki puts the entire json inside list brackets
                return wq[0]
            elif isinstance(wq, dict):
                return wq
            else:
                return None


def get_metadata(isbn, tryservice=SERVICE1):
    try:
        metaget = isbnlib.meta(isbn, service=tryservice)
        g_record_data = json.loads(jsondata(metaget))
    except Exception as e:
        infologger.exception("Exception: {} for book {} at {}".format(e, isbn, tryservice))
        return None
    else:
        return g_record_data


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(e_obj, e_arr, e_key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(e_obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, e_arr, e_key)
                elif k == e_key:
                    e_arr.append(v)
        elif isinstance(e_obj, list):
            for item in e_obj:
                extract(item, e_arr, e_key)
        return e_arr

    values = extract(obj, arr, key)
    return values


def harvard_get(m_isbn):
    h_lcc = ''
    try:
        record_data = json_query(m_isbn, SERVICE1)
    except:
        infologger.error(f'Error getting record_data from Harvard {m_isbn}')
        return None
    try:
        _ = record_data['items']
    except:
        infologger.error(f'Error getting record_data from Harvard {m_isbn}')
        return None
    if record_data['items']:
        if "mods" in record_data['items']:
            record_data = record_data['items']['mods']
        else:
            infologger.warning('JSON is malformed, ,missing mods, skipping this isbn')
        for volfield in record_data:
            if volfield == "classification":
                if isinstance(record_data["classification"], list):
                    h_lcc_j = record_data['classification'][0]
                else:
                    h_lcc_j = record_data['classification']
                if h_lcc_j['@authority'] == 'lcc' and h_lcc == '':
                    harv_lcc = h_lcc_j['#text']
                    infologger.debug(f'Harvard found {m_isbn} {harv_lcc}')
                    return harv_lcc
    return None


def fix_isbn(isbn):
    lib_isbn = isbnlib.canonical(isbn)
    if len(lib_isbn) in (10, 13):
        if len(lib_isbn) == 10:
            isgood = isbnlib.is_isbn10(lib_isbn)
        else:
            isgood = isbnlib.is_isbn13(lib_isbn)
        if isgood:
            return lib_isbn
    if len(lib_isbn) < 10:
        return None
    lib_isbn = isbnlib.get_isbnlike(isbn)
    if len(lib_isbn) < 10:
        return None
    lib_isbn = isbnlib.clean(lib_isbn)
    if len(lib_isbn) < 10:
        return None
    lib_isbn = isbnlib.get_canonical_isbn(lib_isbn)
    if len(lib_isbn) < 10:
        return None
    if not lib_isbn:
        return None
    if len(lib_isbn) in (10, 13):
        if len(lib_isbn) == 10:
            isgood = isbnlib.is_isbn10(lib_isbn)
        else:
            isgood = isbnlib.is_isbn13(lib_isbn)
    else:
        return None
    if isgood:
        return lib_isbn
    else:
        return None


if __name__ == '__main__':
    colchoice = 0
    outchoice = 2
    lineskip = 0
    datafile = 'temp'
    datafile = input("Input file (tsv) default is temp: ")
    if datafile ==  '':
        datafile = 'temp'
    outfile = "LCC_" + datafile
    loggerfile = "log_" + datafile

    infologger = logging.getLogger()
    infologger.setLevel(logging.ERROR)  # DEBUG is the lowest, includes all
    infohandler = logging.FileHandler(loggerfile, 'w', 'utf-8')  # or whatever
    infoformatter = logging.Formatter('%(message)s')  # or whatever
    infohandler.setFormatter(infoformatter)  # Pass handler as a parameter, not assign
    infologger.addHandler(infohandler)
    outfile = "LCC_"+datafile
    sqllite_fullpath = sqlitefile
    print("Working, this can take a long time if a big input list")

    with open(datafile, newline='', encoding='utf-8') as f, open(outfile, 'w', encoding="utf-8",errors="ignore",newline='') as result:
        reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        if lineskip > 0:
            print(f"Skipping the top {str(lineskip)} lines of the data file...\n")
        writer = csv.writer(result, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
        if lineskip > 0:
            for i in range(1,lineskip+1):  # get past header lines if any
                row1 = next(reader)
                if outchoice == 1:
                    writer.writerow(row1)
        for row in reader:
            try:
                _ = row[0]
            except:
                continue
            lcc = None
            if len(row) < colchoice-1:  # something is wrong with this data line
                infologger.error("Not enough columns: {}\t".format(row))
            else:
                """ Here's the row by row logic for whatever you need to fix """
                if not row[0] or row[0] == '':
                    infologger.error("Missing ISBN, skipping line: {}\t".format(row))
                    if outchoice == 1:
                        writer.writerow(row+ ['',''])
                        result.flush()
                        continue
                else:
                    original_isbn = row[colchoice]
                    infologger.debug(f'ISBN to try: {original_isbn}')
                    if len(original_isbn) < 10:
                        infologger.error(f"Starting isbn too short to be valid, skipping: {original_isbn} from row: {row}")
                        if outchoice == 1:
                            writer.writerow(row+ ['',''])
                            result.flush()
                            continue
                    fixed_isbn = fix_isbn(row[colchoice])
                    if not fixed_isbn:
                        infologger.error(f"Unable to fix bad ISBN, skipping row: {row}")
                        if outchoice == 1:
                            writer.writerow(row+ ['',''])
                            result.flush()
                            continue
                    else:  # try first to see if we already have this isbn's LCC in sqlite file
                        sql_entry = None
                        sql_entry = sqlite_search(sqllite_fullpath,fixed_isbn)
                        if sql_entry:
                            lcc = sql_entry[1]
                            source = sql_entry[2]
                        if not lcc: # try to get LCC from OCLC Classify
                            lcc = get_oclc_data('isbn',fixed_isbn)
                            if lcc:
                                source = 'OCLC'
                        if not lcc:  # try to get LCC from Harvard
                            lcc = harvard_get(fixed_isbn)
                            if lcc:
                                source = 'Harvard'
                        if not lcc:  # try to get LCC from Library of Congress
                            lcc = loc_search(fixed_isbn,LOC_Base)
                            if lcc:
                                source = 'LofC'
                        if not lcc:  # try Stanford
                            lcc = get_blacklight('https://searchworks.stanford.edu','Stanford',fixed_isbn)
                            if lcc:
                                source = 'Stanford'
                        if not lcc:  # try Yale
                            lcc = get_blacklight('https://search.library.yale.edu','Yale',fixed_isbn)
                            if lcc:
                                source = 'Yale'
                        if not lcc:  # try JHU
                            lcc = get_blacklight('https://catalyst.library.jhu.edu', 'JHU', fixed_isbn)
                            if lcc:
                                source = 'JHU'
                        if not lcc:  # try Columbia
                            lcc = get_blacklight('https://clio.columbia.edu', 'Columbia', fixed_isbn)
                            if lcc:
                                source = 'Columbia'
                        if not lcc:  # try Cornell
                            lcc = get_blacklight('https://newcatalog.library.cornell.edu', 'Cornell', fixed_isbn)
                            if lcc:
                                source = 'Cornell'
                        if not lcc:  # try PennState
                            lcc = get_blacklight('https://catalog.libraries.psu.edu', 'PennState', fixed_isbn)
                            if lcc:
                                source = 'PennState'
                        if not lcc:  # try NCSU
                            lcc = get_blacklight('https://catalog.lib.ncsu.edu', 'NCSU', fixed_isbn)
                            if lcc:
                                source = 'NCSU'
                        if not lcc:  # try UMichigan
                            lcc = get_blacklight('https://search.lib.umich.edu', 'UMichigan', fixed_isbn)
                            if lcc:
                                source = 'UMichigan'
                        if not lcc:  # try UWisc
                            lcc = get_blacklight('https://search.library.wisc.edu', 'UWisc', fixed_isbn)
                            if lcc:
                                source = 'UWisc'
                        if not lcc:  # try IndianaU
                            lcc = get_blacklight('https://iucat.iu.edu', 'IndianaU', fixed_isbn)
                            if lcc:
                                source = 'IndianaU'
                        if not lcc:  # try Duke
                            lcc = get_blacklight('https://find.library.duke.edu', 'Duke', fixed_isbn)
                            if lcc:
                                source = 'Duke'
                        # Finished looking, now to write out results
                        newdata = []
                        # Reminder, outchoice = 1 means to preserve all line data in output file
                        if outchoice == 1 and not lcc:
                            newdata = row + ['','NOTFOUND']
                        elif outchoice == 1 and lcc:
                            newdata = row + [lcc,source]
                        elif outchoice != 1 and lcc:
                            newdata = (original_isbn, lcc, fixed_isbn, source)
                        else:  # must be outchoice != 1 and not lcc
                            newdata = (original_isbn, "", fixed_isbn, "NOTFOUND")
                        writer.writerow(newdata)
                        print(f'Found new ISBN, writing it now...{original_isbn}\n')
                        result.flush()
                        if lcc and not sql_entry:  #found an LCC from one of the remote servers, add to sqlite
                            newdata_dict = {"ISBN": fixed_isbn, "LC": lcc, "LCSource": source}
                            try:
                                conni = sqlite3.connect(sqllite_fullpath)
                                ci = conni.cursor()
                            except Exception as cie:
                                infologger.error(f'Unable to open {sqllite_fullpath} to add new data {str(newdata_dict)} due to {cie} ')
                            else:
                                try:
                                    sql_tableinsert(ci, 'main', **newdata_dict)
                                    conni.commit()
                                except Exception as cie2:
                                    infologger.error(
                                        f'Unable to save new data {str(newdata_dict)} to {sqllite_fullpath} due to {cie2}')
                                conni.close()
                        if not lcc:
                            infologger.error(f"Unable to find LCC for: {original_isbn} (fixed as: {fixed_isbn})")

    result.close()
    print(f"Done: output in {outfile}, error log in {loggerfile}\n")
