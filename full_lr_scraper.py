import requests
from fake_useragent import UserAgent
import concurrent.futures
from unidecode import unidecode
import json
import random
import time
import fitz
import io
import re
import xlsxwriter
from DateTime import DateTime

print("Time of beginning of the report: ", DateTime())

# Open a session and configure. Unfortunately, 
# Retry library from urllib3 isn't an option due to fact that bad status codes are returned as a response in .json.
# Instead, custom retry logic has been incorporated in send_request(url) function

session = requests.Session()

# Declaring User Agent
ua = UserAgent()

# Proxy whitelist
proxy_whitelist = []

# defining Excel workbook and setting headers
workbook = xlsxwriter.Workbook('LR_Report.xlsx')
worksheet = workbook.add_worksheet("LR_Report")
headers = ["ZK Odjel/Sud", "Glavna knjiga", "Tip", "Broj ZK uloska/KPU poduloska", "Iznosi ovrhe", "URL PDF izvatka"]
header_format = workbook.add_format({'bold': True})
for i, header in enumerate(headers):
    worksheet.write(0, 0 + i, header, header_format)
worksheet.freeze_panes(1, 0)

row = 1
col = 0

current_office_cnt = 0

def is_integer_num(n):
    if isinstance(n, int):
        return True
    if isinstance(n, float):
        return n.is_integer()
    return False

def check_ascii(in_string):
    if in_string.isascii():
        return in_string
    else:
        return unidecode(in_string)  # Converts non-ascii characters to the closest ascii

def def_proxy_whitelist():
    proxylist = []

    with open('proxylist.txt', 'r') as f:
        proxyLines = f.readlines()
        for row in proxyLines:
            new_row = row.replace('\n', '')
            proxylist.append(new_row)

    lrOfficesURL = "https://oss.uredjenazemlja.hr/oss/public/codebooks/search-lr-offices?search="

    def extract(proxy):
        try:
            r = requests.get(lrOfficesURL, proxies={'http': proxy}, timeout = 2)
            proxy_whitelist.append(proxy)
        except:
            pass
        return proxy
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract, proxylist)

    print("[PROXY] - " + str(len(proxy_whitelist)) + " working proxies appended to the proxy whitelist!")

def send_request(url):    
    header = {'User-Agent': str(ua.chrome)}

    # Workaround for cases when non-responsive proxy gets picked
    try:
        response = session.get(url, headers=header, proxies={'http': random.choice(proxy_whitelist)}, timeout = 50)
    except:
        response = session.get(url, headers=header, proxies={'http': random.choice(proxy_whitelist)}, timeout = 50)

    while response.status_code != 200:
        try:
            try:
                response = session.get(url, headers=header, proxies={'http': random.choice(proxy_whitelist)}, timeout = 50)
            except:
                response = session.get(url, headers=header, proxies={'http': random.choice(proxy_whitelist)}, timeout = 50)
            
            break
        except Exception as ex:
            print("Exception - ", ex, "\nStatus code: ", response.status_code)
            time.sleep(2)
            continue
    
    # Skipping unescapeable corrupted jsons which cannot be loaded
    try:
        json_string = check_ascii(response.text)
        json_response = json.loads(json_string)
    except:
        return False

    json_string = check_ascii(response.text)
    json_response = json.loads(json_string)

    # Proper json response of any of the requests sent to oss.uredjena-zemlja.hr doesn't normally contain statusCode
    while 'statusCode' in json_response:
        time.sleep(2)
        response = session.get(url, headers=header, proxies={'http': random.choice(proxy_whitelist)}, timeout = 50)
        try:
            json_string = check_ascii(response.text)
            json_response = json.loads(json_string)
        except:
            return False
    
        json_string = check_ascii(response.text)
        json_response = json.loads(json_string)
    
    return response


# Main PDF scraper
def pdf_parse(url):
    request = requests.get(url)
    filestream = io.BytesIO(request.content)
    with fitz.open(stream=filestream, filetype="pdf") as doc:
        detail_judgement = ""
        for page in doc:
            words = page.get_text()
            detail_judgement += words
        
    words = detail_judgement.split()

    # Appending EUR, KN, CHF to the currencies that are getting scraped for the report
    currency_words = []

    for i in range(len(words)-1):
        if words[i].upper() == 'EUR' or words[i].upper() == 'KN' or words[i].upper() == 'CHF' or words[i].upper() == 'ATS':
            currency_words.append(" ".join((words[i-1],words[i].upper())))

    nine_currency_regex = r'(\d{1,3})([.]\d{3})([.]\d{3})([,]\d{0,2})*'
    six_currency_regex = r'(\d{3})([.]\d{3})([,]\d{0,2})*'

    regex_list = [nine_currency_regex, six_currency_regex]

    currency_list = []

    for currency_regex in regex_list:
        lists_of_text = list(filter(lambda x: re.match(currency_regex, x), currency_words))
        strings_of_text = set(lists_of_text)
        for n in strings_of_text:
            currency_list.append(n)

    # Removing duplicate values from the array
    currency_list = list(set(currency_list))
    
    if currency_list == []:
        return False
    
    return currency_list

# Pre-requisite step - confirming proxy whitelist that works for oss.uredjenazemlja.hr
def_proxy_whitelist()

lrOfficesURL = "https://oss.uredjenazemlja.hr/oss/public/codebooks/search-lr-offices?search="

offices_res = send_request(lrOfficesURL)
lrOfficesStr = check_ascii(offices_res.text)
lrOffices_list = json.loads(lrOfficesStr)

for offices in lrOffices_list:

    print("[DEBUG] Processing a new office.", current_office_cnt,"/", len(lrOffices_list), "offices are done. -  ", DateTime())

    current_office_cnt = current_office_cnt + 1

    mainBookURL = "https://oss.uredjenazemlja.hr/oss/public/search-lr-parcels/main-books?search=&officeId=" + offices['key1'] + "&institutionName="

    mainbook_res = send_request(mainBookURL)

    # Skipping unescapeable corrupted jsons that cannot be loaded
    if mainbook_res == False:
        break

    mainBookStr = check_ascii(mainbook_res.text)
    mainBook_list = json.loads(mainBookStr)
    
    # Parse trough all results

    for mainBooks in mainBook_list:
        lrParcelURL = 'https://oss.uredjenazemlja.hr/oss/public/search-lr-parcels/lr-units?search=&mainBookId=' + mainBooks['key1']
        
        lrParcel_res = send_request(lrParcelURL)

        # Skipping unescapeable corrupted jsons that cannot be loaded
        if lrParcel_res == False:
            break

        lrParcelStr = check_ascii(lrParcel_res.text)
        lrParcel_list = json.loads(lrParcelStr)

        for parcels in lrParcel_list:
            lrUnitURL = 'https://oss.uredjenazemlja.hr/oss/public/lr-units/by-parcel-number?mainBookId=' + mainBooks['key1'] + '&parcelNumber=' + '&lrUnitNumber=' + parcels['key2']
            
            time.sleep(2) # Pausing execution for 2 seconds to normalize bandwidth to oss.uredjenazemlja.hr

            lrUnit_res = send_request(lrUnitURL)

            # Skipping unescapeable corrupted jsons that cannot be loaded
            if lrUnit_res == False:
                break
            
            lrUnitStr = check_ascii(lrUnit_res.text)
            lrUnit_list = json.loads(lrUnitStr)

            for units in lrUnit_list:
                
                ldbExtractURL = 'https://oss.uredjenazemlja.hr/oss/public/lr-units/for-ldb-extract?lrUnitId=' + str(units['lrUnitId']) + '&historical=0'

                ldbExtract_res = send_request(ldbExtractURL)

                # Skipping unescapeable corrupted jsons
                if ldbExtract_res == False:
                    break

                ldbExtractStr = check_ascii(ldbExtract_res.text)
                ldbExtract_list = json.loads(ldbExtractStr)

                finalPDF_URL = 'https://oss.uredjenazemlja.hr/oss/public/reports/ldb-extract/' + ldbExtract_list['fileUrl']
                result = pdf_parse(finalPDF_URL)
                
                if result:
                    print("Found a matching ZK - ", units['institutionName'], " - ", units['mainBookName'], " - ", parcels['key2'], "- ", result)
                    for n in result:
                        worksheet.write(row, col, units['institutionName'])
                        worksheet.write(row, col + 1, units['mainBookName'])
                        worksheet.write(row, col + 2, units['lrUnitTypeName'])
                        worksheet.write(row, col + 3, parcels['key2'])
                        worksheet.write(row, col + 4, n)
                        worksheet.write(row, col + 5, finalPDF_URL)

                        row += 1
                        
print("\nWriting LR_Report.xlsx...")
print("Time of end of the report: ", DateTime())
workbook.close()

