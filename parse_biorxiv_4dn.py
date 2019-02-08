"""
WARNING: Relies on web scraping, and may stop working at any time, depending on biorxiv updates.
Replace with API communication or similar at earliest availability.
Alternatively, look into using RSS feed (?).

`python3 parse_biorxiv_4dn.py --help`

This script incorporates code written by Alex Balashov to finda and download pdfs from the biorxiv 
4DN channel and parse them for grant numbers.

get_pub_list: 
- get list of publications from "http://connect.biorxiv.org/relate/content/66"
- creates data_pre/pub_list.json

get_pub_metadata_all:
- go to link for each publication. check if there is an update.
- update the file data_post/<id>.json with {latest={title, pdflink, etc}, version={title, pdflink, etc}, keeping old versions as is if any}

download_pdf_all:
- for each file in data_post/, download pdf if needed, to pdfs/

parse_pdf_all:
- for each file in data_post/, parse pdf if needed
- add to  daa_post/<id>.json with {latest={..., awards}, version={..., awards}, keeping old versions as is if any}


"""

from bs4 import BeautifulSoup
import os
import requests, sys, json, re
import io
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
import click

with open("data_pre/awards.json","r") as fp:
    awards_list=json.load(fp)

def get_pub_list():
    #get list of publications
    #in scrape_biorxiv, def get_list_of_pdf_urls(collection_page):

    collection_page = "http://connect.biorxiv.org/relate/content/66"
    print("getting publication list from " + collection_page)
    collection_page_html = requests.get(collection_page)
    soup = BeautifulSoup(collection_page_html.text, 'html.parser')
    entries = soup.find_all('div', attrs={'class': 'highwire-article-citation highwire-citation-type-highwire-article'})

    entries_url_list = {}
    for entry in entries:
        entry_link = entry.find_all('a')
        if len(entry_link) != 1:
            raise Exception('expected 1 link per div')
        title = entry_link[0]["title"]  
        href = entry_link[0]['href']
        id=re.match(".*short/(.*)",href)
        id=id.group(1)
        entries_url_list[id] = {"href":href,"title":title}
    outfname = "data_pre/pub_list.json"
    with open(outfname,"w") as fp:
        json.dump(entries_url_list, fp, indent=2)
        print("wrote " + outfname + " with " + str(len(entries_url_list)) + " entries")


def get_pub_metadata(id,href):
    fname_jsoneach = "data_post/" + id + ".json"
    pre_exists = False
    if os.path.exists(fname_jsoneach):
        with open(fname_jsoneach,"r") as fp:
            entry = json.load(fp)
            pre_exists = True
    else:
        entry = {}        

    entry_page_html = requests.get(href)
    soup = BeautifulSoup(entry_page_html.text, 'html.parser')
    version = soup.find('meta', attrs={'name' : "HW.pisa"}).get("content")
    date =  soup.find('meta', attrs={'name' : "DC.Date"}).get("content")
    authors = [ i.get("content") for i in soup.find_all('meta', attrs={'name' : "DC.Contributor"})]

    if version in entry.keys():
        print(id + " " + version + " already in records")
        return
    
    title = soup.find('meta', attrs={'name' : "DC.Title"}).get("content")    
    pdf_link  = "https://www.biorxiv.org" + soup.find('div', attrs={'id' : 'mini-panel-biorxiv_art_tools'}).find('a').get('href')
    url_path, filename = os.path.split(pdf_link)
    fname_pdf = os.path('pdfs/' + filename)

    thisversion = {
        "title" : title,
        "pdf_link" : pdf_link,
        "version" : version,
        "date" : date,
        "authors" : authors,
        "fname_pdf" : fname_pdf
    }

    entry[version] = thisversion
    entry["latest"] = thisversion
    
    with open(fname_jsoneach,"w") as fp:
        json.dump(entry,fp,indent=2)
    if(pre_exists):
        print(id + " " + version + " new version added")
    else:
        print(id + " " + version + " new record created")
        
def get_pub_metadata_all(test=False):
    infname = "data_pre/pub_list.json"
    with open(infname,"r") as fp:
        entries_url_list = json.load(fp)

    os.makedirs("data_post",exist_ok=True)
    counter = 0
    for id, value in entries_url_list.items():
        href = value["href"]
        get_pub_metadata(id, href)
        if test:
            counter = counter + 1
            if counter>=3:
                return

def download_pdf(fname_jsoneach):
    os.makedirs("pdfs",exist_ok=True)  
    with open(fname_jsoneach,"r") as fp:
        entry = json.load(fp)
    pdf_url = entry["latest"].get("pdf_link")
    if pdf_url is None:
       return
    fname_pdf = entry["latest"]["fname_pdf"]
    if os.path.isfile(fname_pdf):
        print(fname_pdf + ' already saved. Skipping.')
        return
    print('Downloading & saving ' + pdf_url)
    with open(fname_pdf, 'wb') as f:
        f.write(requests.get(pdf_url).content)

def download_pdf_all(test=False):
    fname_jsons = os.listdir("data_post")
    counter = 0
    for fname_jsoneach in fname_jsons:
        download_pdf("data_post/" + fname_jsoneach)
        if test:
            counter = counter + 1
            if counter>=3:
                return

def get_text_from_pdf(fname_pdf):
    pdf_io = open(fname_pdf, 'rb')
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    device = TextConverter(rsrcmgr, retstr, codec='utf-8', laparams=LAParams())
    # Create a PDF interpreter object.
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    # Process each page contained in the document.

    for page in PDFPage.get_pages(pdf_io):
        interpreter.process_page(page)

    return retstr.getvalue()

def parse_pdf(fname_jsoneach):
    with open(fname_jsoneach,"r") as fp:
        entry = json.load(fp)

    if "pmid" in entry["latest"].keys():
        return
    
    if "awards" in entry["latest"].keys():
        print(entry["latest"]["version"] + " already parsed, not rerunning")
        return
    else:
        print(entry["latest"]["version"] + " parsing for awards")

    pdf_url = entry["latest"]["pdf_link"]
    fname_pdf = entry["latest"]["fname_pdf"]
    if not os.path.exists(fname_pdf):
        print(fname_pdf + " does not exist, download file first")
        return
    try:
        data = get_text_from_pdf(fname_pdf)
        ret_list = []
        found_award = False
        for award in awards_list:
            if data.find(award) > -1:
                found_award = True
                ret_list.append(award)
        if not found_award:
            for award in awards_list:
                if data.find(award[2:]) > -1:
                    found_award = True
                    ret_list.append(award)
        if not found_award:
            ret_list.append('NONE')
        entry["latest"]["awards"] = ret_list
        entry[entry["latest"]["version"]]["awards"] = ret_list
        with open(fname_jsoneach,"w") as fp:
            json.dump(entry,fp,indent=2)
    except Exception as e:
        print('Failed to parse ' + fname_pdf + '. Check it manually.')
        return
        
def parse_pdf_all(test):
    fname_jsons = os.listdir("data_post")
    counter=0
    for fname_jsoneach in fname_jsons:
        parse_pdf("data_post/" + fname_jsoneach)
        if test:
            counter = counter + 1
            if counter>=3:
                return

@click.command()
@click.option('--step', type=click.Choice(['1', '2', '3', '4', 'all']), help="1: get_pub_list \n2: get_pub_metadata \n3: download_pdfs \n4: parse_pdfs \nall: all")
@click.option('--test', is_flag=True,help="for steps 2-4, run only on 3 publications")

def run(step,test):
    if step == None:
        print("step is required, see --help")
    if   step in ["1","all"]:
        get_pub_list()
    if step in ["2","all"]:
        get_pub_metadata_all(test)
    if step in ["3","all"]:
        download_pdf_all(test)
    if step in ["4","all"]:
        parse_pdf_all(test)

if __name__ == '__main__':
    run()
