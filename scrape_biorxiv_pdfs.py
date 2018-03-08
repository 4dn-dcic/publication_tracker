"""
WARNING: Relies on web scraping, and may stop working at any time, depending on biorxiv updates.
Replace with API communication or similar at earliest availability.
Alternatively, look into using RSS feed (?).

Run this file to get list of PDF Urls, separated by spaces.
"""
import requests, sys, json
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

def scrape_pdf(entry_page_url):
    entry_page_html = requests.get(entry_page_url)
    soup = BeautifulSoup(entry_page_html.text, 'html.parser')
    page_panel = soup.find('div', attrs={'id' : 'mini-panel-biorxiv_art_tools'})
    pdf_link = page_panel.find('a').get('href') # It happens to be first link
    if pdf_link:
        return pdf_link

def get_list_of_pdf_urls(collection_page):
    entries_url_list = []
    collection_page_html = requests.get(collection_page)
    soup = BeautifulSoup(collection_page_html.text, 'html.parser')
    entries = soup.find_all('div', attrs={'class': 'highwire-article-citation highwire-citation-type-highwire-article'})

    for entry in entries:
        entry_link = entry.find('a') # It happens to be first link (title)
        if entry_link:
            entries_url_list.append(entry_link['href'])

    pool = ThreadPool(processes=6)
    results = pool.map(scrape_pdf, entries_url_list)
    pool.close()
    pool.join()
    return results

if __name__ == '__main__':
    collection_page = sys.argv[1]
    print(json.dumps(get_list_of_pdf_urls(collection_page)))