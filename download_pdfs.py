
import os, io, sys, json
import requests
from multiprocessing.pool import ThreadPool
from scrape_biorxiv_pdfs import get_list_of_pdf_urls

def worker(pdf_url):
    url_path, filename = os.path.split(pdf_url)
    saved_as = os.path.abspath('./pdfs/' + filename)
    if os.path.isfile(saved_as):
        print(saved_as + ' already saved. Skipping.')
        return (pdf_url, saved_as)
    print('Downloading & saving ' + pdf_url)
    with open(saved_as, 'wb') as f:
        f.write(requests.get(pdf_url).content)
    return (pdf_url, saved_as)

def download_pdfs(pdf_list):
    pool = ThreadPool(processes=6)
    results = pool.map(worker, pdf_list)
    pool.close()
    pool.join()
    return results

if __name__ == '__main__':
    pdf_list = None
    outfile = './pdfs/url_map.json'

    # Scrape from URL
    if len(sys.argv) > 1 and sys.argv[1][0:4] == 'http':
        pdf_list = get_list_of_pdf_urls(sys.argv[1])

    if not pdf_list:
        pdf_list = json.load(open('./pdf_uris.json'))

    if outfile:
        with open(outfile, 'w') as f:
            f.write(json.dumps(download_pdfs(pdf_list), sort_keys=True, indent=4))
