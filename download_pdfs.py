
import os, io, sys, json
import requests
from multiprocessing.pool import ThreadPool
from scrape_biorxiv_pdfs import get_list_of_pdf_urls

DEFAULT_PDF_LIST = [
    'https://www.biorxiv.org/content/early/2017/01/15/099911.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/07/095802.full.pdf',
    'https://www.biorxiv.org/content/early/2016/12/17/094946.full.pdf',
    'https://www.biorxiv.org/content/early/2016/12/15/094185.full.pdf',
    'https://www.biorxiv.org/content/early/2016/12/13/093476.full.pdf',
    'https://www.biorxiv.org/content/early/2016/11/27/090001.full.pdf',
    'https://www.biorxiv.org/content/early/2016/10/17/081448.full.pdf',
    'https://www.biorxiv.org/content/early/2016/09/23/076893.full.pdf',
    'https://www.biorxiv.org/content/early/2016/08/24/071357.full.pdf',
    'https://www.biorxiv.org/content/early/2016/01/30/038281.1.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/21/110239.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/15/100198.full.pdf',
    'https://www.biorxiv.org/content/early/2016/09/09/074294.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/30/103614.full.pdf',
    'https://www.biorxiv.org/content/early/2016/11/07/086017.full.pdf',
    'https://www.biorxiv.org/content/early/2016/07/23/065052.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/18/101477.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/23/111179.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/31/099523.full.pdf',
    'https://www.biorxiv.org/content/early/2016/08/01/066464.full.pdf',
    'https://www.biorxiv.org/content/early/2016/10/21/082339.full.pdf',
    'https://www.biorxiv.org/content/early/2016/11/16/088146.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/26/103499.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/01/104653.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/27/112268.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/01/112631.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/27/111690.full.pdf',
    'https://www.biorxiv.org/content/early/2016/11/22/089011.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/08/105676.full.pdf',
    'https://www.biorxiv.org/content/early/2017/08/04/101386.full.pdf',
    'https://www.biorxiv.org/content/early/2016/03/15/024620.full.pdf',
    'https://www.biorxiv.org/content/early/2018/02/22/115436.full.pdf',
    'https://www.biorxiv.org/content/early/2016/12/05/091827.full.pdf',
    'https://www.biorxiv.org/content/early/2017/07/16/083212.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/30/121889.full.pdf',
    'https://www.biorxiv.org/content/early/2017/07/09/123588.full.pdf',
    'https://www.biorxiv.org/content/early/2017/04/10/125815.full.pdf',
    'https://www.biorxiv.org/content/early/2017/04/18/128280.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/28/119651.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/18/138289.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/17/138677.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/18/139782.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/25/142026.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/12/099804.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/02/145110.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/26/156091.full.pdf',
    'https://www.biorxiv.org/content/early/2016/12/15/094466.full.pdf',
    'https://www.biorxiv.org/content/early/2017/07/12/162156.full.pdf',
    'https://www.biorxiv.org/content/early/2017/12/15/165340.full.pdf',
    'https://www.biorxiv.org/content/early/2017/08/03/171801.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/31/171819.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/08/133124.full.pdf',
    'https://www.biorxiv.org/content/early/2017/01/26/103358.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/13/177832.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/02/104844.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/21/110668.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/26/155473.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/04/111674.full.pdf',
    'https://www.biorxiv.org/content/early/2017/02/24/083014.full.pdf',
    'https://www.biorxiv.org/content/early/2017/08/10/172643.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/26/142604.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/08/134767.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/26/171983.full.pdf',
    'https://www.biorxiv.org/content/early/2017/08/17/177766.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/24/189373.full.pdf',
    'https://www.biorxiv.org/content/early/2017/09/20/191213.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/17/204776.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/20/206094.full.pdf',
    'https://www.biorxiv.org/content/early/2018/02/05/085241.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/16/117531.full.pdf',
    'https://www.biorxiv.org/content/early/2017/03/20/118737.full.pdf',
    'https://www.biorxiv.org/content/early/2017/04/25/130567.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/09/135905.full.pdf',
    'https://www.biorxiv.org/content/early/2017/05/09/135947.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/05/146241.full.pdf',
    'https://www.biorxiv.org/content/early/2017/06/21/153098.full.pdf',
    'https://www.biorxiv.org/content/early/2017/07/14/158352.full.pdf',
    'https://www.biorxiv.org/content/early/2017/07/03/159004.full.pdf',
    'https://www.biorxiv.org/content/early/2017/08/10/174649.full.pdf',
    'https://www.biorxiv.org/content/early/2017/09/05/184846.full.pdf',
    'https://www.biorxiv.org/content/early/2018/02/05/188755.full.pdf',
    'https://www.biorxiv.org/content/early/2017/09/15/189654.full.pdf',
    'https://www.biorxiv.org/content/early/2017/09/29/195966.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/03/196261.full.pdf',
    'https://www.biorxiv.org/content/early/2017/09/30/196345.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/02/197566.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/19/205740.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/29/206367.full.pdf',
    'https://www.biorxiv.org/content/early/2017/10/25/208710.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/09/212928.1.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/06/214361.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/13/218768.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/15/219253.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/18/219683.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/21/221762.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/21/222794.full.pdf',
    'https://www.biorxiv.org/content/early/2017/11/21/222877.full.pdf',
    'https://www.biorxiv.org/content/early/2018/02/04/259515.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/09/237834.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/09/244038.full.pdf',
    'https://www.biorxiv.org/content/early/2018/02/16/264648.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/25/252049.full.pdf',
    'https://www.biorxiv.org/content/early/2017/12/24/239368.full.pdf',
    'https://www.biorxiv.org/content/early/2018/01/28/254797.full.pdf',
    'https://www.biorxiv.org/content/early/2017/12/29/240747.full.pdf'
]

def worker(pdf_url):
    url_path, filename = os.path.split(pdf_url)
    saved_as = os.path.abspath('./pdfs/' + filename)
    if os.path.isfile(saved_as):
        print(filename + ' already saved. Skipping.')
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
        # TODO: Load local file?
        pdf_list = DEFAULT_PDF_LIST
    if len(sys.argv) > 2:
        outfile = sys.argv[2]

    if outfile:
        with open(outfile, 'w') as f:
            f.write(json.dumps(download_pdfs(pdf_list), sort_keys=True, indent=4))