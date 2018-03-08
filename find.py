import sys
import json
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
import io, os
from bs4 import BeautifulSoup
import requests
import multiprocessing
import glob
from scrape_biorxiv_pdfs import get_list_of_pdf_urls as biorxiv_get_pdf_list


awards_list = [
    'CA200060',
    'CA200147',
    'DA040582',
    'DA040583',
    'DA040588',
    'DA040601',
    'DA040612',
    'DA040709',
    'EB021223',
    'EB021230',
    'EB021232',
    'EB021236',
    'EB021237',
    'EB021238',
    'EB021239',
    'EB021240',
    'EB021247',
    'HL129958',
    'HL129971',
    'HL129998',
    'HL130007',
    'HL130010',
    'DK107979',
    'DK107980',
    'DK107965',
    'DK107967',
    'DK107977',
    'DK107981',
    'CA200059'
]

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
    #'https://www.biorxiv.org/content/early/2017/10/20/206094.full.pdf',
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

def get_text_from_pdf(pdf_url):

    pdf_io = None
    if pdf_url[0:4] == 'http':  # Remote PDF file
        pdf_io = io.BytesIO(requests.get(pdf_url).content)
    else:                       # Local PDF File
        pdf_io = open(pdf_url, 'rb')

    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    device = TextConverter(rsrcmgr, retstr, codec='utf-8', laparams=LAParams())
    # Create a PDF interpreter object.
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    # Process each page contained in the document.

    for page in PDFPage.get_pages(pdf_io):
        interpreter.process_page(page)

    return retstr.getvalue()

def progress_output(outstring):
    sys.stdout.write(' ' + outstring + '\r')
    sys.stdout.flush()

def worker(args):
    idx = args[0]
    pdf_link = args[1]
    try:
        ret_list = []
        data = get_text_from_pdf(pdf_link)
        found_award = False
        for award in awards_list:
            if data.find(award) > -1:
                found_award = True
                ret_list.append((award, pdf_link))
        if not found_award:
            ret_list.append(('NONE', pdf_link))
        return ret_list
    except Exception as e:
        print('Failed to parse ' + pdf_link + '. Check it manually.')
        return [('ERROR', pdf_link)]

def collect_results(award_matches, out_file, all_pdfs):
    
    def add_to_ret_dict(ret_dict, flattened_matches):
        for award, pdf_link in flattened_matches:
            if award == 'ERROR':
                ret_dict["parsing_errors"].append(pdf_link)
            else:
                if ret_dict["awards"].get(award) is None:
                    ret_dict["awards"][award] = []
                ret_dict["awards"][award].append(pdf_link)
            if pdf_link in ret_dict['remaining']:
                ret_dict['remaining'].remove(pdf_link)
        return ret_dict

    # Flatten, Collect
    count_collected = 0
    flattened_matches = []
    ret_dict = {
        "remaining" : all_pdfs[:],
        "parsing_errors" : [],
        "awards" : {}
    }
    for result_list in award_matches:
        count_collected += 1
        progress_output('(' + str(count_collected) + '/' + str(count_pdf) + ') Found ' + str(len([ r for r in result_list if r[0] != 'NONE'])) + ' awards in ' + result_list[0][1] + '   \r')
        ret_dict = add_to_ret_dict(ret_dict, result_list)
        write_result_to_file(out_file, ret_dict)

    return ret_dict

def write_result_to_file(out_file, ret_dict):
    if out_file:
        try:
            with open(out_file, 'w') as f:
                json.dump(ret_dict, f, sort_keys=True, indent=4)
        except:
            print('Failed to save to ' + out_file)
            print(json.dumps(ret_dict, indent=4, sort_keys=True))

if __name__ == '__main__':
    '''
    :param argv[1]: Filename / path to write output to.
    :param argv[2]: URL to Biorxiv collection page to scrape. Or local path to PDF folder. Uses default PDF list if not supplied.
    '''
    out_file = None
    collection_page = None
    pdf_list = []

    if len(sys.argv) > 2:
        out_file = sys.argv[1]
        collection_page = sys.argv[2]
    else:
        print('Need an output file name (1st arg) and url/file list (.json) or directory of PDFs (2nd arg).')

    
    entries_url_list = []

    print('Running, please wait...')

    # Scrape PDF Urls if collection_page supplied
    if len(pdf_list) == 0 and collection_page and collection_page[0:4] == 'http':
        pdf_list = biorxiv_get_pdf_list(collection_page)
    elif len(pdf_list) == 0 and collection_page[-5:] == '.json': # JSON list
        pdf_list = [ pdf_item[1] for pdf_item in json.load(open(collection_page)) ]
    elif len(pdf_list) == 0 and collection_page: # collection_page is OS dir.
        curr_dir = os.getcwd()
        os.chdir(collection_page)
        for pdf_filename in glob.glob("*.pdf"):
            pdf_list.append(os.path.join(collection_page, pdf_filename))
        os.chdir(curr_dir)

    # Load, Convert, Check PDF
    count_pdf = len(pdf_list)
    ret_dict = None

    if count_pdf > 0:
        try:
            pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
            ret_dict = collect_results(pool.imap_unordered(worker, enumerate(pdf_list)), out_file, pdf_list)
        except KeyboardInterrupt:
            print('Exiting...')
            pool.close()
            pool.terminate()
            pool.join()
        else:
            pool.close()
            pool.join()

            if not out_file:
                print(json.dumps(ret_dict, indent=4, sort_keys=True))

    


