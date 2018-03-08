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


awards_list = [ # TODO: put in own file?
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
    elif len(pdf_list) == 0 and collection_page[-5:] == '.json':     # JSON list / url_map.json
        pdf_list = [ pdf_item[1] for pdf_item in json.load(open(collection_page)) ]
    elif len(pdf_list) == 0 and collection_page:                    # Local directory of PDFs
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

    


