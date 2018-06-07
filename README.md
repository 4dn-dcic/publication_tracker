# Usage


## Step 0
Setup your virtualenv/venv if needed. Run `pip3 install -U -r requirements.txt`.

## Step 1
Run `python3 download_pdfs.py http://connect.biorxiv.org/relate/content/66` to scrape or `python3 download_pdfs.py` to use pre-set list of PDF URLs.

This will save PDFs to ./pdfs/ directory, as well as a 'url_map.json' with list of remote URLs & local filenames.

### Step 1a
Can update pre-set list of PDF URLs from biorxiv by running `python3 scrape_biorxiv_pdfs.py http://connect.biorxiv.org/relate/content/66 > pdf_uris.json`, and then resuming step 1 at `python3 download_pdfs.py` to download these pdf uris into ./pdfs/ directory.

## Step 2
Run `python3 find.py outfile.json ./pdfs/url_map.json` will then parse these PDFs as text and find which awards are present. Will continuously update/save outfile.json with result as each PDF file is processed in case of failures or a long-running/hanging PDF. 

### Alternatively:
Can run `python3 find.py outfile.json http://connect.biorxiv.org/relate/content/66` to download/parse PDFs directly without saving locally but this is not recommended due to potential network issues.

Can also run `python3 find.py outfile.json ./pdfs/` (or other local directory) to parse all PDFs in a local directory without using url_map to keep track of remote file location.

## Step 3
After any number of runs, run `python3 combine_results.py ./pdfs/url_map.json > output.json` which combine results of all/any JSON files output in step 2 (in case of multiple runs due to network issues) in working directory into one result-set, and most importantly, convert local PDF filenames to remote PDF URLs.
