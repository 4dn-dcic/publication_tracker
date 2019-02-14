Setup your virtualenv/venv by running `pip3 install -U -r requirements.txt`.

### Parsing biorxiv 4DN channel

Try `python3 parse_biorxiv_4dn.py --step all --test`

See `python3 parse_biorxiv_4dn.py --help` for more information.

1. `get_pub_list`:
   -  get list of publications from `http://connect.biorxiv.org/relate/content/66`
   -  create `data_pre/pub_list.json`
1. `get_pub_metadata_all`:
   - go to link for each publication. Check if there is an update.
   - Update the file `data_post/<id>.json` with `{latest={title, pdflink, etc}, version={title, pdflink, etc}, old_version={<retained if any>}`
1. `download_pdf_all`:
   - for each file in `data_post/`, download pdf if needed, to `pdfs/`
1. `parse_pdf_all`:
   - for each file in `data_post/`, parse pdf if needed
   - add to  `data_post/<id>.json` with `{latest={..., awards}`.

### Parsing nihreporter for 4DN awards

Try `python3 parse_nihreport_4dn.py --infname data_pre/Publ_08Feb2019_090308_30431073.csv`.

See header of `parse_nihreport_4dn.py` for complete instructions.

1. Download list of publication records from nihreporter and place in
data_pre.

1. Run `parse_nihreport_4dn.py` to create a file for each publication
as `data_post/PMID<pmid>.json`

### Collating results

1. Is this biorxiv already published? Match records by title and authors:
`python3 collate.py --match-pubs`

1. Take the records and output a file per award: `python3 collate.py --per-grant`



