Setup your virtualenv/venv by running `pip3 install -U -r requirements.txt`.

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
