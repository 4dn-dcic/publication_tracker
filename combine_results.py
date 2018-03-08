import json
import glob, sys, os

"""Optionally accepts single positional argument for directory in which to combine JSON files."""

if __name__ == '__main__':

    url_map = './pdfs/url_map.json'

    if len(sys.argv) > 1:
        url_map = sys.argv[1]

    try:
        url_map = json.load(open(url_map))
    except:
        url_map = None

    if len(sys.argv) > 2:
        working_dir = sys.argv[2]
        os.chdir(working_dir)

    output_dicts = []
    for file in glob.glob("*.json"):
        try:
            json_dict = json.load(open(file))
            if isinstance(json_dict, dict) and json_dict.get('awards') is not None and json_dict.get('parsing_errors') is not None:
                output_dicts.append(json_dict)
        except Exception as e:
            continue

    main_dict = {
        "awards" : {},
        "parsing_errors" : set(),
        "remaining" : set()
    }

    all_award_keys = set()

    for out_dict in output_dicts:
        for award_key in out_dict.get('awards', {}).keys():
            all_award_keys.add(award_key)
        for err_pdf in out_dict.get('parsing_errors', []):
            main_dict['parsing_errors'].add(err_pdf)
        for err_pdf in out_dict.get('remaining', []):
            main_dict['remaining'].add(err_pdf)

    all_award_keys = list(all_award_keys)

    for award in all_award_keys:
        main_dict["awards"][award] = set()
        for out_dict in output_dicts:
            for pdf_link in out_dict["awards"].get(award, []):
                main_dict["awards"][award].add(pdf_link)
        main_dict["awards"][award] = list(main_dict["awards"][award])

    main_dict['parsing_errors'] = list(main_dict['parsing_errors'])
    main_dict['remaining'] = list(main_dict['remaining'])

    # Clean up 'NONE' & parsing_errors:
    for award in all_award_keys:
        for pdf_link in main_dict["awards"].get(award, []):
            if pdf_link in main_dict["parsing_errors"]:
                main_dict["parsing_errors"].remove(pdf_link)
            if pdf_link in main_dict["remaining"]:
                main_dict["remaining"].remove(pdf_link)
            if award == 'NONE':
                continue
            elif pdf_link in main_dict["awards"].get('NONE', []):
                main_dict["awards"]["NONE"].remove(pdf_link)


    if url_map:
        if isinstance(url_map, list):
            url_map = { r[1] : r[0] for r in url_map }
        for award in all_award_keys:
            main_dict["awards"][award] = [ url_map.get(r, r) for r in main_dict["awards"][award] ]
        main_dict["parsing_errors"] = [ url_map.get(r, r) for r in main_dict["parsing_errors"] ]
        main_dict["remaining"] = [ url_map.get(r, r) for r in main_dict["remaining"] ]

    print(json.dumps(main_dict, sort_keys=True, indent=4))

