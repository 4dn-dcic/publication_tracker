import collections
import os
import json
import click
from difflib import SequenceMatcher as SM
from itertools import combinations
from datetime import date
import boto3
import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import time
import string
import pandas as pd
from collections import OrderedDict


def match_pubs_titleauthor():
    '''
    look for records with very similar titles and authors to fill "other_ids" field
    if biorxiv and pubmed match, mark biorxiv with exclude flag
    if two pubmeds match, mark latter with exclude flag. (typically erratums)
    '''
    print("matching publications by titles and authors, this make take some time")

    def get_primary_and_secondary_awards(publication, warning1_log, warning2_log):

        def is_same_author(author1, author2_fname, author2_lname):
            '''
            Determines if the author in the publication (author1) is the same as the 4DN member (author2)
            '''

            if author2_lname == 'Qi':
                if 'Lei' in author1[:len(author2_fname)] and 'Qi' in author1[-len(author2_lname):]:
                    return True
                else:
                    return False

            elif author2_lname == 'Hu' or author2_lname == 'Li' or author2_lname == 'Chen':
                score1 = SM(None, author1[:len(author2_fname)].lower(), author2_fname.lower()).ratio()
                score2 = SM(None, author1[-len(author2_lname):].lower(), author2_lname.lower()).ratio()
                if score1 > 0.95 and score2 > 0.95:
                    return True
                else:
                    return False

            else:
                author2_fullname = author2_fname + ' ' + author2_lname

                score1 = SM(None, author1.lower(), author2_fullname.lower()).ratio()

                score2 = SM(None, author1[-len(author2_lname):].lower(), author2_lname.lower()).ratio()

                if score1 > 0.85 or (score1 > 0.65 and score2 > 0.90):
                    return True
                else:
                    return False

        primary_award = []
        secondary_award = []
        main_4DN_author_in_pub = 'none'
        all_4DN_authors_in_pub = []
        isthere_primary_award = False
        isthere_4DN_member = False
        warn1 = False
        warn2 = False

        wdir = os.getcwd()
        with open(wdir + "/Pis_and_awards.json", 'r') as f:
            pi_and_awards_portal = json.load(f)

        # if publication['id'] == "103614":
        #     import pdb; pdb.set_trace()

        authors_in_pub = publication.get('authors')
        awards_in_pub = publication.get('awards', None)

        for last_author in reversed(authors_in_pub):
            for pi in pi_and_awards_portal.values():
                secondary_award = []
                fname_pi = pi['first_name']
                lname_pi = pi['last_name']
                awards_pi = pi['awards']
                if is_same_author(last_author, fname_pi, lname_pi):
                    isthere_4DN_member = True
                    full_name_pi = fname_pi + ' ' + lname_pi
                    all_4DN_authors_in_pub.append(full_name_pi)
                    if awards_in_pub == None or awards_in_pub[0] == "NONE":
                        awards_in_pub = ['CA200147' if 'TCPA' in award else award[4:-3] for award in awards_pi]
                    for award_in_pub in awards_in_pub:
                        for award_of_pi in awards_pi:
                            select_award = None
                            if award_in_pub == award_of_pi[4:-3]:
                                select_award = award_in_pub
                                break
                            elif award_in_pub == 'CA200147' and 'TCPA' in award_of_pi:
                                select_award = award_of_pi
                                break

                        if len(primary_award) == 0 and select_award is not None:
                            primary_award.append(select_award)
                            isthere_primary_award = True
                            main_4DN_author_in_pub = full_name_pi
                        else:
                            secondary_award.append(award_in_pub)
                    break

            if isthere_primary_award:
                break

        # Catching when authors in pub are not in the PI list, but award in pub is 4DN award
        if isthere_4DN_member is False:
            PIs_4DN_with_pub_award = []
            for award_in_pub in awards_in_pub:
                for pi in pi_and_awards_portal.values():
                    fullname_pi = pi['first_name'] + ' ' + pi['last_name']
                    for award_of_pi in pi['awards']:
                        if award_in_pub in award_of_pi:
                            PIs_4DN_with_pub_award.append(fullname_pi)
                primary_award.append(award_in_pub)
                warning1_log['no_4DN_authors'][publication['fname']] = {'awards': publication['awards'], 'authors': publication['authors'], '4DN_PIs_with_pub_award': PIs_4DN_with_pub_award}
                warn1 = True
        if isthere_4DN_member is True and isthere_primary_award is False:
            for award_in_pub in awards_in_pub:
                primary_award.append(award_in_pub)
            secondary_award = []
            warning2_log['no_matching_awards'][publication['fname']] = {'awards_in_pub': awards_in_pub, 'authors_in_pub': authors_in_pub, '4DN_author_in_pub': all_4DN_authors_in_pub}
            warn2 = True
        return primary_award, secondary_award, warn1, warn2, main_4DN_author_in_pub

    entries = {}
    fname_jsons = os.listdir("data_post")
    for fname_jsoneach in fname_jsons:
        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry = json.load(fp)
            version = entry["latest"]
            name = version["source"] + ":" + version["id"]
            entries[name] = version
            entries[name]["other_ids"] = []
            entries[name]["fname"] = fname_jsoneach
            entries[name]["primary_awards"] = []
            entries[name]["secondary_awards"] = []
            entries[name]["4DN_member_author"] = 'none'

    for (n1, t1), (n2, t2) in combinations(entries.items(), 2):
        score1 = SM(None, t1["title"], t2["title"]).ratio()
        score2 = SM(None, " ".join(t1["authors"]), " ".join(t2["authors"])).ratio()
        if score1 > 0.75 or (score1 > 0.6 and score2 > 0.9):
            entries[n1]["other_ids"].append(n2)
            entries[n2]["other_ids"].append(n1)
            if(t1["source"] == "biorxiv" and t2["source"] == "PubMed"):
                entries[n1]["exclude"] = True
            elif(t2["source"] == "biorxiv" and t1["source"] == "PubMed"):
                entries[n2]["exclude"] = True
            elif(t1["source"] == "PubMed" and t2["source"] == "PubMed"):
                if(t1["id"] < t2["id"]):
                    entries[n2]["exclude"] = True
                else:
                    entries[n1]["exclude"] = True
            else:
                raise Exception("two biorxivs matched", t1["title"], t2["title"])

    num_pubs = 0
    warn1_ct = 0
    warn2_ct = 0
    warning_log1 = collections.defaultdict(dict)
    warning_log2 = collections.defaultdict(dict)

    for entry in entries.values():
        # Makes sure the awards in pub are in the same format
        awards_in_entry = entry.get('awards', None)
        if awards_in_entry:
            indx = 0
            for award in awards_in_entry:
                if len(award) > 8:
                    awards_in_entry[indx] = award[3:]
                indx += 1

        num_pubs += 1
        isthere_warn1 = False
        isthere_warn2 = False
        primary_awards, secondary_awards, isthere_warn1, isthere_warn2, author_4DN_in_pub = get_primary_and_secondary_awards(entry, warning_log1, warning_log2)
        entry['primary_awards'] = primary_awards
        entry['secondary_awards'] = secondary_awards
        entry['4DN_member_author'] = author_4DN_in_pub

        fname_jsoneach = entry.pop("fname")
        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry_orig = json.load(fp)
            entry_orig["latest"] = entry
        with open("data_post/" + fname_jsoneach, "w") as fp:
            json.dump(entry_orig, fp, indent=2)

        if isthere_warn1:
            warn1_ct += 1
        if isthere_warn2:
            warn2_ct += 1

    os.makedirs("warning_logs", exist_ok=True)
    with open("warning_logs/no_4DN_authors.json", 'w') as fp:
        json.dump(warning_log1, fp, indent=3)

    with open("warning_logs/4DN_authors_no_matching_awards.json", 'w') as fp:
        json.dump(warning_log2, fp, indent=3)

    print("done!")
    print("There are {} publications.".format(num_pubs))
    print("{} do not have 4DN members as authors. See warning logs".format(warn1_ct))
    print("{} do not have matching awards with 4DN members authors. See warning logs".format(warn2_ct))


def write_per_grant():
    '''
    turn per-publication entries to per-grant entries
    write output to data_grant/ with added=<today>
    (if the publication was already in data_grant/<grant>.json file
    then retain the old added date)
    '''
    fname_jsons = os.listdir("data_post")
    dict_pergrant = collections.defaultdict(dict)
    for fname_jsoneach in fname_jsons:
        print(fname_jsoneach)
        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry = json.load(fp)
            record = entry["latest"]
            if record.get("exclude", False) or record.get("primary_awards") is None:
                continue
            this = {k: record.get(k, None) for k in ('source', 'id', 'title', 'authors', 'date', 'link', 'other_ids', 'primary_awards', 'secondary_awards', '4DN_member_author')}
            for award in record['primary_awards']:
                dict_pergrant[award][this["id"]] = this

    os.makedirs("data_grant", exist_ok=True)
    for award in dict_pergrant.keys():
        fname_pergrant = "data_grant/" + award + ".json"
        if os.path.exists(fname_pergrant):
            with open(fname_pergrant, "r") as fp:
                entry = json.load(fp)
        else:
            entry = {}

        today = str(date.today())

        for id in dict_pergrant[award].keys():
            added = today
            if id in entry.keys():
                added = entry[id]["added"]
            dict_pergrant[award][id]["added"] = added

        with open(fname_pergrant, "w") as fp:
            json.dump(dict_pergrant[award], fp, indent=2)


def write_tables_per_grant():
    '''
    read data_grant/<grant>.json and output data_grant/<grant> tables to google sheets
    '''
    def update_google_sheets():
        # Read the existing google sheet
        worksheet_values = worksheet.get_all_values()
        df = pd.DataFrame(worksheet_values[1:], columns=worksheet_values[0])
        col_names = df.columns.values
        google_sheet = df.to_dict(orient='records', into=OrderedDict)

        # Comparing prublications with grant as primary award with existing google sheets and adding new publications
        new_sheet_existing_primary = []
        new_sheet_new_primary = []
        for a_row in pubs_when_grant_primary[grant].values():
            # check excel
            existing_item = [i for i in google_sheet if a_row['title'] == i['Title']]
            if existing_item:
                new_sheet_existing_primary.append(existing_item)
            else:
                new_sheet_new_primary.append(a_row)

        # Comparing publications with grant as secondary award with existing google sheets and adding new publications
        new_sheet_existing_secondary = []
        new_sheet_new_secondary = []
        for a_row in pubs_when_grant_secondary[grant].values():
            # check excel
            existing_item = [i for i in google_sheet if a_row['title'] == i['Title']]
            if existing_item:
                new_sheet_existing_secondary.append(existing_item)
            else:
                new_sheet_new_secondary.append(a_row)

        # Adding the existing publications in the google sheets that were added manually
        gs_only_items = []
        for a_row in google_sheet:
            if a_row['Title'] == '' or a_row['Title'] == 'Publications in which this award is listed but it is not primary':
                continue
            existing_item_1 = [i for i in pubs_when_grant_primary[grant].values() if a_row['Title'] == i['title']]
            existing_item_2 = [i for i in pubs_when_grant_secondary[grant].values() if a_row['Title'] == i['title']]
            if existing_item_1 or existing_item_2:
                continue
            else:
                gs_only_items.append(a_row)

        # Writting info to the google sheets
        col_order = {'title': 1, 'authors': 2, 'date':3, 'contact_4DN_author':4,'primary_awards':len(col_names) - 4, 'secondary_awards':len(col_names) - 3, 'other_ids':len(col_names) - 2, 'link':len(col_names) - 1, 'date_added': len(col_names)}
        gs_write = []
        row = 1
        # Writting the existing info
        for r, line in enumerate(new_sheet_existing_primary):
            row = r + 1
            if row == 1:  # Writes the labels of the columns (the keys)
                for c, key in enumerate(line[0]):
                    col = c + 1
                    gs_write.append(gspread.models.Cell(row, col, key))
            row = r + 2
            for c, key in enumerate(line[0]):
                col = c + 1
                gs_write.append(gspread.models.Cell(row, col, line[0][key]))
        # Writing existing info in gs only
        for line in gs_only_items:
            row = row + 1
            for c, key in enumerate(line):
                col = c + 1
                gs_write.append(gspread.models.Cell(row, col, line[key]))

        # Writting new info
        for line in new_sheet_new_primary:
            row = row + 1
            for key in line:
                col = col_order[key]
                gs_write.append(gspread.models.Cell(row, col, line[key]))

        # Writting message before publications in which grant is secondary
        row = row + 1
        spaces = ['' for x in range(len(col_names))]
        message = ['Publications in which this award is listed but it is not primary' if x == 0 else '' for x in range(len(col_names))]

        for c, txt in enumerate(spaces):
            col = c + 1
            gs_write.append(gspread.models.Cell(row,col,txt))

        row = row + 1
        for c, txt in enumerate(message):
            col = c + 1
            gs_write.append(gspread.models.Cell(row,col,txt))
            format_range_message = [('A{}'.format(row),fmt_bold)]

        # Writting secondary awards existing info
        for r, line in enumerate(new_sheet_existing_secondary):
            row = row + 1
            for c, key in enumerate(line[0]):
                col = c + 1
                gs_write.append(gspread.models.Cell(row, col, line[0][key]))

        # Writting secondary awards new info
        for r, line in enumerate(new_sheet_new_secondary):
            row = row + 1
            for c, key in enumerate(line):
                col = col_order[key]
                gs_write.append(gspread.models.Cell(row, col, line[key]))

        # Overwrites existing google sheet with new information
        # worksheet.update_cells(gs_write,value_input_option='USER_ENTERED')

        # Adding formatting to worksheet
        num_rows = len(gs_write)//len(df.columns)
        fmt_plain = cellFormat(textFormat=textFormat(bold=False))

        range_worksheet_formatting = [('A1:{}{}'.format(num_to_letter_dict[len(df.columns)],num_rows),fmt_plain)]
        range_title_formatting = [('A1:{}1'.format(num_to_letter_dict[len(df.columns)]), fmt_bold)]

        format_cell_ranges(worksheet,range_worksheet_formatting)  # Clean the formatting of worksheet
        format_cell_ranges(worksheet,format_range_message)  # Make the title bold
        format_cell_ranges(worksheet,range_title_formatting)  # Make the subtitle bold


        return '{} new primary publications and {} new secondary publications'.format(len(new_sheet_new_primary), len(new_sheet_new_secondary))

    pubs_when_grant_primary = collections.defaultdict(dict)
    pubs_when_grant_secondary = collections.defaultdict(dict)

    fname_grant_jsons = os.listdir("data_grant")
    for fname_json in fname_grant_jsons:
        with open("data_grant/" + fname_json, "r") as fp:
            dic_pergrant = json.load(fp)
            pubs = dic_pergrant.keys()
            grant_name = fname_json[:-5]
            pubs_when_grant_primary[grant_name] = collections.defaultdict(dict)
            pubs_when_grant_secondary[grant_name] = collections.defaultdict(dict)
            for pub in pubs:
                authors = "; ".join(dic_pergrant[pub]["authors"])
                title = dic_pergrant[pub]["title"]
                link = dic_pergrant[pub]["link"]
                date = dic_pergrant[pub]["date"]
                other_ids = ";".join(dic_pergrant[pub]["other_ids"])
                primary_awards = ";".join(dic_pergrant[pub]["primary_awards"])
                secondary_awards = ";".join(dic_pergrant[pub]["secondary_awards"])
                contact_4DN_author = dic_pergrant[pub]["4DN_member_author"]
                added_date = dic_pergrant[pub]["added"]

                pubs_when_grant_primary[grant_name][pub]['title'] = title
                pubs_when_grant_primary[grant_name][pub]['authors'] = authors
                pubs_when_grant_primary[grant_name][pub]['date'] = date
                pubs_when_grant_primary[grant_name][pub]['link'] = link
                pubs_when_grant_primary[grant_name][pub]['other_ids'] = other_ids
                pubs_when_grant_primary[grant_name][pub]['primary_awards'] = primary_awards
                pubs_when_grant_primary[grant_name][pub]['secondary_awards'] = secondary_awards
                pubs_when_grant_primary[grant_name][pub]['date_added'] = added_date
                pubs_when_grant_primary[grant_name][pub]['contact_4DN_author'] = contact_4DN_author

    # Keeps track of publications in which the grant is secondary
    for grant in pubs_when_grant_primary:
        for pub in pubs_when_grant_primary[grant]:
            if pubs_when_grant_primary[grant][pub]['secondary_awards'] != '':
                if ';' not in pubs_when_grant_primary[grant][pub]['secondary_awards']:
                    sec_award = pubs_when_grant_primary[grant][pub]['secondary_awards']
                    pubs_when_grant_secondary[sec_award][pub] = pubs_when_grant_primary[grant][pub].copy()
                else:
                    sec_award_list = pubs_when_grant_primary[grant][pub]['secondary_awards'].split(';')
                    for sec_award in sec_award_list:
                        pubs_when_grant_secondary[sec_award][pub] = pubs_when_grant_primary[grant][pub].copy()

            pubs_when_grant_primary[grant][pub]['primary_awards'] = ' -- '

    # Accesses to the google sheets
    s3 = boto3.resource('s3')
    obj = s3.Object('elasticbeanstalk-fourfront-webprod-system', 'DCICjupgoogle.json')
    cont = obj.get()['Body'].read().decode()
    key_dict = json.loads(cont)
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPES)
    gc = gspread.authorize(creds)
    f = open('google_sheet_link.txt', 'r')
    link = f.readline().split('\n')
    google_management_sheet_id = link[0]
    f.close()
    google_management_spreadsheet = gc.open_by_key(google_management_sheet_id)
    google_management_sheet = google_management_spreadsheet.sheet1
    grant_sheets_links = google_management_sheet.get_all_values()

    col_labels = ['Title', 'Authors', 'Date', 'Contact 4DN Author', 'Portal link', 'Primary awards', 'Secondary awards', 'Other ids', 'Publication link', 'Date added']
    num_to_letter_dict = {x:y for x, y in zip(range(1, 27), string.ascii_uppercase)}
    fmt_bold = cellFormat(textFormat=textFormat(bold=True))
    fmt_plain = cellFormat(textFormat=textFormat(bold=False))
    reqst_ct = 1

    for grant in pubs_when_grant_primary:
        # if grant != 'EB021223':  # test individual grant
        #     continue
        gs_write = []
        for item in grant_sheets_links:
            if grant in item[1]:
                spreadsheet_id = item[2]
                if reqst_ct >= 60:
                    time.sleep(100)
                    reqst_ct = 0
                    spreadsheet = gc.open_by_key(spreadsheet_id)
                    print('Working on ', spreadsheet)
                    reqst_ct += 1
                else:
                    spreadsheet = gc.open_by_key(spreadsheet_id)
                    print('Working on ', spreadsheet)
                    reqst_ct += 1

                if reqst_ct >= 60:
                    time.sleep(100)
                    reqst_ct = 0
                    try:
                        worksheet = spreadsheet.worksheet('Publication List')
                        reqst_ct += 1
                    except:
                        worksheet = spreadsheet.add_worksheet(title='Publication List', rows='200', cols='15')
                        reqst_ct += 1
                        for c, label in enumerate(col_labels):
                            col = c + 1
                            gs_write.append(gspread.models.Cell(1,col, label))
                            worksheet.update_cells(gs_write,value_input_option='USER_ENTERED')
                            reqst_ct += 1
                            gs_write = []
                else:
                    try:
                        worksheet = spreadsheet.worksheet('Publication List')
                        reqst_ct += 1

                    except:
                        worksheet = spreadsheet.add_worksheet(title='Publication List', rows='200', cols='15')
                        reqst_ct += 1
                        for c, label in enumerate(col_labels):
                            col = c + 1
                            gs_write.append(gspread.models.Cell(1,col, label))
                            worksheet.update_cells(gs_write,value_input_option='USER_ENTERED')
                            reqst_ct += 1
                            gs_write = []

                results = update_google_sheets()
                print(results + 'in ' + item[0])
                print()


@click.command()
@click.option('--match-pubs', is_flag=True, help="look for records with very similar titles and authors to fill other_ids field", default=False)
@click.option('--per-grant', is_flag=True, help="reorganize data to be per award under data_grant/", default=False)
@click.option('--out-tables', is_flag=True, help="convert jsons to tsvs in data_grant/", default=False)
def run(match_pubs, per_grant, out_tables):
    if(match_pubs):
        match_pubs_titleauthor()
    if(per_grant):
        write_per_grant()
    if(out_tables):
        write_tables_per_grant()


if __name__ == '__main__':
    run()
