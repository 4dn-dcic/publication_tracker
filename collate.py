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

    for entry in entries.values():
        fname_jsoneach = entry["fname"]
        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry_orig = json.load(fp)
            entry_orig["latest"] = entry
        with open("data_post/" + fname_jsoneach, "w") as fp:
            json.dump(entry_orig, fp, indent=2)

    print("done!")


def find_primary_and_secondary_awards():
    '''
    Identifies primary and secondary awards from the awards in the publications
    by matching the main 4DN author in the publication with the authors in the 4DN data portal.
    Primary awards: awards from the main 4DN member authors in the publication (last 3)
    Secondary awards: other awards
    '''
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

    entries = {}
    warning_log = collections.defaultdict(dict)
    fname_jsons = os.listdir("data_post")
    for fname_jsoneach in fname_jsons:
        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry = json.load(fp)
            version = entry["latest"]
            name = version["source"] + ":" + version["id"]
            entries[name] = version
            entries[name]["primary_awards"] = []
            entries[name]["secondary_awards"] = []
            entries[name]["4DN_member_author"] = 'none'
            entries[name]["fname"] = fname_jsoneach

    for entry in entries.values():
        primary_award = []
        secondary_award = []
        main_4DN_author_in_pub = 'none'
        all_4DN_authors_in_pub = []
        found_primary_award = False
        found_4DN_member = False

        # Makes sure the awards in pub are in the same format (Pubmed & Biorvix)
        awards_in_entry = entry.get('awards', None)
        if awards_in_entry:
            indx = 0
            for award in awards_in_entry:
                if len(award) > 8:
                    awards_in_entry[indx] = award[3:]
                indx += 1

        # Gets the list of PIs and their awards in the Portal
        wdir = os.getcwd()
        with open(wdir + "/Pis_and_awards.json", 'r') as f:
            pi_and_awards_portal = json.load(f)

        authors_in_pub = entry.get('authors')
        awards_in_pub = entry.get('awards', None)

        num_authors = 0
        for last_author in reversed(authors_in_pub):
            num_authors += 1
            if num_authors == 4:  # Just check the first 3 main authors
                break
            for pi in pi_and_awards_portal.values():
                secondary_award = []
                fname_pi = pi['first_name']
                lname_pi = pi['last_name']
                awards_pi = pi['awards']
                if is_same_author(last_author, fname_pi, lname_pi):
                    found_4DN_member = True
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
                                select_award = award_of_pi[:-1]
                                break

                        if len(primary_award) == 0 and select_award is not None:
                            primary_award.append(select_award)
                            found_primary_award = True
                            main_4DN_author_in_pub = full_name_pi
                        else:
                            secondary_award.append(award_in_pub)
                    break

            if found_primary_award:
                break

        # Catching when authors in pub are not in the PI list, but award in pub is 4DN award
        if found_4DN_member is False:
            PIs_4DN_with_pub_award = []
            for award_in_pub in awards_in_pub:
                for pi in pi_and_awards_portal.values():
                    fullname_pi = pi['first_name'] + ' ' + pi['last_name']
                    for award_of_pi in pi['awards']:
                        if award_in_pub in award_of_pi:
                            PIs_4DN_with_pub_award.append(fullname_pi)
                secondary_award.append(award_in_pub)
                warning_log['no_4DN_authors_found'][entry['fname']] = {'awards': entry['awards'], 'authors': entry['authors'], '4DN_PIs_with_pub_award': PIs_4DN_with_pub_award}

        # Catching when the award does not match between the 4DN author in pub and authors in the portal
        if found_4DN_member is True and found_primary_award is False:
            for award_in_pub in awards_in_pub:
                primary_award.append(award_in_pub)
            secondary_award = []
            warning_log['no_matching_awards'][entry['fname']] = {'awards_in_pub': awards_in_pub, 'authors_in_pub': authors_in_pub, '4DN_author_in_pub': all_4DN_authors_in_pub}

        # Assigning primary and secondary awards and main 4DN author fields to pub
        entry['primary_awards'] = primary_award
        entry['secondary_awards'] = secondary_award
        entry['4DN_member_author'] = main_4DN_author_in_pub
        fname_jsoneach = entry.pop("fname")

        with open("data_post/" + fname_jsoneach, "r") as fp:
            entry_orig = json.load(fp)
            entry_orig["latest"] = entry
        with open("data_post/" + fname_jsoneach, "w") as fp:
            json.dump(entry_orig, fp, indent=2)

    num_pubs = len(entries.keys())
    num_no_4DN_author_found = len(warning_log['no_4DN_authors_found'].keys())
    num_no_matching_awards = len(warning_log['no_matching_awards'].keys())

    os.makedirs("warning_logs", exist_ok=True)
    with open("warning_logs/no_4DN_authors_found.json", 'w') as fp:
        json.dump(warning_log['no_4DN_authors_found'], fp, indent=3)

    with open("warning_logs/4DN_authors_no_matching_awards.json", 'w') as fp:
        json.dump(warning_log['no_matching_awards'], fp, indent=3)

    print("done!")
    print("There are {} publications.".format(num_pubs))
    print("{} do not have 4DN members as authors. See warning logs".format(num_no_4DN_author_found))
    print("{} do not have matching awards with 4DN members authors. See warning logs".format(num_no_matching_awards))


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
            if record.get("exclude", False) or record.get("primary_awards") is None or record.get("primary_awards") == []:
                continue
            this = {k: record.get(k, None) for k in ('source', 'id', 'title', 'authors', 'date', 'link', 'other_ids', 'primary_awards', 'secondary_awards', '4DN_member_author')}
            for award in record['primary_awards'] or award in record:
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
    reads data_grant/<grant>.json and output data_tables/<grant>.json which includes
    publications in which the grant is the primary award and secondary award.
    '''
    pubs_when_grant_primary = collections.defaultdict(dict)
    pubs_when_grant_secondary = collections.defaultdict(dict)
    per_grant_tables = collections.defaultdict(dict)

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

    for grant in pubs_when_grant_primary.keys():
        per_grant_tables[grant]["id"] = grant
        per_grant_tables[grant]["pubs_award_primary"] = pubs_when_grant_primary[grant]
    for grant in pubs_when_grant_secondary.keys():
        per_grant_tables[grant]["pubs_award_secondary"] = pubs_when_grant_secondary[grant]

    os.makedirs("data_tables", exist_ok=True)
    for grant in per_grant_tables.keys():
        fname_pergrant = "data_tables/" + grant + ".json"

        with open(fname_pergrant, "w") as fp:
            json.dump(per_grant_tables[grant], fp, indent=2)

    print('done!')


def write_to_google_sheets():
    '''Writes data_tables/<grant>.json to google sheets'''

    # Accesses to the google sheets
    s3 = boto3.resource('s3')
    obj = s3.Object('elasticbeanstalk-fourfront-webprod-system', 'DCICjupgoogle.json')
    cont = obj.get()['Body'].read().decode()
    key_dict = json.loads(cont)
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPES)
    gc = gspread.authorize(creds)

    # Read the google management sheet
    with open('google_sheet_link.txt', 'r') as f:
        link = f.readlines()[0].split('\n')
    google_management_sheet_id = link[0]

    google_management_spreadsheet = gc.open_by_key(google_management_sheet_id)
    google_management_sheet = google_management_spreadsheet.sheet1
    grant_sheets_links = google_management_sheet.get_all_values()
    grant_google_sheets_dic = {item[1]: item[2] for item in grant_sheets_links}

    fname_grant_jsons = os.listdir("data_tables")
    for fname_json in fname_grant_jsons:
        grant = fname_json[:-5]
        # if grant != 'DK107965':  # Test an individual grant
        #     continue
        if grant == 'HL130007':  # ignore this for now
            continue
        with open("data_tables/" + fname_json, "r") as fp:
            dic_grant_table = json.load(fp)

        gs_write = []

        if grant == "DA040612" or grant == "HL130007":  # Guttman lab, both awards in one sheet
            spreadsheet_id = grant_google_sheets_dic["DA040612,HL130007"]
        else:
            spreadsheet_id = grant_google_sheets_dic[grant]

        spreadsheet = gc.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet('Publication List')
        except:
            worksheet = spreadsheet.add_worksheet(title='Publication List', rows='200', cols='15')
            col_labels = ['Title', 'Authors', 'Date', 'Contact 4DN Author', 'Portal link', 'Primary awards', 'Secondary awards', 'Other ids', 'Publication link', 'Date added']
            for c, label in enumerate(col_labels):
                col = c + 1
                gs_write.append(gspread.models.Cell(1,col, label))

        print('Working on', spreadsheet)
        time.sleep(2)
        # Read the existing google sheet
        worksheet_values = worksheet.get_all_values()
        if worksheet_values[0][0] == 'Title':
            df = pd.DataFrame(worksheet_values[1:], columns=worksheet_values[0])
        else:
            df = pd.DataFrame(worksheet_values[2:], columns=worksheet_values[1])
        col_names = df.columns.values
        google_sheet = df.to_dict(orient='records', into=OrderedDict)
        # Comparing prublications with grant as primary award with existing google sheets and adding new publications
        new_sheet_existing_primary = []
        new_sheet_new_primary = []
        for a_row in dic_grant_table["pubs_award_primary"].values():
            # check excel
            existing_item = [i for i in google_sheet if a_row['title'] == i['Title']]
            if existing_item:
                new_sheet_existing_primary.append(existing_item)
            else:
                new_sheet_new_primary.append(a_row)

        # Comparing publications with grant as secondary award with existing google sheets and adding new publications
        new_sheet_existing_secondary = []
        new_sheet_new_secondary = []
        for a_row in dic_grant_table["pubs_award_secondary"].values():
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
            existing_item_1 = [i for i in dic_grant_table["pubs_award_primary"].values() if a_row['Title'] == i['title']]
            existing_item_2 = [i for i in dic_grant_table["pubs_award_secondary"].values() if a_row['Title'] == i['title']]
            if existing_item_1 or existing_item_2:
                continue
            else:
                gs_only_items.append(a_row)

        # Writting info to the google sheets
        # Setting labels and formatting
        col_order = {'title': 1, 'authors': 2, 'date': 3, 'contact_4DN_author': 4, 'primary_awards': len(col_names) - 4, 'secondary_awards': len(col_names) - 3, 'other_ids': len(col_names) - 2, 'link': len(col_names) - 1, 'date_added': len(col_names)}
        num_to_letter_dict = {x: y for x, y in zip(range(1, 27), string.ascii_uppercase)}
        fmt_bold = cellFormat(textFormat=textFormat(bold=True))
        fmt_plain = cellFormat(textFormat=textFormat(bold=False))

        # Writting warning header
        row = 1
        col = 1
        message = ['WARNING: This publication list below is provided for reference to identify data sets generated by the network. It was collected automatically from the Biorvix 4DN channel and PubMed. The information may not be accurate or up to date.','','','','','','','','','','','','','','']
        for i in message:
            gs_write.append(gspread.models.Cell(row, col, i))
            col = col + 1

        # Writting the existing info
        for r, line in enumerate(new_sheet_existing_primary):
            row = r + 1
            if row == 1:  # Writes the labels of the columns (the keys)
                for c, key in enumerate(line[0]):
                    row = 2
                    col = c + 1
                    gs_write.append(gspread.models.Cell(row, col, key))
            row = r + 3
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

        # Writting message to separate primary and secondary publications
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
        worksheet.update_cells(gs_write,value_input_option='USER_ENTERED')

        # Adding formatting to worksheet
        num_rows = len(gs_write)//len(df.columns)

        range_worksheet_formatting = [('A1:{}{}'.format(num_to_letter_dict[len(df.columns)],num_rows),fmt_plain)]
        range_title_formatting = [('A1:{}2'.format(num_to_letter_dict[len(df.columns)]), fmt_bold)]

        format_cell_ranges(worksheet, range_worksheet_formatting)  # Clean the formatting of worksheet
        format_cell_ranges(worksheet, format_range_message)  # Make the subtitle bold
        format_cell_ranges(worksheet, range_title_formatting)  # Make the title bold

        print('{} new primary publications and {} new secondary publications'.format(len(new_sheet_new_primary), len(new_sheet_new_secondary)))
        time.sleep(2)
    print('done!')


@click.command()
@click.option('--match-pubs', is_flag=True, help="look for records with very similar titles and authors to fill other_ids field", default=False)
@click.option('--find-awards', is_flag=True, help='find primary and secondary awards for each pub')
@click.option('--per-grant', is_flag=True, help="reorganize data to be per award under data_grant/", default=False)
@click.option('--out-tables', is_flag=True, help="reorganize data for each award to be written to google sheets under data_tables/", default=False)
@click.option('--write-sheets', is_flag=True, help="writes data to google sheets", default=False)
def run(match_pubs, find_awards, per_grant, out_tables, write_sheets):
    if(match_pubs):
        match_pubs_titleauthor()
    if(find_awards):
        find_primary_and_secondary_awards()
    if(per_grant):
        write_per_grant()
    if(out_tables):
        write_tables_per_grant()
    if(write_sheets):
        write_to_google_sheets()


if __name__ == '__main__':
    run()
