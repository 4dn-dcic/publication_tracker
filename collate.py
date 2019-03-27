import collections
import os
import json
import click
from difflib import SequenceMatcher as SM
from itertools import combinations
from datetime import date
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time


def match_pubs_titleauthor():
    '''
    look for records with very similar titles and authors to fill "other_ids" field
    if biorxiv and pubmed match, mark biorxiv with exclude flag
    if two pubmeds match, mark latter with exclude flag. (typically erratums)
    '''
    print("matching publications by titles and authors, this make take some time")

    def get_primary_and_secondary_awards(publication, warning1_log, warning2_log):
        '''
        Find the primary and secondary awards of the publication if the publication has more than 1 award.
        Primary awards: awards from 4DN members authors in the publication
        Secondary awards: other awards

        if there are not 4DN member authors in the publication, the function gives a warning
        if there are 4DN member authors in the publication, but their awards are not in this publication, it gives a warning.
        '''

        def is_same_author(author1, author2_first, author2_last):
            '''
            Determines if the author in the publication (author1) is the same as the 4DN member
            '''

            if author2_last == 'Qi':
                if 'Lei' in author1[:len(author2_first)] and 'Qi' in author1[-len(author2_last):]:
                    return True
                else:
                    return False

            elif author2_last == 'Hu' or author2_last == 'Li' or author2_last == 'Chen':
                score1 = SM(None, author1[:len(author2_first)].lower(), author2_first.lower()).ratio()
                score2 = SM(None, author1[-len(author2_last):].lower(), author2_last.lower()).ratio()
                if score1 > 0.95 and score2 > 0.95:
                    return True
                else:
                    return False
            else:
                author2_fullname = author2_first + ' ' + author2_last

                score1 = SM(None, author1.lower(), author2_fullname.lower()).ratio()

                score2 = SM(None, author1[-len(author2_last):].lower(), author2_last.lower()).ratio()

                if score1 > 0.85 or (score1 > 0.65 and score2 > 0.90):
                    return True
                else:
                    return False

        primary_award = []
        secondary_award = []
        authors_4DN_in_pub = []  # list of all 4DN authors in pub for warning logs when no matching awards
        author_4DN_in_pub = ' '  # This is the author that was matched
        isthere_primary_award = False   # Does the publication has primary award?
        isthere_4DN_member = False  # is there a 4DN member in the publication?
        warn1 = False
        warn2 = False

        # Get list of Pis and their awards in the Data Portal
        wdir = os.getcwd()
        with open(wdir + "/Pis_and_awards_updated.json", 'r') as f:
            pi_and_awards_portal = json.load(f)

        authors_in_pub = publication['authors'][:]
        awards_in_pub = publication.get("awards", ["NONE"])

        # Find the main 4DN author in the publication
        # check if the award of the 4DN member in the portal matches the awards in the publication
        # that becomes the primary award.
        # If the publication does not have an award, give the award of the last 4DN author in the publication
        # If the award is the OH award get the TCPA award from the member if applies

        for last_author in authors_in_pub.reverse():
            for pi in pi_and_awards_portal.values():
                ismatch = False  # Do the award of this 4DN member matches the award in the publication?
                pi_fname = pi['first_name']
                pi_lname = pi['last_name']
                pi_awards = pi['awards']

                if is_same_author(last_author, pi_fname, pi_lname):  # find the 4DN member in the list of authors in the publication
                    isthere_4DN_member = True
                    ismatch = True
                    authors_4DN_in_pub.append(pi)

                    # if publication does not have award, assign 4DN author's award
                    if len(awards_in_pub) == 1 and awards_in_pub[0] == 'NONE' and isthere_4DN_member:
                        for awrd in pi_awards:
                            if 'TCPA' in awrd:
                                primary_award.append(awrd)
                            else:
                                primary_award.append(awrd[1:-3])
                        isthere_primary_award = True
                        author_4DN_in_pub = pi_fname + ' ' + pi_lname
                        break

                    # If the publication has one award, that is the primary award
                    # If the award is the OH award, check for TCPA award and assign it
                    elif len(awards_in_pub) == 1:
                        if awards_in_pub[0] == 'U01CA200147' and 'TCPA' in pi_awards[0]:
                            primary_award.append(pi_awards[0])
                        else:
                            primary_award.append(awards_in_pub[0])
                        author_4DN_in_pub = pi_fname + ' ' + pi_lname
                        isthere_primary_award = True
                        break

                    # If the publication has more than one award, find the primary and secondary awards
                    elif len(awards_in_pub) > 1:
                        for award in awards_in_pub:
                            if len(pi_awards) > 1:  # if the 4DN member has more than 1 award, check all the awards
                                awrd_ct = 0
                                for awrd in pi_awards:
                                    awrd_ct = awrd_ct + 1
                                    if award in awrd:
                                        primary_award.append(award)
                                        author_4DN_in_pub = pi_fname + ' ' + pi_lname
                                        isthere_primary_award = True
                                        break
                                    elif 'U01CA200147' == award and 'TCPA' in awrd:
                                        primary_award.append(awrd)
                                        author_4DN_in_pub = pi_fname + ' ' + pi_lname
                                        isthere_primary_award = True
                                        break
                                    else:
                                        if awrd_ct == len(pi_awards):
                                            secondary_award.append(award)
                            else:
                                if award in pi_awards[0]:
                                    isthere_primary_award = True
                                    primary_award.append(award)
                                    author_4DN_in_pub = pi_fname + ' ' + pi_lname
                                elif 'U01CA200147' == award and 'TCPA' in pi_awards[0]:
                                    primary_award.append(pi_awards[0])
                                    isthere_primary_award = True
                                    pi_fname + ' ' + pi_lname
                                else:
                                    secondary_award.append(award)

                if ismatch is True:  # If found a match in author, stop searching in the 4DN member list
                    break
            if isthere_4DN_member is True:  # if found a match in author, stop searching the Authors in pub list
                break

        # If there are not 4DN members in this publication, give a warning and awards in pub becomes primary
        if isthere_4DN_member is False:
            PIs_4DN = []
            for award in awards_in_pub:
                for pi in pi_and_awards_portal.values():
                    pi_fullname = pi['first_name'] + ' ' + pi['last_name']
                    pi_awards = pi['awards']
                    for awrd in pi['awards']:
                        if award in awrd:
                            PIs_4DN.append(pi_fullname)
                primary_award.append(award)
            warning1_log['no_4DN_authors'][publication["fname"]] = {'awards': publication['awards'], 'authors': publication['authors'], '4DN_PIs': PIs_4DN}
            warn1 = True

        # if there are 4DN members, but their awards do not match with the awards in pub, give warning
        # All the awards in pub become primary awards
        if isthere_4DN_member is True and isthere_primary_award is False:
            for awrd in secondary_award:
                primary_award.append(awrd)
            secondary_award = []
            warning2_log['no_matching_awards'][publication["fname"]] = {'awards in pub': awards_in_pub, '4DN_authors': [{'name': pi['first_name'] + ' ' + pi['last_name'], 'awards':pi['awards']} for pi in authors_4DN_in_pub]}
            warn2 = True

        return primary_award, secondary_award, warn1, warn2, author_4DN_in_pub

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

    # Combines the same grants from Biorxiv with Pubmed into a sigle one
    all_grants = list(dict_pergrant.keys())
    for award in all_grants:
        if len(award) == 8:  # Biorxiv awards are 8 character long
            for key in all_grants:
                if len(key) > 8:  # Pubmed awards are longer than 8 characters
                    if award in key:
                        for pub in dict_pergrant[award].values():
                            dict_pergrant[key][pub['id']] = dict_pergrant[award][pub['id']]
                        dict_pergrant.pop(award)

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
    read data_grant/<grant>.json and output data_grant/<grant> table to google sheets
    '''

    pubs_grant_primary = collections.defaultdict(dict)
    pubs_grant_secondary = collections.defaultdict(dict)
    pubs_grant_secondary_spreadsheet = collections.defaultdict(dict)
    fname_grant_jsons = os.listdir("data_grant")

    for fname_json in fname_grant_jsons:
        with open("data_grant/" + fname_json, "r") as fp:
            dic_pergrant = json.load(fp)
            pubs = dic_pergrant.keys()
            grant_name = fname_json[:-5]
            for pub in pubs:
                authors = {pub: "; ".join(dic_pergrant[pub]["authors"]) for pub in pubs}
                title = {pub: dic_pergrant[pub]["title"] for pub in pubs}
                link = {pub: dic_pergrant[pub]["link"] for pub in pubs}
                date = {pub: dic_pergrant[pub]["date"] for pub in pubs}
                other_ids = {pub: ";".join(dic_pergrant[pub]["other_ids"]) for pub in pubs}
                primary_awards = {pub: ";".join(dic_pergrant[pub]["primary_awards"]) if len(dic_pergrant[pub]["primary_awards"]) > 1 else " -- " for pub in pubs}
                secondary_awards = {pub: ";".join(dic_pergrant[pub]["secondary_awards"]) for pub in pubs}
                contact_4DN_author = {pub: dic_pergrant[pub]["4DN_member_author"] for pub in pubs}
                added_date = {pub: dic_pergrant[pub]["added"] for pub in pubs}

        pubs_grant_primary[grant_name]['title'] = [title[pub] for pub in title]
        pubs_grant_primary[grant_name]['authors'] = [authors[pub] for pub in authors]
        pubs_grant_primary[grant_name]['date'] = [date[pub] for pub in date]
        pubs_grant_primary[grant_name]['link'] = [link[pub] for pub in link]
        pubs_grant_primary[grant_name]['other_ids'] = [other_ids[pub] for pub in other_ids]
        pubs_grant_primary[grant_name]['primary_awards'] = [primary_awards[pub] for pub in primary_awards]
        pubs_grant_primary[grant_name]['secondary_awards'] = [secondary_awards[pub] for pub in secondary_awards]
        pubs_grant_primary[grant_name]['date_added'] = [added_date[pub] for pub in added_date]
        pubs_grant_primary[grant_name]['contact_4DN_author'] = [contact_4DN_author[pub] for pub in contact_4DN_author]

        # Keeps track of the publications when awards are secondary awards

        for key in secondary_awards:
            if secondary_awards[key] != '':
                if ';' not in secondary_awards[key]:
                    pubs_grant_secondary[secondary_awards[key]][key] = dic_pergrant[key]
                else:
                    sec_awards_list = secondary_awards[key].split(';')
                    for sec_award in sec_awards_list:
                        pubs_grant_secondary[sec_award][key] = dic_pergrant[key]

    for grant in pubs_grant_secondary:
        pubs_grant_secondary_spreadsheet[grant]['title'] = [pubs_grant_secondary[grant][pub]["title"] for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['authors'] = ["; ".join(pubs_grant_secondary[grant][pub]["authors"]) for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['date'] = [pubs_grant_secondary[grant][pub]["date"] for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['link'] = [pubs_grant_secondary[grant][pub]["link"] for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['other_ids'] = [";".join(pubs_grant_secondary[grant][pub]["other_ids"]) for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['primary_awards'] = [";".join(pubs_grant_secondary[grant][pub]["primary_awards"]) for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['secondary_awards'] = [";".join(pubs_grant_secondary[grant][pub]["secondary_awards"]) if len(pubs_grant_secondary[grant]) > 1 else "--" for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]["added_date"] = [pubs_grant_secondary[grant][pub]["added"] for pub in pubs_grant_secondary[grant]]
        pubs_grant_secondary_spreadsheet[grant]['contact_4DN_author'] = [pubs_grant_secondary[grant][pub]["4DN_member_author"] for pub in pubs_grant_secondary[grant]]

    # Accesses to the google sheets
    s3 = boto3.resource('s3')
    obj = s3.Object('elasticbeanstalk-fourfront-webprod-system', 'DCICjupgoogle.json')
    cont = obj.get()['Body'].read().decode()
    key_dict = json.loads(cont)
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPES)
    gc = gspread.authorize(creds)

    # Reads the link of google management sheet
    f = open('google_sheet_link.txt', 'r')
    link = f.readline().split('\n')
    google_management_sheet_id = link[0]
    f.close()

    # Opens the google management sheet and get all the values
    google_management_spreadsheet = gc.open_by_key(google_management_sheet_id)
    google_management_sheet = google_management_spreadsheet.sheet1
    grant_sheets_links = google_management_sheet.get_all_values()

    col_labels = ['Title', 'Authors', 'Date', 'Link', 'Other ids', 'Primary_awards', 'Secondary Awards', 'Date added', 'Contact 4DN Author']
    reqst_ct = 1
    row = 2
    col = 1

    # Writes data to the google sheets
    for grant in pubs_grant_primary:
        gs_write = []
        for item in grant_sheets_links:
            if grant in item[1]:
                spreadsheet_id = item[2]
                if reqst_ct >= 90:
                    time.sleep(100)
                    reqst_ct = 0
                    spreadsheet = gc.open_by_key(spreadsheet_id)
                    print('Working on ', spreadsheet)
                    reqst_ct += 1
                else:
                    spreadsheet = gc.open_by_key(spreadsheet_id)
                    print('Working on ', spreadsheet)
                    reqst_ct += 1

                if reqst_ct >= 90:
                    time.sleep(100)
                    reqst_ct = 0
                    try:
                        worksheet = spreadsheet.worksheet('Publication List')
                        reqst_ct += 1
                    except:
                        worksheet = spreadsheet.add_worksheet(title='Publication List', rows='200', cols='15')
                        reqst_ct += 1
                else:
                    try:
                        worksheet = spreadsheet.worksheet('Publication List')
                        reqst_ct += 1

                    except:
                        worksheet = spreadsheet.add_worksheet(title='Publication List', rows='200', cols='15')
                        reqst_ct += 1
                col = 1
                row = 1
                for label in col_labels:
                    gs_write.append(gspread.models.Cell(1, col, label))
                    col += 1

                col = 1
                for item in pubs_grant_primary[grant].values():
                    row = 2
                    for info in item:
                        gs_write.append(gspread.models.Cell(row, col, info))
                        row += 1
                    col += 1

                row += 2
                new_row = row
                col = 1

                if grant in pubs_grant_secondary_spreadsheet.keys():
                    for item in pubs_grant_secondary_spreadsheet[grant].values():
                        new_row = row
                        for info in item:
                            gs_write.append(gspread.models.Cell(new_row, col, info))
                            new_row += 1
                        col += 1

                #worksheet.update_cells(gs_write)
                reqst_ct += 1


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
