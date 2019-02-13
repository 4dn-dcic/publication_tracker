"""
Get a list of awards for 4DN publications as identified by nihreporter

1. Download list of 4DN publications from nihreporter:

On https://projectreporter.nih.gov/
- Select FY to include 2015 through Active.
- Enter the 4DN Project Numbers: U01CA200060, U01CA200147, U01DA040582, U01DA040583, U01DA040588, U01DA040601, U01DA040612, U01DA040709, U01EB021223, U01EB021230, U01EB021232, U01EB021236, U01EB021237, U01EB021238, U01EB021239, U01EB021240, U01EB021247, U01HL129958, U01HL129971, U01HL129998, U01HL130007, U01HL130010, U54DK107979, U54DK107980, U54DK107965, U54DK107967, U54DK107977, U54DK107981, U01CA200059
- Go to publications tab and export to csv
- You should end up with a file similar to data_pre/Publ_08Feb2019_090308_30431073.csv.

2. Run this script:

python3 parse_nihreport_4dn.py --infname data_pre/Publ_08Feb2019_090308_30431073.csv`

This will create files data_post/PMID<pmid>.json with {latest={pmid, title, awards, authors}}

3. Repeat.

If new records appear in pubmed, new files will be created for them.
If old records change, old files will be updated with backups of old information.
    {latest={pmid, title, awards, authors},OldDate={pmid,...]}

"""
import pandas
import unicodedata 
import re, json
import os, glob
import click

@click.command()
@click.option('--infname', default='data_pre/Publ_08Feb2019_090308_30431073.csv', help='code assumes this looks like <folder>/<something>_<date>_<sth_sth>.<extension>')
def parse_nihreport(infname):
    # assume infname = folder/something_date_sth_sth.
    version = re.match(".*/.*?_(.*)\..*",infname).group(1)
    version_short = re.match("(.*?)_",version).group(1)
    publist = pandas.read_csv(infname , encoding='latin-1')   
    publist.rename(columns={'Title (Link to full-text in PubMed Central) ': "Title"}, inplace=True)
    
    # the same publication may appear multiple times for different awards.
    # We'll group the results by publication
    pmids = set(publist['PMID'])    

    for i in publist.index:
        title=publist.loc[i,"Title"]
        if re.search("<a",title):
            publist.loc[i,"Title"] = re.search(">(.*)<",title).group(1)

    for pmid in pmids:
        # all records per same publication
        records = publist[publist["PMID"]==pmid]

        # "Rao, Suhas S P" -> "Suhas S P Rao"
        authors = records["Authors "].iloc[0]
        authors = unicodedata.normalize("NFKD",authors)
        authors= authors.split("; ")
        authors = [" ".join(i.split(", ")[::-1]) for i in authors]

        pmid=str(pmid)
        awards = list(records["Core Project Number"])
        link = "http://www.ncbi.nlm.nih.gov/pubmed/" + pmid
        date = (records["PUB Date"].iloc[0])[:-1]
        
        thisversion = {
            "source" : "PubMed",
            "id" : pmid,
            "title": records["Title"].iloc[0],
            "link": link,
            "version" : version_short,
            "date": date,
            "authors" : authors,
            "awards" : awards
            }

        #if a json for this pmid does not exist write one
        #if a json does exist, check if anything is different
        fname_jsoneach = "data_post/PubMed-" + pmid + ".json"
        pre_exists = False
        updated = False
        entry={}
        if os.path.exists(fname_jsoneach):
            pre_exists = True
            with open(fname_jsoneach,"r") as fp:
                entry = json.load(fp)
                for key in thisversion.keys():
                    if thisversion[key] != entry["latest"].get(key,""):
                        entry[entry["latest"]["version"]] = entry["latest"]
                        updated = True
                        break
                #if the code comes here, the file is unchanged. return.
                if not updated:
                    print(fname_jsoneach + ": record already exists, unchanged")
                    continue

        entry["latest"] = thisversion
        with open(fname_jsoneach,"w") as fp:
            json.dump(entry,fp,indent=2)
        if(pre_exists):
            print(fname_jsoneach + ": something is different with previous record, saved both")
        else:
            print(fname_jsoneach + ": created new record")


if __name__ == '__main__':
    parse_nihreport()
