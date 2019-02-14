import collections
import os, sys, json, re
import click
from difflib import SequenceMatcher as SM
from itertools import combinations 
from datetime import date 

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
        with open("data_post/" + fname_jsoneach,"r") as fp:
            entry = json.load(fp)
            version = entry["latest"]
            name=version["source"] + ":" + version["id"]
            entries[name]=version
            entries[name]["other_ids"] = []
            entries[name]["fname"] = fname_jsoneach

    for (n1,t1), (n2,t2) in combinations(entries.items(),2):
        score1 = SM(None, t1["title"], t2["title"]).ratio()
        score2 = SM(None, " ".join(t1["authors"]), " ".join(t2["authors"])).ratio()
        if score1 > 0.75 or (score1>0.6 and score2>0.9):
            entries[n1]["other_ids"].append(n2)
            entries[n2]["other_ids"].append(n1)
            if(t1["source"]=="biorxiv" and t2["source"]=="PubMed"):
                entries[n1]["exclude"]=True
            elif(t2["source"]=="biorxiv" and t1["source"]=="PubMed"):
                entries[n2]["exclude"]=True
            elif(t1["source"]=="PubMed" and t2["source"]=="PubMed"):
                if(t1["id"] < t2["id"]):
                    entries[n2]["exclude"]=True
                else:
                    entries[n1]["exclude"]=True
            else:
                raise Exception("two biorxivs matched",t1["title"], t2["title"])

    for entry in entries:
        fname_jsoneach = entry.pop("fname")
        with open("data_post/" + fname_jsoneach,"r") as fp:
            entry_orig = json.load(fp)
            entry_orig["latest"] = entry
        with open(c + fname_jsoneach,"w") as fp:
            json.dump(entry_orig, fp)
    print("done!")

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
        with open("data_post/" + fname_jsoneach,"r") as fp:
            entry = json.load(fp)
            record = entry["latest"]
            if record.get("exclude",False) or record.get("awards") is None:
                continue
            this = {k: record.get(k, None) for k in ('source', 'id', 'title', 'authors','other_ids')}
            for award in record["awards"]:
                dict_pergrant[award][this["id"]] = this

    os.makedirs("data_grant",exist_ok=True)  
    for award in dict_pergrant.keys():
        fname_pergrant = "data_grant/" + award + ".json"
        if os.path.exists(fname_pergrant):
            with open(fname_pergrant,"r") as fp:
                entry = json.load(fp)
        else:
            entry = {}        

        today = str(date.today())

        for id in dict_pergrant[award].keys():
            added = today
            if id in entry.keys():
                added = entry[id]["added"]
            dict_pergrant[award][id]["added"] = added

        with open(fname_pergrant,"w") as fp:
            json.dump(dict_pergrant[award],fp,indent=2)


@click.command()
@click.option('--match-pubs', is_flag=True, help="look for records with very similar titles and authors to fill other_ids field", default=False)
@click.option('--per-grant', is_flag=True, help="reorganize data to be per award under data_grant/", default=False)

def run(match_pubs,per_grant):
    if(match_pubs):
        match_pubs_titleauthor()
    if(per_grant):
        write_per_grant()  

if __name__ == '__main__':
    run()
