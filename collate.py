import collections
import os, sys, json, re
import click
from difflib import SequenceMatcher as SM
from itertools import combinations 

def match_pubs_titleauthor():
    # look for records with very similar titles and author lists
    # if biorxiv and pubmed match, mark biorxiv with exclude flag
    # if two pubmeds match, mark latter with exclude flag. (typically erratums)
    # add information on records in now_published, preprint, correction fields.

    entries = {}
    fname_jsons = os.listdir("data_post")
    for fname_jsoneach in fname_jsons:
        with open("data_post/" + fname_jsoneach,"r") as fp:
            entry = json.load(fp)
            version = entry["latest"]
            name=version["source"] + ":" + version["id"]
            entries[name]=version
            entries[name]["now_published"] = []
            entries[name]["preprint"] = []
            entries[name]["correction"] = []
            entries[name]["fname"] = fname_jsoneach

    for (n1,t1), (n2,t2) in combinations(entries.items(),2):
        score1 = SM(None, t1["title"], t2["title"]).ratio()
        score2 = SM(None, " ".join(t1["authors"]), " ".join(t2["authors"])).ratio()
        if score1 > 0.75 or (score1>0.6 and score2>0.9):
            if(t1["source"]=="biorxiv" & t2["source"]=="PubMed"):
                entries[n1]["now_published"].append(n2)
                entries[n2]["preprint"].append(n1)
                entries[n1]["exclude"]=True
            elif(t2["source"]=="biorxiv" & t1["source"]=="PubMed"):
                entries[n2]["now_published"].append(n1)
                entries[n1]["preprint"].append(n2)
                entries[n2]["exclude"]=True
            elif(t1["source"]=="PubMed" & t2["source"]=="PubMed"):
                entries[n2]["correction"].append(n1)
                entries[n1]["correction"].append(n2)
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
        with open("data_post/" + fname_jsoneach,"w") as fp:
            json.dump(entry_orig, fp)

match_pubs_titleauthor()
        
# organize data per grant

# per grant
# read existing file
# update records with new and changed publications



# turn per-publication entries to per-grant entries
# under construction

def grant_group():
    fname_jsons = os.listdir("data_post")

    out = collections.defaultdict(list)

    for fname_jsoneach in fname_jsons:
        print(fname_jsoneach)
        with open("data_post/" + fname_jsoneach,"r") as fp:
            entry = json.load(fp)
            version = entry["latest"]
            title = version["title"]
            version["id"] = version.get("pmid",version.get("version"))
            this = {k: version.get(k, None) for k in ('id', 'title', 'authors')}
            for award in version["awards"]:
                out[award].append(this)



