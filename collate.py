import collections
import os, sys, json, re
import click

# turn per-publication entries to per-grant entries
# under construction

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
        
