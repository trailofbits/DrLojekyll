#!/usr/bin/env python3.7
from json import dumps, dump, load, loads
from collections import defaultdict
from exrex import getone

def pretty_print(gdict):
    for k,v in gdict.items():
        print(f"{k} = {v}")

def transform(x):
    x = x.strip()
    if len(x) >= 1:
        if x[0] == "r":
            x = getone(x).strip("r")
        elif x[0] != '"':
            x = f"<{x}>"
    return x

def parse_grammar():
    with open("grammar.txt", "r") as grammar:
        gdict = defaultdict(list)
        for l in grammar:
            label, gramm = l.split(": ")
            lexed = map(lambda x: transform(x), gramm.split(" "))
            lexed = (list(lexed))
            gdict[f"<{label}>"].append(lexed)
    gdict["<start>"] = [["<module>"]]
    return gdict

def serialize_grammar(gdict):
    with open("grammar.json", "w") as jg:
        js = dumps(gdict, indent=1)
        jg.write(js)
        
if __name__ == "__main__":
    gdict = parse_grammar()
    #pretty_print(gdict)
    serialize_grammar(gdict)

