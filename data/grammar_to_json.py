#!/usr/bin/env python3.7
from json import dumps
from collections import defaultdict

def pretty_print(gdict):
    for k,v in gdict.items():
        print(f"{k} = {v}")

def transform(x):
    x = x.strip()
    if len(x) >= 1:
        if x[0] != '"':
            x = f"<{x}>"
    return x

def parse_grammar():
    with open("grammar.txt", "r") as grammar:
        gdict = defaultdict(list)
        for l in grammar:
            label, gramm = l.split(": ")
            lexed = map(lambda x: transform(x), gramm.split(" "))
            lexed = [(list(lexed)), ]
            gdict[f"<{label}>"].append(lexed)
    return gdict

def serialize_grammar(gdict):
    with open("grammar.json", "w") as jg:
        js = dumps(gdict, indent=1)
        jg.write(js)
        
if __name__ == "__main__":
    gdict = parse_grammar()
    serialize_grammar(gdict)
