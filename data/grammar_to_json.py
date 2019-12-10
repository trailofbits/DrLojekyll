#!/usr/bin/env python3.7
from json import dumps, dump, load, loads
from collections import defaultdict
from exrex import getone

def pretty_print(gdict):
    for k,v in gdict.items():
        print(f"{k} = {v}")

def add_spaces(ls):
    rv = []
    if len(ls) > 1:
        for i in ls:
            if i == "\\n":
                i = "\n"
            rv.append(i)
            rv.append(" ")
        return rv
    return ls

def transform(x):
    x = x.strip("\n")
    if len(x) >= 1:
        if x[0] == "r":
            x = [getone(x).strip("r") for i in range(15)]
        elif x[0] != '"':
            x = f'<{x}>'
    if isinstance(x, str):
        x = x.strip('"')
    else:
        x = [i.strip('"') for i in x]
    return x


def parse_grammar():
    with open("grammar.txt", "r") as grammar:
        gdict = defaultdict(list)
        for l in grammar:
            label, gramm = l.split(": ")
            toks = gramm.split(" ")
            new_toks = []
            regex_flag = False
            for t in toks:
                trans = transform(t)
                if isinstance(trans, list):
                    new_toks.extend([i for i in trans])
                    regex_flag = True
                else:
                    new_toks.append(trans)
            if regex_flag:
                for tok in new_toks:
                    gdict[f"<{label}>"].append([tok])
            else:
                gdict[f"<{label}>"].append(add_spaces(new_toks))
    gdict["<start>"] = [["<module>"]]
    return gdict

def serialize_grammar(gdict):
    with open("grammar.json", "w") as jg:
        js = dumps(gdict, indent=1)
        jg.write(js)
        
if __name__ == "__main__":
    gdict = parse_grammar()
    serialize_grammar(gdict)

