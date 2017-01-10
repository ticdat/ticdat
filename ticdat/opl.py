from ticdat.utils import verify, containerish, stringish
import os
from collections import defaultdict

def opl_run(mod_file, input_tdf, input_dat, soln_tdf):
    """
    solve an optimization problem using an OPL .mod file
    :param mod_file: An OPL .mod file.
    :param input_tdf: A TicDatFactory defining the input schema
    :param input_dat: A TicDat object consistent with input_tdf
    :param soln_tdf: A TicDatFactory defining the solution schema
    :return: a TicDat object consistent with soln_tdf, or None if no solution found
    """
    verify(os.path.isfile(mod_file), "mod_file %s is not a valid file."%mod_file)
    msg  = []
    verify(input_tdf.good_tic_dat_object(input_dat, msg.append),
           "tic_dat not a good object for the input_tdf factory : %s"%"\n".join(msg))
    verify(False, "!!!!Under Construction!!!!")

def create_opl_text(tdf, tic_dat):
    msg = []
    verify(tdf.good_tic_dat_object(tic_dat, msg.append),
           "tic_dat not a good object for this factory : %s"%"\n".join(msg))
    verify(not tdf.generator_tables, "doesn't work with generator tables.")
    verify(not tdf.generic_tables, "doesn't work with generic tables. (not yet - will add ASAP as needed) ")
    dict_with_lists = defaultdict(list)
    dict_tables = {t for t,pk in tdf.primary_key_fields.items() if pk}
    for t in dict_tables:
        for k,r in getattr(tic_dat, t).items():
            row = list(k) if containerish(k) else [k]
            for f in tdf.data_fields.get(t, []):
                row.append(r[f])
            dict_with_lists[t].append(row)
    for t in set(tdf.all_tables).difference(dict_tables):
        for r in getattr(tic_dat, t):
            row = [r[f] for f in tdf.data_fields[t]]
            dict_with_lists[t].append(row)

    rtn = ""
    for i, (t,l) in enumerate(dict_with_lists.items()):
        rtn += "\n" if i > 0 else ""
        rtn += "%s = {\n"%t
        for r in l:
            rtn += "<"
            for i,v in enumerate(r):
                rtn += ('"%s"'%v if stringish(v) else str(v)) + (", " if i < len(r)-1 else "")
            rtn += ">\n"
        rtn += "};\n"

    return rtn

def read_opl_text(text):
    verify(stringish(text), "text needs to be a string")
    dict_with_lists = defaultdict(list)
    mode = 'none'
    field = ''
    tbn = ''
    row = []
    '''
    First I'm writing it without error handling, then I'll add all of that.
    THINGS IDK HOW TO HANDLE YET QUOTES IN QUOTES(probably want them escaped) EMPTY FIELDS(skip) INFINITY
    '''
    for i,c in enumerate(text):
        if mode != 'inquotes' and c.isspace():
            continue

        elif mode is 'inquotes':
            if c is '"':
                row.append(field)
                field = ''
                mode = 'isrow'
            else:
                field += c

        elif c is '=':
            mode = 'intable'

        elif c is '{':
            continue

        elif c is '<':
            mode = 'isrow'

        elif c is ',':
            if mode is 'isnumber':
                row.append(float(field))
                field = ''
                mode = 'isrow'

        elif c is '"':
            if mode is 'isrow':
                mode = 'inquotes'

        elif c is '}':
            row = []
            tbn = ''
            mode = 'none'

        elif c is '>':
            if mode is 'isnumber':
                row.append(field)
                field = ''
            dict_with_lists[tbn].append(row)
            row = []
            mode = 'intable'

        elif c is ';':
            continue

        else:
            # 'either a name of a table or a number'
            if mode is 'none':
                tbn += c
            else:
                mode = 'isnumber'
                field += c

    return dict_with_lists