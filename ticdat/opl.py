from ticdat.utils import verify, containerish, stringish
import os
from collections import defaultdict

INFINITY = 999999


def opl_run(mod_file, input_tdf, input_dat, soln_tdf, infinity=INFINITY):
    """
    solve an optimization problem using an OPL .mod file
    :param mod_file: An OPL .mod file.
    :param input_tdf: A TicDatFactory defining the input schema
    :param input_dat: A TicDat object consistent with input_tdf
    :param soln_tdf: A TicDatFactory defining the solution schema
    :param infinity: A number used to represent infinity in OPL
    :return: a TicDat object consistent with soln_tdf, or None if no solution found
    """
    verify(os.path.isfile(mod_file), "mod_file %s is not a valid file."%mod_file)
    msg  = []
    verify(input_tdf.good_tic_dat_object(input_dat, msg.append),
           "tic_dat not a good object for the input_tdf factory : %s"%"\n".join(msg))
    verify(False, "!!!!Under Construction!!!!")

def create_opl_text(tdf, tic_dat, infinity=INFINITY):
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
        rtn += "%s = {"%t
        if len(l[0]) > 1:
            rtn += "\n"
        for x in range(len(l)):
            r = l[x]
            if len(r) > 1:
                rtn += "<"
            for i,v in enumerate(r):
                rtn += ('"%s"'%v if stringish(v) else (str(infinity) if float('inf') == v else str(v))) + (", " if i < len(r)-1 else "")
            if len(r) == 1 and len(l)-1 != x:
                rtn += ', '
            if len(r) > 1:
                rtn += ">\n"
        rtn += "};\n"

    return rtn

def create_opl_mod_text(tdf):
    msg = []
    verify(not tdf.generator_tables, "doesn't work with generator tables.")
    verify(not tdf.generic_tables, "doesn't work with generic tables. (not yet - will add ASAP as needed) ")
    rtn = ''
    dict_tables = {t for t, pk in tdf.primary_key_fields.items() if pk}
    for t in dict_tables:
        rtn += "tuple " + t + "_type\n{"
        def getType(data_types, table, field):
            try:
                return "float" if data_types[table][field].number_allowed else "string"
            except KeyError:
                return "string"
        for pk in tdf.primary_key_fields[t]:
            rtn += "\n\tkey " + getType(tdf.data_types, t, pk) + " " + pk + ";"
        for df in tdf.data_fields[t]:
            rtn += "\n\t" + getType(tdf.data_types, t, df) + " " + df + ";"
        rtn += "\n};\n\n"
        rtn += "{" + t + "_type} " + t.upper() + "=...;\n\n"
    return rtn

def read_opl_text(tdf,text):
    verify(stringish(text), "text needs to be a string")
    # probably want to verify something about the ticdat factory, look at the wiki
    dict_with_lists = defaultdict(list)
    NONE, TABLE, ROW, ROWSTRING, ROWNUM, FIELD, STRING,  NUMBER = 1, 2, 3, 4, 5, 6, 7, 8
    mode = NONE
    field = ''
    table_name = ''
    row = []

    def to_number(st, pos):
        try:
            return float(st)
        except ValueError:
            verify(False,
                   "Badly formatted string - Field '%s' is not a valid number. Character position [%s]." % (st, pos))

    for i,c in enumerate(text):
        if mode is not STRING and mode is not ROWSTRING and (c.isspace() or c == '{' or c == ';'):
            continue

        elif mode is STRING or mode is ROWSTRING:
            if c == '"':
                if text[i-1] == '\\':
                    field = field[:-1] + '"'
                else:
                    if mode is ROWSTRING:
                        row.append(field)
                        field = ''
                        verify(len(row) == len((dict_with_lists[table_name] or [row])[0]),
                               "Inconsistent row lengths found for table %s" % table_name)
                        dict_with_lists[table_name].append(row)
                        row = []
                        mode = TABLE
                    else:
                        mode = FIELD
            else:
                field += c
        # I can get tricky with these verify's to give some more helpful tips
        elif c == '=':
            verify(mode is NONE, "Badly formatted string, unrecognized '='. Character position [%s]"%i)
            verify(len(table_name) > 0, "Badly formatted string, table name can't be blank. Character position [%s]"%i)
            # is the dup table names check neccessary?
            verify(table_name not in dict_with_lists.keys(), "Can't have duplicate table name. [Character position [%s]"%i)
            dict_with_lists[table_name] = []
            mode = TABLE

        elif c == '<':
            verify(mode is TABLE, "Badly formatted string, unrecognized '<'. Character position [%s]"%i)
            mode = ROW

        elif c == ',':
            verify(mode is ROW or mode is FIELD or mode is NUMBER or mode is ROWNUM or mode is TABLE, "Badly formatted string, unrecognized ','. \
                                                                    Character position [%s]"%i)
            if mode is TABLE:
                continue
            if mode is ROWNUM:
                field = to_number(field,i)
                row.append(field)
                field = ''
                verify(len(row) == len((dict_with_lists[table_name] or [row])[0]),
                       "Inconsistent row lengths found for table %s" % table_name)
                dict_with_lists[table_name].append(row)
                row = []
                mode = TABLE
            else:
                if mode is NUMBER:
                    field = to_number(field,i)
                row.append(field)
                field = ''
                mode = ROW

        elif c == '"':
            verify(mode is ROW or mode is TABLE, "Badly formatted string, unrecognized '\"'. Character position [%s]"%i)
            if mode is ROW:
                mode = STRING
            if mode is TABLE:
                mode = ROWSTRING

        elif c == '}':
            verify(mode is TABLE or mode is ROWNUM, "Badly formatted string, unrecognized '}'. Character position [%s]"%i)
            if mode is ROWNUM:
                field = to_number(field,i)
                row.append(field)
                field = ''
                verify(len(row) == len((dict_with_lists[table_name] or [row])[0]),
                       "Inconsistent row lengths found for table %s" % table_name)
                dict_with_lists[table_name].append(row)
            row = []
            table_name = ''
            mode = NONE

        elif c == '>':
            verify(mode is ROW or mode is FIELD or mode is NUMBER, "Badly formatted string, unrecognized '>'. \
                                                                    Character position [%s]"%i)
            if mode is NUMBER:
                field = to_number(field,i)
                mode = FIELD
            if mode is FIELD:
                row.append(field)
                field = ''
            verify(len(row) == len((dict_with_lists[table_name] or [row])[0]),
                   "Inconsistent row lengths found for table %s"%table_name)
            dict_with_lists[table_name].append(row)
            row = []
            mode = TABLE
        else:
            verify(mode is NONE or mode is ROW or mode is ROWNUM or mode is FIELD or mode is NUMBER, "Badly formatted string, \
                                                                    unrecognized '%s'. Character position [%s]"%(c,i))
            if mode is NONE:
                table_name += c
            elif mode is TABLE:
                mode = ROWNUM
                field += c
            else:
                mode = NUMBER
                field += c
    return tdf.TicDat(**dict_with_lists)