from ticdat.utils import verify, containerish, stringish, find_duplicates_from_dict_ticdat
import os, subprocess, inspect
from collections import defaultdict

INFINITY = 999999

def _code_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(_code_dir)))

def opl_run(mod_file, input_tdf, input_dat, soln_tdf, infinity=INFINITY, oplrun_path=None, post_solve=None):
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
    datfile = create_opl_text(input_tdf, input_dat, infinity)
    with open("temp.dat", "w") as f:
        f.write(datfile)
    verify(os.path.isfile("temp.dat"), "Could not create temp.dat")
    if not oplrun_path:
        with open(os.path.join(_code_dir(),"oplrun_path.txt"),"r") as f:
            oplrun_path = f.read()
    verify(os.path.isfile(oplrun_path), "Not a valid path to oplrun")
    output = subprocess.check_output([oplrun_path, mod_file, "temp.dat"])
    os.remove("temp.dat")
    if post_solve:
        post_solve()
    with open("testdebug","w") as f:
        f.write(output)
    def pattern_finder(string, pattern, rsearch=False):
        """
        Searches a string for the pattern ignoring whitespace
        :param string: A text string
        :param pattern: A string containing the pattern to search for
        :param rsearch: Optional parameter indicating if the search should be performed backwards
        """
        verify(len(pattern) <= len(string), "Pattern is larger than string, cannot be found. Pattern is '%s'" % pattern)
        poss_string = []
        nospaces = lambda (k): filter(lambda j: not j.isspace(), k)
        if rsearch:
            pattern = pattern[::-1]
            string = string[::-1]
        for i, j in enumerate(string):
            if len(nospaces(poss_string)) < len(pattern):
                poss_string.append(str(j))
            else:
                while (poss_string[0].isspace()):
                    poss_string.pop(0)
                poss_string.pop(0)
                poss_string.append(str(j))
                pass
            if ''.join(nospaces(poss_string)) == pattern:
                while (poss_string[0].isspace()):
                    poss_string.pop(0)
                if rsearch:
                    return len(string) - (i + 1 - len(poss_string))
                return i - len(poss_string)
        return False

    min = len(output) + 1
    for tbn in soln_tdf.primary_key_fields.keys():
        pos = pattern_finder(output, tbn + '={')
        verify(pos, "Invalid Output. Solution table '%s' not found." % tbn)
        if min > pos:
            min = pos
    max = pattern_finder(output, '>}', True)
    verify(max, "Invalid Output. Missing end of solution table.")
    verify(max > min,
           "Invalid Output. End of table (position %s) is positioned before table name is defined (position %s" % (
           min, max))
    return read_opl_text(soln_tdf, output[min:max], False)
    # not sure how to return None if no solution found


def create_opl_text(tdf, tic_dat, infinity=INFINITY):
    """
    Generate a OPL .dat string from a TicDat object
    :param tdf: A TicDatFactory defining the schema
    :param tic_dat: A TicDat object consistent with tdf
    :param infinity: A number used to represent infinity in OPL
    :return: A string consistent with the OPL .dat format
    """
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

def create_opl_mod_text(input_tdf, soln_tdf):
    """
    Generate a OPL .mod string from a TicDat object for diagnostic purposes
    :param input_tdf: A TicDatFactory defining the input schema
    :param soln_tdf: A TicDatFactory defining the solution schema (optional)
    :return: A string consistent with the OPL .mod format
    """
    verify(not input_tdf.generator_tables, "Input schema error - doesn't work with generator tables.")
    verify(not input_tdf.generic_tables, "Input schema error - doesn't work with generic tables. (not yet - will \
            add ASAP as needed) ")
    rtn = ''
    dict_tables = {t for t, pk in input_tdf.primary_key_fields.items() if pk}

    def get_type(data_types, table, field):
        try:
            return "float" if data_types[table][field].number_allowed else "string"
        except KeyError:
            return "string"

    def get_table_as_mod_text(tdf, tbn, is_soln=False):
        rtn = ''
        if len(tdf.primary_key_fields[t]) is 1 and len(tdf.data_fields[t]) is 0:
            rtn = "{" + get_type(tdf.data_types, t, tdf.primary_key_fields[t][0]) + "} " + t + " = ...;\n\n"
        else:
            rtn += "tuple " + t + "_type\n{"
            for pk in tdf.primary_key_fields[t]:
                pk.replace(' ', '_')
                rtn += "\n\tkey " + get_type(tdf.data_types, t, pk) + " " + pk + ";"
            for df in tdf.data_fields[t]:
                df.replace(' ', '_')
                rtn += "\n\t" + get_type(tdf.data_types, t, df) + " " + df + ";"
            rtn += "\n};\n"
            if not is_soln:
                rtn += "\n{" + t + "_type} " + t + "=...;\n"
            rtn += "\n"
        return rtn

    for t in dict_tables:
        rtn += get_table_as_mod_text(input_tdf, t)
    if soln_tdf:
        verify(not soln_tdf.generator_tables, "Solution schema error - doesn't work with generator tables")
        verify(not soln_tdf.generic_tables, "Solution schema error - doesn't work with generic tables (not yet - will \
                add ASAP as needed) ")
        soln_dict_tables = {t for t, pk in soln_tdf.primary_key_fields.items() if pk}
        for t in soln_dict_tables:
            rtn += get_table_as_mod_text(soln_tdf,t,True)
    return rtn

def read_opl_text(tdf,text, commaseperator = True):
    """
    Read an OPL .dat string
    :param tdf: A TicDatFactory defining the schema
    :param text: A string consistent with the OPL .dat format
    :return: A TicDat object consistent with tdf
    """
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
        if mode not in [STRING, ROWSTRING] and (c.isspace() or c == '{' or c == ';'):
            if mode in [ROWNUM,FIELD] and not commaseperator:
                c = ','
            else:
                continue
        if mode in [STRING, ROWSTRING]:
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
        elif c == '=':
            verify(mode is NONE, "Badly formatted string, unrecognized '='. Character position [%s]"%i)
            verify(len(table_name) > 0, "Badly formatted string, table name can't be blank. Character position [%s]"%i)
            verify(table_name not in dict_with_lists.keys(), "Can't have duplicate table name. [Character position [%s]"%i)
            dict_with_lists[table_name] = []
            mode = TABLE
        elif c == '<':
            verify(mode is TABLE, "Badly formatted string, unrecognized '<'. Character position [%s]"%i)
            mode = ROW

        elif c == ',':
            verify(mode in [ROW, FIELD, NUMBER, ROWNUM, TABLE], "Badly formatted string, unrecognized ','. \
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
            verify(mode in [ROW, TABLE], "Badly formatted string, unrecognized '\"'. Character position [%s]"%i)
            if mode is ROW:
                mode = STRING
            if mode is TABLE:
                mode = ROWSTRING

        elif c == '}':
            verify(mode in [TABLE, ROWNUM], "Badly formatted string, unrecognized '}'. Character position [%s]"%i)
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
            verify(mode in [ROW, FIELD, NUMBER], "Badly formatted string, unrecognized '>'. \
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
            verify(mode in [NONE, ROW, ROWNUM, FIELD, NUMBER], "Badly formatted string, \
                                                                    unrecognized '%s'. Character position [%s]"%(c,i))
            if mode is NONE:
                table_name += c
            elif mode is TABLE:
                mode = ROWNUM
                field += c
            else:
                mode = NUMBER
                field += c
    assert not find_duplicates_from_dict_ticdat(tdf, dict_with_lists), \
            "duplicates were found - if asserts are disabled, duplicate rows will overwrite"
    return tdf.TicDat(**dict_with_lists)