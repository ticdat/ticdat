from ticdat.utils import verify, containerish, stringish, find_duplicates_from_dict_ticdat
from ticdat.utils import find_case_space_duplicates, change_fields_with_reserved_keywords
import ticdat.utils as tu
from ticdat.ticdatfactory import TicDatFactory
import os, subprocess, inspect, time, uuid, shutil
from collections import defaultdict
from ticdat.jsontd import make_json_dict

INFINITY = 999999 # Does lingo have a way to mark infinity?

lingo_keywords = ["I imagine this will come up eventually"]

def _code_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(_code_dir)))

def _fix_fields_with_lingo_keywords(tdf):
    return change_fields_with_reserved_keywords(tdf, lingo_keywords)

def _unfix_fields_with_lingo_keywords(tdf):
    return change_fields_with_reserved_keywords(tdf, lingo_keywords, True)

def _data_has_underscores(tdf, tic_dat):
    has_underscores = False
    has_spaces = False
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
    for i, (t, l) in enumerate(dict_with_lists.items()):
        for row in l:
            for field in row:
                if stringish(field):
                    if " " in field:
                        has_spaces = True
                    if "_" in field:
                        has_underscores = True
                    verify(not (stringish(field) and has_spaces and has_underscores),
                           "Lingo doesn't support spaces in strings,"
                           " so data can't contain spaces and strings. In table %s,"
                           " field %s" % (t, field))
    return has_underscores


def lingo_run(lng_file, input_tdf, input_dat, soln_tdf, infinity=INFINITY, lingorun_path=None):
    os.environ["TICDAT_LINGO_PATH"] = "/opt/opalytics/lenticular/sams-stuff/lingo/install/runlingo"
    tu.development_deployed_environment = True
    """
    solve an optimization problem using an Lingo .lng file
    :param lng_file: An Lingo .lng file.
    :param input_tdf: A TicDatFactory defining the input schema
    :param input_dat: A TicDat object consistent with input_tdf
    :param soln_tdf: A TicDatFactory defining the solution variables
    :param infinity: A number used to represent infinity in Lingo
    :return: a TicDat object consistent with soln_tdf, or None if no solution found
    """
    verify(os.path.isfile(lng_file), "lng_file %s is not a valid file."%lng_file)
    verify(not find_case_space_duplicates(input_tdf), "There are case space duplicate field names in the input schema.")
    verify(not find_case_space_duplicates(soln_tdf), "There are case space duplicate field names in the solution schema.")
    verify(len({input_tdf.lingo_prepend + t for t in input_tdf.all_tables}.union(
               {soln_tdf.lingo_prepend + t for t in soln_tdf.all_tables})) ==
           len(input_tdf.all_tables) + len(soln_tdf.all_tables),
           "There are colliding input and solution table names.\nSet lingo_prepend so " +
           "as to insure the input and solution table names are effectively distinct.")
    msg = []
    verify(input_tdf.good_tic_dat_object(input_dat, msg.append),
           "tic_dat not a good object for the input_tdf factory : %s"%"\n".join(msg))
    orig_input_tdf, orig_soln_tdf = input_tdf, soln_tdf
    input_tdf = _fix_fields_with_lingo_keywords(input_tdf)
    soln_tdf = _fix_fields_with_lingo_keywords(soln_tdf)
    input_dat = input_tdf.TicDat(**make_json_dict(orig_input_tdf, input_dat))
    assert input_tdf.good_tic_dat_object(input_dat)
    lng_file_name = os.path.basename(lng_file)[:-4]
    with open(lng_file, "r") as f:
        lng = f.read()
        assert ("ticdat_" + lng_file_name + ".lng") in lng
        assert ("ticdat_" + lng_file_name + "_output.dat") in lng
        assert ("ticdat_" + lng_file_name + ".dat") in lng
    working_dir = os.path.abspath(os.path.dirname(lng_file))
    if tu.development_deployed_environment:
        working_dir = os.path.join(working_dir, "lingoticdat_%s"%uuid.uuid4())
        shutil.rmtree(working_dir, ignore_errors = True)
        os.mkdir(working_dir)
        working_dir = os.path.abspath(working_dir)
        _ = os.path.join(working_dir, os.path.basename(lng_file))
        shutil.copy(lng_file, _)
        lng_file = _
    commandsfile = os.path.join(working_dir, "ticdat_"+lng_file_name+".ltf")
    datfile = os.path.join(working_dir, "ticdat_"+lng_file_name+".dat")
    output_txt = os.path.join(working_dir, "output.txt")
    soln_tables = {t for t, pk in soln_tdf.primary_key_fields.items() if pk}
    results = []
    for tbn in soln_tables:
        fn = os.path.join(working_dir, tbn+".dat")
        if os.path.isfile(fn):
            os.remove(fn)
        results.append(fn)
    has_underscores = _data_has_underscores(input_tdf, input_dat)
    with open(datfile, "w") as f:
        f.write(create_lingo_text(input_tdf, input_dat, infinity))
    verify(os.path.isfile(datfile), "Could not create ticdat_" + lng_file_name+".dat")
    with open(os.path.join(working_dir, "ticdat_"+lng_file_name+".lng"), "w") as f:
        f.write("! Autogenerated input file, created by lingo.py on " + time.asctime() + " ;\n")
        f.write(create_lingo_mod_text(orig_input_tdf))
    with open(os.path.join(working_dir,"ticdat_"+lng_file_name+"_output.dat"), "w") as f:
        f.write("! Autogenerated output file, created by lingo.py on " + time.asctime() + " ;\n")
        f.write(create_lingo_output_text(orig_soln_tdf))
    commands = [
        "! Autogenerated commands file, created by lingo.py on " + time.asctime() + " ;",
        "TAKE " + lng_file,
        "GO",
        "QUIT"
    ]
    with open(commandsfile, "w") as f:
        f.write("\n".join(commands))

    if not lingorun_path:
        if 'TICDAT_LINGO_PATH' in os.environ.keys():
            lingorun_path = os.environ['TICDAT_LINGO_PATH']
        else:
            verify(os.path.isfile(os.path.join(_code_dir(),"lingo_run_path.txt")),
               "need to either pass lingorun_path argument or run lingo_run_setup.py")
            with open(os.path.join(_code_dir(),"lingorun_path.txt"),"r") as f:
                lingorun_path = f.read().strip()
    verify(os.path.isfile(lingorun_path), "%s not a valid path to lingorun"%lingorun_path)
    output = ''
    try:
        output = subprocess.check_output([lingorun_path, commandsfile], stderr=subprocess.STDOUT, cwd=working_dir)
    except subprocess.CalledProcessError as err:
        if tu.development_deployed_environment:
            raise Exception("runlingo failed to complete: " + str(err.output))
    with open(output_txt, "w") as f:
        f.write(output)
    output_data = {}
    for i in zip(soln_tables,results):
        if not os.path.isfile(i[1]):
            print("%s is not a valid file. A solution was likely not generated. Check 'output.txt' for details."%i[1])
            return None
        with open(i[1], "r") as f:
            output_data[i[0]] = f.read()
    soln_tdf = _unfix_fields_with_lingo_keywords(soln_tdf)
    return read_lingo_text(soln_tdf, output_data, has_underscores)

_can_run_lingo_run_tests = os.path.isfile(os.path.join(_code_dir(),"runlingo_path.txt"))

def create_lingo_output_text(tdf):
    """
    I'm not sure what to put here yet.
    :param tdf:
    :return:
    """
    dict_tables = {t for t, pk in tdf.primary_key_fields.items() if pk}
    rtn = 'data:\n'
    for tbn in dict_tables:
        rtn += '\t@TEXT(\"' + tbn + ".dat\") = " + tbn
        for fk in tdf.data_fields[tbn]:
            rtn += ", " + tbn + "_" + fk.lower().replace(" ","")
        rtn += ";\n"
    rtn += 'enddata'
    return rtn

def create_lingo_text(tdf, tic_dat, infinity=INFINITY):
    """
    Generate a lingo .dat string from a TicDat object
    :param tdf: A TicDatFactory defining the schema
    :param tic_dat: A TicDat object consistent with tdf
    :param infinity: A number used to represent infinity in lingo
    :return: A string consistent with the lingo .dat format
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
    rtn = "data:\n"
    for i, (t,l) in enumerate(sorted(dict_with_lists.items(), key=lambda k: len(tdf.primary_key_fields[k[0]]))):
        rtn += "%s"%(tdf.lingo_prepend + t)
        for field in tdf.data_fields[t]:
            rtn += ',' +t + "_" + field.replace(" ", "_").lower()
        rtn += "=\n"
        for row in l:
            rtn += "\t"
            for field in row:
                if stringish(field):
                    rtn += field.replace(" ", "_") + " "
                else:
                    # Is this if condition right?
                    rtn += str(infinity) if float('inf') == field else str(field) + " "
            rtn += "\n"
        rtn += ";\n"
    rtn+="enddata"
    return rtn

def create_lingo_mod_text(tdf):
    """
    Generate a lingo .mod string from a TicDat object for diagnostic purposes
    :param tdf: A TicDatFactory defining the input schema
    :return: A string consistent with the lingo .mod input format
    """
    verify(not find_case_space_duplicates(tdf), "There are case space duplicate field names in the schema.")
    verify(not tdf.generator_tables, "Input schema error - doesn't work with generator tables.")
    verify(not tdf.generic_tables, "Input schema error - doesn't work with generic tables. (not yet - will \
            add ASAP as needed) ")
    tdf = _fix_fields_with_lingo_keywords(tdf)
    rtn = 'sets:\n'
    dict_tables = {t for t, pk in tdf.primary_key_fields.items() if pk}
    verify(set(dict_tables) == set(tdf.all_tables), "not yet handling non-PK tables of any sort")

    prepend = getattr(tdf, "lingo_prepend", "")

    def get_table_as_mod_text(tdf, tbn):
        p_tbn = prepend + tbn
        rtn = p_tbn
        if len(tdf.primary_key_fields[tbn]) > 1:
            rtn += '(foods, categories) ' # I'm cheating for now
        rtn += ':'
        fields = []
        for df in tdf.data_fields[tbn]:
            df_m = p_tbn + '_' + df.replace(' ', '_').lower()
            fields.append(df_m)
        rtn += ','.join(fields)
        rtn += ';\n'
        return rtn

    # This is dangerous if multi PK tables depend on one another
    for t in sorted(dict_tables, key=lambda k: len(tdf.primary_key_fields[k])):
        rtn += get_table_as_mod_text(tdf, t)
    rtn+='endsets'
    return rtn

# This might make more sense as read_lingo_solution
def read_lingo_text(tdf,results_text, has_underscores):
    """
    Read an lingo .dat string
    :param tdf: A TicDatFactory defining the schema
    :param results_text: A list of strings defining lingo tables
    :param has_underscores:
    :return: A TicDat object consistent with tdf
    """

    for i in results_text.values():
        verify(stringish(i), "text needs to be a string")

    def _get_as_type(val):
        try:
            return float(val)
        except ValueError:
            return val.replace("_", " ") if not has_underscores else val

    dict_with_lists = defaultdict(list)

    for tbn in results_text.keys():
        rows = []
        text = results_text[tbn].strip().split("\n")
        for line in text:
            rows.append(map(lambda k: _get_as_type(k),line.strip().split()))
        dict_with_lists[tbn] = rows


    assert not find_duplicates_from_dict_ticdat(tdf, dict_with_lists), \
            "duplicates were found - if asserts are disabled, duplicate rows will overwrite"

    return tdf.TicDat(**{k.replace(tdf.lingo_prepend,"",1):v for k,v in dict_with_lists.items()})