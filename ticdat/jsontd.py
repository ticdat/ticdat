"""
Read/write ticDat objects from json files. Requires the json module
PEP8
"""
import os
from collections import defaultdict
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish
from ticdat.utils import find_duplicates_from_dict_ticdat
import datetime
import itertools

try:
    import json
except:
    json = None

_can_unit_test = json

def _standard_verify(tdf):
    verify(json, "json needs to be installed to use this subroutine")
    verify(not tdf.generator_tables, "json not yet implemented for generator tables.")
    verify(not tdf.generic_tables, "json not yet implemented for generic tables.\n" +
           "This is due to lack of multi-index json support. See goo.gl/u6FGBg")

def make_json_dict(tdf, tic_dat, verbose=False, use_infinity_io_flag_if_provided=False):
    assert tdf.good_tic_dat_object(tic_dat)
    def write_cell(t, f, x):
        if isinstance(x, datetime.datetime):
            return str(x)
        return x if not use_infinity_io_flag_if_provided else tdf._infinity_flag_write_cell(t, f, x)
    jdict = {t: [] for t in tdf.all_tables}
    for t in tdf.all_tables:
        all_fields = tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t,())
        def make_row(row):
            assert containerish(row) and len(row) == len(all_fields)
            row = [write_cell(t, f, x) for f, x in zip(all_fields, row)]
            return {f:v for f,v in zip(all_fields, row)} if verbose else row
        appender = lambda row : jdict[t].append(make_row(row))
        tbl = getattr(tic_dat, t)
        if tdf.primary_key_fields.get(t):
            for pk, data_row in tbl.items():
                appender((list(pk) if containerish(pk) else [pk]) +
                         [data_row[df] for df in tdf.data_fields[t]])
        else:
            for data_row in tbl:
                appender([data_row[df] for df in tdf.data_fields[t]])
    return jdict

class JsonTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing json files with TicDat objects.
    You need the json package to be installed to use it.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A JsonTicFactory will
        automatically be associated with the json attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def _looks_pandas(self, jdict):
        if not all(set(itertools.chain(*v)) == {'columns', 'data'} for v in self.tic_dat_factory.schema().values()):
            return all(dictish(v) and set(v.keys()) == {'columns', 'data'} for v in  jdict.values())
    def create_tic_dat(self, json_file_path, freeze_it = False, from_pandas = False):
        """
        Create a TicDat object from a json file

        :param json_file_path: A json file path. It should encode a dictionary
                               with table names as keys. Could also be an actual JSON string

        :param freeze_it: boolean. should the returned object be frozen?

        :param from_pandas: boolean.  If truthy, then use pandas json readers. See
                            PanDatFactory json readers for more details. This argument is historical, as a
                            json format that matches the PanDatFactory.json format will be detected automatically,
                            and thus client code is generally safe ignoring this argument completely.

        :return: a TicDat object populated by the matching tables.

        caveats: Table names matches are case insensitive and also
                 underscore-space insensitive.
                 Tables that don't find a match are interpreted as an empty table.
                 Dictionary keys that don't match any table are ignored.
        """
        _standard_verify(self.tic_dat_factory)
        if from_pandas:
            from ticdat import PanDatFactory
            pdf = PanDatFactory.create_from_full_schema(self.tic_dat_factory.schema(include_ancillary_info=True))
            _rtn = pdf.json.create_pan_dat(json_file_path)
            return pdf.copy_to_tic_dat(_rtn, freeze_it=freeze_it)
        jdict = self._create_jdict(json_file_path)
        if self._looks_pandas(jdict):
            return self.create_tic_dat(json_file_path, freeze_it=freeze_it, from_pandas=True)
        tic_dat_dict = self._create_tic_dat_dict(jdict)
        missing_tables = set(self.tic_dat_factory.all_tables).difference(tic_dat_dict)
        if missing_tables:
            print ("The following table names could not be found in the json file/string\n%s\n"%
                   "\n".join(missing_tables))
        rtn = self.tic_dat_factory.TicDat(**tic_dat_dict)
        rtn = self.tic_dat_factory._parameter_table_post_read_adjustment(rtn)
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn
    def find_duplicates(self, json_file_path, from_pandas = False):
        """
        Find the row counts for duplicated rows.

        :param json_file_path: A json file path. It should encode a dictionary
                               with table names as keys.

        :param from_pandas: boolean.  If truthy, then use pandas json readers. See
                            PanDatFactory json readers for more details.

        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the json entry with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        _standard_verify(self.tic_dat_factory)
        if from_pandas:
            from ticdat import PanDatFactory
            pdf = PanDatFactory.create_from_full_schema(self.tic_dat_factory.schema(include_ancillary_info=True))
            _rtn = pdf.json.create_pan_dat(json_file_path)
            jdict = {t: [tuple(_) for _ in getattr(_rtn, t).itertuples(index=False)] for t in pdf.all_tables}
        else:
            jdict = self._create_jdict(json_file_path)
            if self._looks_pandas(jdict):
                return self.find_duplicates(json_file_path, from_pandas=True)
        rtn = find_duplicates_from_dict_ticdat(self.tic_dat_factory, jdict)
        return rtn or {}
    def _create_jdict(self, path_or_buf):
        if stringish(path_or_buf) and os.path.exists(path_or_buf):
            reasonble_string = path_or_buf
            verify(os.path.isfile(path_or_buf), "json_file_path is not a valid file path.")
            try :
                with open(path_or_buf, "r") as fp:
                    jdict = json.load(fp)
            except Exception as e:
                raise TicDatError("Unable to interpret %s as json file : %s" %
                                  (path_or_buf, e))
        else:
            verify(stringish(path_or_buf), "%s isn't a string" % path_or_buf)
            reasonble_string = path_or_buf[:10]
            try:
                jdict = json.loads(path_or_buf)
            except Exception as e:
                raise TicDatError("Unable to interpret %s as json string : %s" %
                                  (reasonble_string, e))

        verify(dictish(jdict), "%s failed to load a dictionary" % reasonble_string)
        verify(all(map(stringish, jdict)),
               "The dictionary loaded from %s isn't indexed by strings" % reasonble_string)
        verify(all(map(containerish, jdict.values())),
               "The dictionary loaded from %s doesn't have containers as values" % reasonble_string)
        return jdict
    def _create_tic_dat_dict(self, jdict):
        tdf = self.tic_dat_factory
        rtn = {}
        table_keys = defaultdict(list)
        for t in tdf.all_tables:
            for t2 in jdict:
                if stringish(t2) and t.lower() == t2.replace(" ", "_").lower():
                    table_keys[t].append(t2)
            if len(table_keys[t]) >= 1:
                verify(len(table_keys[t]) < 2, "Found duplicate matching keys for table %s"%t)
                rtn[t] = jdict[table_keys[t][0]]
        orig_rtn, rtn = rtn, {}
        for t, rows in orig_rtn.items():
            all_fields = tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ())
            rtn[t] = []
            for row in rows:
                if dictish(row):
                    rtn[t].append({f: tdf._general_read_cell(t, f, x) for f, x in row.items()})
                else:
                    rtn[t].append([tdf._general_read_cell(t, f, x) for f, x in zip(all_fields, row)])
        return rtn
    def write_file(self, tic_dat, json_file_path, allow_overwrite=False, verbose=False, to_pandas=False):
        """
        write the ticDat data to a json file (or json string)

        :param tic_dat: the data object to write (typically a TicDat)

        :param json_file_path: The file path of the json file to create. If empty string, then return a JSON string.

        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?

        :param verbose: boolean. Verbose mode writes the data rows as dicts
                        keyed by field name. Otherwise, they are lists.

        :param to_pandas: boolean. if truthy, then use the PanDatFactory method of writing to json.

        :return:
        """
        _standard_verify(self.tic_dat_factory)
        verify(not (to_pandas and verbose), "verbose argument is inconsistent with to_pandas")
        verify(not (json_file_path and os.path.exists(json_file_path) and not allow_overwrite),
               "%s exists and allow_overwrite is not enabled"%json_file_path)
        if to_pandas:
            from ticdat import PanDatFactory
            pdf = PanDatFactory.create_from_full_schema(self.tic_dat_factory.schema(include_ancillary_info=True))
            return pdf.json.write_file(self.tic_dat_factory.copy_to_pandas(tic_dat, drop_pk_columns=False),
                                       json_file_path)
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        jdict = make_json_dict(self.tic_dat_factory, tic_dat, verbose, use_infinity_io_flag_if_provided=True)
        if not json_file_path:
            return json.dumps(jdict, sort_keys=True, indent=2)
        with open(json_file_path, "w") as fp:
            json.dump(jdict, fp, sort_keys=True, indent=2)
