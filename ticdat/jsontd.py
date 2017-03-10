"""
Read/write ticDat objects from json files. Requires the json module
PEP8
"""
import os
from collections import defaultdict
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish
from ticdat.utils import find_duplicates_from_dict_ticdat

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

def make_json_dict(tdf, tic_dat, verbose=False):
    assert tdf.good_tic_dat_object(tic_dat)
    jdict = defaultdict(list)
    for t in tdf.all_tables:
        all_fields = tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t,())
        def make_row(row):
            assert containerish(row) and len(row) == len(all_fields)
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
    Primary class for reading/writing json files with ticDat objects.
    You need the json package to be installed to use it.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A JsonTicFactory will
        automatically be associated with the sql attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, json_file_path, freeze_it = False):
        """
        Create a TicDat object from a json file
        :param json_file_path: A json file path. It should encode a dictionary
                               with table names as keys.
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the matching tables.
        caveats: Table names matches are case insensitive and also
                 underscore-space insensitive.
                 Tables that don't find a match are inteprested as an empty table.
                 Dictionary keys that don't match any table are ignored.
        """
        _standard_verify(self.tic_dat_factory)
        jdict = self._create_jdict(json_file_path)
        rtn = self.tic_dat_factory.TicDat(**self._create_tic_dat_dict(jdict))
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn
    def find_duplicates(self, json_file_path):
        """
        Find the row counts for duplicated rows.
        :param json_file_path: A json file path. It should encode a dictionary
                               with table names as keys.
        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the json entry with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        _standard_verify(self.tic_dat_factory)
        jdict = self._create_jdict(json_file_path)
        rtn = find_duplicates_from_dict_ticdat(self.tic_dat_factory, jdict)
        return rtn or {}
    def _create_jdict(self, json_file_path):
        verify(os.path.isfile(json_file_path), "json_file_path is not a valid file path.")
        try :
            with open(json_file_path, "r") as fp:
                jdict = json.load(fp)
        except Exception as e:
            raise TicDatError("Unable to interpret %s as json file : %s"%
                              (json_file_path, e.message))
        verify(dictish(jdict), "%s failed to load a dictionary"%json_file_path)
        verify(all(map(stringish, jdict)),
               "The dictionary loaded from %s isn't indexed by strings"%json_file_path)
        verify(all(map(containerish, jdict.values())),
               "The dictionary loaded from %s doesn't have containers as values"%json_file_path)
        return jdict
    def _create_tic_dat_dict(self, jdict):
        tdf = self.tic_dat_factory
        rtn = {}
        table_keys = defaultdict(list)
        for t in tdf.all_tables:
            for t2 in jdict:
                if stringish(t2) and t.lower() == t2.replace(" ", "_").lower():
                    table_keys[t].append(t2)
            verify(len(table_keys[t]) >= 1, "Unable to find a matching key for table %s"%t)
            verify(len(table_keys[t]) < 2, "Found duplicate matching keys for table %s"%t)
            rtn[t] = jdict[table_keys[t][0]]
        return rtn
    def write_file(self, tic_dat, json_file_path, allow_overwrite = False, verbose = False):
        """
        write the ticDat data to an excel file
        :param tic_dat: the data object to write (typically a TicDat)
        :param json_file_path: The file path of the json file to create.
        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?
        :param verbose: boolean. Verbose mode writes the data rows as dicts
                        keyed by field name. Otherwise, they are lists.
        :return:
        """
        _standard_verify(self.tic_dat_factory)
        verify(not (os.path.exists(json_file_path) and not allow_overwrite),
               "%s exists and allow_overwrite is not enabled")
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        jdict = make_json_dict(self.tic_dat_factory, tic_dat, verbose)
        with open(json_file_path, "w") as fp:
            json.dump(jdict, fp, sort_keys=True, indent=2)