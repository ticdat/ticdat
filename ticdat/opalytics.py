"""
Read ticDat objects from Opalytics Cloud Platform.
PEP8
"""

# Note that we're really just leveraging ticdat's ability handle DataFrame's here
from ticdat.utils import freezable_factory, verify, find_duplicates, create_duplicate_focused_tdf
from ticdat.utils import dictish, DataFrame, stringish
from itertools import product
from collections import defaultdict
import inspect

class OpalyticsTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading ticDat objects from the Opalytics Cloud Platform
    Not expected to used outside of Opalytics Cloud hosted notebooks
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A OpalyticsTicFactory will
        automatically be associated with the opalytics attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._duplicate_focused_tdf = create_duplicate_focused_tdf(tic_dat_factory)
        self._isFrozen = True

    def _find_table_matchings(self, inputset):
        rtn = defaultdict(list)
        for t,x in product(self.tic_dat_factory.all_tables, inputset.schema):
            if stringish(x) and t.lower() == x.lower().replace(" ", "_"):
                rtn[t].append(x)
        return rtn
    def _good_inputset(self, inputset, message_writer = lambda x : x):
        if not hasattr(inputset, "schema") and dictish(inputset.schema):
            message_writer("Failed to find dictish schema attribute")
            return False
        if not hasattr(inputset, "getTable") and callable(inputset.getTable):
            message_writer("Failed to find calleable getTable attribute")
            return False
        table_matchings = self._find_table_matchings(inputset)
        badly_matched = {t for t,v in table_matchings.items() if len(v) != 1}
        if badly_matched:
            message_writer("Following tables could not be uniquely resolved in inputset.schema\n%s"%
                           badly_matched)
            return False
        return True
    def find_duplicates(self, inputset, raw_data=False):
        """
        Find the row counts for duplicated rows.
        :param inputset: An opalytics inputset consistent with this TicDatFactory
        :param raw_data: boolean. should data cleaning be skipped? See create_tic_dat.
        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the table with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates.
        """
        message = []
        verify(self._good_inputset(inputset, message.append),
               "inputset is inconsistent with this TicDatFactory : %s"%(message or [None])[0])
        if not self._duplicate_focused_tdf:
            return {}
        tdf = self._duplicate_focused_tdf
        return find_duplicates(tdf.opalytics.create_tic_dat(inputset, raw_data=raw_data), tdf)
    def _table_as_lists(self, t, df):
        verify(isinstance(df, DataFrame), "table %s isn't a DataFrame"%t)
        all_fields = set(self.tic_dat_factory.primary_key_fields[t]).\
                     union(self.tic_dat_factory.data_fields[t])
        verify("_active" not in all_fields, "Table %s has a field named '_active'.\n" +
               "This conflicts with internal data processing.\n" +
               " Don't use '_active' for in your TicDatFactory definition if you want to use this reader.")
        for f in all_fields:
            verify(f in df.columns, "field %s can't be found in the DataFrame for %s"%(f,t))
        all_fields = {f:list(df.columns).index(f) for f in all_fields}
        has_active = "_active" in df.columns
        active_index = list(df.columns).index("_active") if has_active else None
        rtn = []
        for row in df.itertuples(index=False, name=None):
            if not has_active or row[active_index]:
                rtn.append(tuple(row[all_fields[f]] for f in self.tic_dat_factory.primary_key_fields[t] +
                                 self.tic_dat_factory.data_fields[t]))
        return rtn

    def create_tic_dat(self, inputset, raw_data=False, freeze_it=False):
        """
        Create a TicDat object from an opalytics inputset
        :param inputset: An opalytics inputset consistent with this TicDatFactory
        :param raw_data: boolean. should data cleaning be skipped? On the Opalytics Cloud Platform
                         cleaned data will be passed to instant apps. Data cleaning involves
                         removing data type failures, data row predicate failures, foreign key
                         failures and deactivated records.
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the tables as they are rendered by inputset
        """
        message = []
        verify(self._good_inputset(inputset, message.append),
               "inputset is inconsistent with this TicDatFactory : %s"%(message or [None])[0])
        verify(DataFrame, "pandas needs to be installed to use the opalytics functionality")
        verify(not self.tic_dat_factory.generator_tables or self.tic_dat_factory.generic_tables,
               "The opalytics data reader is not yet working for generic tables nor generator tables")

        tms = {k:v[0] for k,v in self._find_table_matchings(inputset).items()}
        ia = {}
        if "includeActive" in inspect.getargspec(inputset.getTable)[0]:
            ia = {"includeActive": not raw_data}
        tl = lambda t: self._table_as_lists(t, inputset.getTable(tms[t], **ia))
        rtn = self.tic_dat_factory.TicDat(**{t:tl(t) for t in self.tic_dat_factory.all_tables})
        if not raw_data:
            def removing():
                dtfs = self.tic_dat_factory.find_data_type_failures(rtn)
                for (t,f),(bvs, pks) in dtfs.items():
                    if pks is None: # i.e. no primary keys
                        for dr in getattr(rtn, t):
                            if dr[f] in bvs:
                                getattr(rtn, t).remove(dr)
                    else:
                        for k in pks:
                            getattr(rtn, t).pop(k, None) # could be popped for two fields
                drfs = self.tic_dat_factory.find_data_row_failures(rtn)
                cant_remove_again = set()
                for (t, pn),row_posns in drfs.items():
                    if self.tic_dat_factory.primary_key_fields[t]:
                        for k in row_posns:
                            getattr(rtn, t).pop(k, None) # could be popped for two predicates
                    elif t not in cant_remove_again:
                        bad_drs = [dr for i,dr in enumerate(getattr(rtn, t)) if i in row_posns]
                        for dr in bad_drs:
                            getattr(rtn, t).remove(dr)
                        # once we start removing data rows by row index, the remaining row indicies
                        # become invalid, so will need to ignoring any more such indicies for this table
                        cant_remove_again.add(t)

                fkfs = self.tic_dat_factory.find_foreign_key_failures(rtn)
                if fkfs:
                    self.tic_dat_factory.remove_foreign_keys_failures(rtn)
                return dtfs or drfs or fkfs
            while removing():
                pass
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn

