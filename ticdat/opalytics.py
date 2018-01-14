"""
Read ticDat objects from Opalytics Cloud Platform.
PEP8
"""

# Note that we're really just leveraging ticdat's ability handle DataFrame's here
from ticdat.utils import freezable_factory, verify, find_duplicates, create_duplicate_focused_tdf
from ticdat.utils import dictish, DataFrame


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
    def _good_inputset(self, inputset):
        return hasattr(inputset, "schema") and dictish(inputset.schema) and \
               hasattr(inputset, "getTable") and callable(inputset.getTable) and \
               set(self.tic_dat_factory.all_tables).issubset(inputset.schema)
    def find_duplicates(self, inputset):
        """
        Find the row counts for duplicated rows.
        :param inputset: An opalytics inputset consistent with this TicDatFactory
        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the table with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates.
        """
        verify(self._good_inputset(inputset), "inputset is inconsistent with this TicDatFactory")
        if not self._duplicate_focused_tdf:
            return {}
        return find_duplicates(self._duplicate_focused_tdf.opalytics.create_tic_dat(inputset),
                               self._duplicate_focused_tdf)
    def _table_as_lists(self, t, df):
        verify(isinstance(df, DataFrame), "table %s isn't a DataFrame"%t)
        all_fields = set(self.tic_dat_factory.primary_key_fields[t]).\
                     union(self.tic_dat_factory.data_fields[t])
        for f in all_fields:
            verify(f in df.columns, "field %s can't be found in the DataFrame for %s"%(f,t))
        d = df.T.to_dict()
        rtn = []
        for k in sorted(d):
            rtn.append({f:d[k][f] for f in all_fields})
        return rtn

    def create_tic_dat(self, inputset, freeze_it = False):
        """
        Create a TicDat object from an opalytics inputset
        :param inputset: An opalytics inputset consistent with this TicDatFactory
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the tables as they are rendered by inputset
        """
        verify(self._good_inputset(inputset), "inputset is inconsistent with this TicDatFactory")
        verify(DataFrame, "pandas needs to be installed to use the opalytics functionality")

        rtn = self.tic_dat_factory.TicDat(**{t:self._table_as_lists(t, inputset.getTable(t))
                                             for t in self.tic_dat_factory.all_tables})
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn

