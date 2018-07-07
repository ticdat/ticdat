"""
Create PanDatFactory. Along with ticdatfactory.py, one of two main entry points for ticdat library.
PEP8
"""

import ticdat.utils as utils
from ticdat.utils import ForeignKey, ForeignKeyMapping, TypeDictionary
pd, DataFrame = utils.pd, utils.DataFrame # if pandas not installed will be falsey


class PanDatFactory(object):
    """
    Creates simple schema shin functionality for defining schemas with pandas.DataFrame objects.
    This class is constructed with a schema, and can be used to generate PanDat objects,
    or to write PanDat objects to different file types. Analytical code that uses PanDat objects can be used,
    without change, on different data sources.
    """
    @property
    def primary_key_fields(self):
        return self._primary_key_fields
    @property
    def data_fields(self):
        return self._data_fields
    def schema(self, include_ancillary_info = False):
        """
        :param include_ancillary_info: if True, include all the foreign key, default, and data type information
                                       as well. Otherwise, just return table-fields dictionary
        :return: a dictionary with table name mapping to a list of lists
                 defining primary key fields and data fields
                 If include_ancillary_info, this table-fields dictionary is just one entry in a more comprehensive
                 dictionary.
        """
        tables_fields = {t: [list(self.primary_key_fields.get(t, [])),
                             list(self.data_fields.get(t, []))]
                          for t in set(self.primary_key_fields).union(self.data_fields)}
        for t in self.generic_tables:
            tables_fields[t]='*'
        if not include_ancillary_info:
            return tables_fields
        return {"tables_fields" : tables_fields,
                "foreign_keys" : self.foreign_keys,
                "default_values" : self.default_values,
                "data_types" : self.data_types}
