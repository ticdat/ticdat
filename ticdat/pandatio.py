try:
    import xlrd
except:
    xlrd=None
import os
from ticdat.utils import freezable_factory, verify, case_space_to_pretty, pd, TicDatError, FrozenDict, all_fields
from itertools import product
from collections import defaultdict

_longest_sheet = 30 # seems to be an Excel limit with pandas

class XlsPanFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Excel files with panDat objects.
    Your system will need the xlrd package to read to read table names in a case-space insensitive way.
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A XlsPanFactory will
        automatically be associated with the xls attribute of the parent
        PanDatFactory.
        :param pan_dat_factory:
        :return:
        """
        self.pan_dat_factory = pan_dat_factory
        self._isFrozen = True

    def create_tic_dat(self, xls_file_path):
        """
        Create a TicDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :return: a PanDat object populated by the matching sheets.
        caveats: Missing sheets resolve to an empty table, but missing fields
                 on matching sheets throw an Exception.
                 If xlrd is installed, then sheet names are considered case insensitive,
                 and white space is replaced with underscore for table name matching.
                 If xlrd isn't installed, then sheet names must match perfectly.
                 Field names are considered case insensitive, but white space is respected.
                 (ticdat supports whitespace in field names but not table names).
                 The following two caveats apply only if data_types are used.
        """
        rtn = {}
        for t, s in self._get_sheet_names(xls_file_path).items():
            tbl = self._read_sheet(xls_file_path, s)
            if tbl is not None:
                rtn[t] = tbl
        missing_tables = {t for t in self.pan_dat_factory.all_tables if t not in rtn}
        if missing_tables:
            print ("The following table names could not be found in the %s file.\n%s\n"%
                   (xls_file_path,"\n".join(missing_tables)))
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        verify(not missing_fields, "The following are (table, field) pairs missing from the %s file.\n%s"%
               (xls_file_path, missing_fields))
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _read_sheet(self, xls_file_path, sheet):
        verify(pd, "pandas not installed")
        if xlrd:
            return pd.read_excel(xls_file_path, sheet)
        try:
            return pd.read_excel(xls_file_path, sheet)
        except:
            return None
    def _get_sheet_names(self, xls_file_path):
        if not xlrd:
            return FrozenDict({t:t for t in self.pan_dat_factory.all_tables})
        try :
            book = xlrd.open_workbook(xls_file_path)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xls_file_path, e.message))
        sheets = defaultdict(list)
        for table, sheet in product(self.pan_dat_factory.all_tables, book.sheets()) :
            if table.lower()[:_longest_sheet] == sheet.name.lower().replace(' ', '_')[:_longest_sheet]:
                sheets[table].append(sheet)
        duplicated_sheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicated_sheets, "The following sheet names were duplicated : " +
               ",".join(duplicated_sheets))
        sheets = FrozenDict({k:v[0].name for k,v in sheets.items() })
        return sheets
    def write_file(self, pan_dat, file_path, allow_overwrite = False, case_space_sheet_names = False):
        """
        write the panDat data to an excel file
        :param pan_dat: the PanDat object to write
        :param file_path: The file path of the excel file to create
        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?
              case_space_sheet_names: boolean - make best guesses how to add spaces and upper case
                                      characters to sheet names
        :return:
        caveats: The row names (index) isn't written.
        """
        verify(pd, "pandas not installed")
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid xls file path")
        verify(allow_overwrite or not os.path.exists(file_path),
               "The %s path exists and overwrite is not allowed"%file_path)
        case_space_sheet_names = case_space_sheet_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        writer = pd.ExcelWriter(file_path)
        for t in self.pan_dat_factory.all_tables:
            getattr(pan_dat, t).to_excel(writer, case_space_to_pretty(t) if case_space_sheet_names else t,
                                         index=False)
        writer.save()
