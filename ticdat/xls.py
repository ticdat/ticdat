"""
Read/write ticDat objects from xls files. Requires the xlrd/xlrt module.
PEP8
"""
import ticdat.utils as utils
from ticdat.utils import freezable_factory, TicDatError, verify, containerish, do_it, FrozenDict
import os
from collections import defaultdict
from itertools import product

try:
    import xlrd
except:
    xlrd=None
try:
    import xlwt
except:
    xlwt=None
try:
    import xlsxwriter as xlsx
except:
    xlsx=None

_can_unit_test = xlrd and xlwt and xlsx

_xlsx_hack_inf = 1e+100 # the xlsxwriter doesn't handle infinity as seamlessly as xls
_longest_sheet = 30

class XlsTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Excel files with ticDat objects.
    Your system will need the xlrd package to read .xls and .xlsx files,
    the xlwt package to write .xls files, and the xlsxwriter package to
    write .xlsx files.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A XlsTicDatFactory will
        automatically be associated with the xls attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, xls_file_path, row_offsets={}, headers_present = True,
                       treat_large_as_inf = False,
                       freeze_it = False):
        """
        Create a TicDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :param row_offsets: (optional) A mapping from table names to initial
                            number of rows to skip
        :param headers_present: Boolean. Does the first row of data contain the
                                column headers?
        :param treat_large_as_inf: Boolean. Treat numbers >= 1e100 as infinity
                                   Generally only needed for .xlsx files that were
                                   themselves created by ticdat (see write_file docs)
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the matching sheets.
        caveats: Missing sheets resolve to an empty table, but missing fields
                 on matching sheets throw an Exception.
                 Sheet names are considered case insensitive, and white space is replaced
                 with underscore for table name matching.
                 Field names are considered case insensitive, but white space is respected.
                 (ticdat supports whitespace in field names but not table names).
                 The following two caveats apply only if data_types are used.
                 --> Any field for which an empty string is invalid data and None is
                     valid data will replace the empty string with None.
                 --> Any field for which must_be_int is true will replace numeric
                     data that satisfies int(x)==x with int(x). In other words,
                     the ticdat equivalent of pandas.read_excel convert_float
                     is to set must_be_int to true in data_types.
        """
        self._verify_differentiable_sheet_names()
        verify(xlrd, "xlrd needs to be installed to use this subroutine")
        tdf = self.tic_dat_factory
        verify(not(treat_large_as_inf and tdf.generator_tables),
               "treat_large_as_inf not implemented for generator tables")
        verify(headers_present or not tdf.generic_tables,
               "headers need to be present to read generic tables")
        verify(utils.DataFrame or not tdf.generic_tables,
               "Strange absence of pandas despite presence of generic tables")
        rtn =  tdf.TicDat(**self._create_tic_dat_dict
                          (xls_file_path, row_offsets, headers_present))
        replaceable = defaultdict(dict)
        for t, dfs in tdf.data_types.items():
            replaceable[t] = {df for df, dt in dfs.items()
                           if (not dt.valid_data('')) and dt.valid_data(None)}
        for t in set(tdf.all_tables).difference(tdf.generator_tables, tdf.generic_tables):
            _t =  getattr(rtn, t)
            for r in _t.values() if utils.dictish(_t) else _t:
                for f,v in r.items():
                    if f in replaceable[t] and v == '':
                        r[f] = None
                    elif treat_large_as_inf:
                        if v >= _xlsx_hack_inf:
                            r[f] = float("inf")
                        if v <= -_xlsx_hack_inf:
                            r[f] = -float("inf")
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn
    def _verify_differentiable_sheet_names(self):
        rtn = defaultdict(set)
        for t in self.tic_dat_factory.all_tables:
            rtn[t[:_longest_sheet]].add(t)
        rtn = [v for k,v in rtn.items() if len(v) > 1]
        verify(not rtn, "The following tables collide when names are truncated to %s characters.\n%s"%
               (_longest_sheet, sorted(map(sorted, rtn))))
    def _get_sheets_and_fields(self, xls_file_path, all_tables, row_offsets, headers_present):
        verify(utils.stringish(xls_file_path) and os.path.exists(xls_file_path),
               "xls_file_path argument %s is not a valid file path."%xls_file_path)
        try :
            book = xlrd.open_workbook(xls_file_path)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xls_file_path, e.message))
        sheets = defaultdict(list)
        for table, sheet in product(all_tables, book.sheets()) :
            if table.lower()[:_longest_sheet] == sheet.name.lower().replace(' ', '_')[:_longest_sheet]:
                sheets[table].append(sheet)
        duplicated_sheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicated_sheets, "The following sheet names were duplicated : " +
               ",".join(duplicated_sheets))
        sheets = FrozenDict({k:v[0] for k,v in sheets.items() })
        field_indicies, missing_fields, dup_fields = {}, {}, {}
        for table, sheet in sheets.items() :
            field_indicies[table], missing_fields[table], dup_fields[table] = \
                self._get_field_indicies(table, sheet, row_offsets[table], headers_present)
        verify(not any(_ for _ in missing_fields.values()),
               "The following field names could not be found : \n" +
               "\n".join("%s : "%t + ",".join(bf) for t,bf in missing_fields.items() if bf))
        verify(not any(_ for _ in dup_fields.values()),
               "The following field names were duplicated : \n" +
               "\n".join("%s : "%t + ",".join(bf) for t,bf in dup_fields.items() if bf))
        return sheets, field_indicies
    def _create_generator_obj(self, xlsFilePath, table, row_offset, headers_present):
        tdf = self.tic_dat_factory
        ho = 1 if headers_present else 0
        def tableObj() :
            sheets, field_indicies = self._get_sheets_and_fields(xlsFilePath,
                                        (table,), {table:row_offset}, headers_present)
            if table in sheets :
                sheet = sheets[table]
                table_len = min(len(sheet.col_values(field_indicies[table][field]))
                               for field in tdf.data_fields[table])
                for x in (sheet.row_values(i) for i in range(table_len)[row_offset+ho:]):
                    yield self._sub_tuple(table, tdf.data_fields[table],
                                          field_indicies[table])(x)
        return tableObj

    def _create_tic_dat_dict(self, xls_file_path, row_offsets, headers_present):
        verify(utils.dictish(row_offsets) and
               set(row_offsets).issubset(self.tic_dat_factory.all_tables) and
               all(utils.numericish(x) and (x>=0) for x in row_offsets.values()),
               "row_offsets needs to map from table names to non negative row offset")
        row_offsets = dict({t:0 for t in self.tic_dat_factory.all_tables}, **row_offsets)
        tdf = self.tic_dat_factory
        rtn = {}
        sheets, field_indicies = self._get_sheets_and_fields(xls_file_path,
                                    set(tdf.all_tables).difference(tdf.generator_tables),
                                    row_offsets, headers_present)
        ho = 1 if headers_present else 0
        for tbl, sheet in sheets.items() :
            fields = tdf.primary_key_fields.get(tbl, ()) + tdf.data_fields.get(tbl, ())
            assert fields or tbl in self.tic_dat_factory.generic_tables
            indicies = field_indicies[tbl]
            table_len = min(len(sheet.col_values(indicies[field]))
                            for field in (fields or indicies))
            if tdf.primary_key_fields.get(tbl, ()) :
                tableObj = {self._sub_tuple(tbl, tdf.primary_key_fields[tbl], indicies)(x):
                            self._sub_tuple(tbl, tdf.data_fields.get(tbl, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in
                                        range(table_len)[row_offsets[tbl]+ho:])}
            elif tbl in tdf.generic_tables:
                tableObj = [{f:x[i] for f,i in field_indicies[tbl].items()}
                            for x in (sheet.row_values(i) for i in
                                      range(table_len)[row_offsets[tbl]+ho:])]
            else :
                tableObj = [self._sub_tuple(tbl, tdf.data_fields.get(tbl, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in
                                        range(table_len)[row_offsets[tbl]+ho:])]
            rtn[tbl] = tableObj
        for tbl in tdf.generator_tables :
            rtn[tbl] = self._create_generator_obj(xls_file_path, tbl, row_offsets[tbl],
                                                    headers_present)
        return rtn

    def find_duplicates(self, xls_file_path, row_offsets={}, headers_present = True):
        """
        Find the row counts for duplicated rows.
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema (non primary key tables ignored).
        :param row_offsets: (optional) A mapping from table names to initial
                            number of rows to skip (non primary key tables ignored)
        :param headers_present: Boolean. Does the first row of data contain the
                                column headers?
        caveats: Missing sheets resolve to an empty table, but missing primary fields
                 on matching sheets throw an Exception.
                 Sheet names are considered case insensitive.
        :return: A dictionary whose keys are the table names for the primary key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered
                 in the table, and the value is the count of records in the
                 Excel sheet with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        self._verify_differentiable_sheet_names()
        verify(xlrd, "xlrd needs to be installed to use this subroutine")
        verify(utils.dictish(row_offsets) and
               set(row_offsets).issubset(self.tic_dat_factory.all_tables) and
               all(utils.numericish(x) and (x>=0) for x in row_offsets.values()),
               "row_offsets needs to map from table names to non negative row offset")
        row_offsets = dict({t:0 for t in self.tic_dat_factory.all_tables}, **row_offsets)
        tdf = self.tic_dat_factory
        pk_tables = tuple(t for t,_ in tdf.primary_key_fields.items() if _)
        rtn = {t:defaultdict(int) for t in pk_tables}
        sheets, fieldIndicies = self._get_sheets_and_fields(xls_file_path, pk_tables,
                                        row_offsets, headers_present)
        ho = 1 if headers_present else 0
        for table, sheet in sheets.items() :
            fields = tdf.primary_key_fields[table] + tdf.data_fields.get(table, ())
            indicies = fieldIndicies[table]
            table_len = min(len(sheet.col_values(indicies[field])) for field in fields)
            for x in (sheet.row_values(i) for i in range(table_len)[row_offsets[table]+ho:]) :
                rtn[table][self._sub_tuple(table, tdf.primary_key_fields[table],
                                           indicies)(x)] += 1
        for t in list(rtn.keys()):
            rtn[t] = {k:v for k,v in rtn[t].items() if v > 1}
            if not rtn[t]:
                del(rtn[t])
        return rtn
    def _sub_tuple(self, table, fields, field_indicies) :
        assert set(fields).issubset(field_indicies)
        data_types = self.tic_dat_factory.data_types
        def _convert_float(x, field):
            rtn = x[field_indicies[field]]
            if utils.numericish(rtn) and utils.safe_apply(int)(rtn) == rtn and \
               table in data_types and field in data_types[table] and \
               data_types[table][field].must_be_int:
                return int(rtn)
            return rtn
        def rtn(x) :
            if len(fields) == 1 :
                return _convert_float(x, fields[0])
            return tuple(_convert_float(x, field) for field in fields)
        return rtn

    def _get_field_indicies(self, table, sheet, row_offset, headers_present) :
        fields = self.tic_dat_factory.primary_key_fields.get(table, ()) + \
                 self.tic_dat_factory.data_fields.get(table, ())
        if not headers_present:
            row_len = len(sheet.row_values(row_offset)) if sheet.nrows > 0  else len(fields)
            return ({f : i for i,f in enumerate(fields) if i < row_len},
                    [f for i,f in enumerate(fields) if i >= row_len], [])
        if sheet.nrows - row_offset <= 0 :
            return {}, fields, []
        if table in self.tic_dat_factory.generic_tables:
            temp_rtn = defaultdict(list)
            for ind, val in enumerate(sheet.row_values(row_offset)):
                temp_rtn[val].append(ind)
        else:
            temp_rtn =  {field:list() for field in fields}
            for field, (ind, val) in product(fields, enumerate(sheet.row_values(row_offset))) :
                if field == val or (all(map(utils.stringish, (field, val))) and
                                    field.lower() == val.lower()):
                    temp_rtn[field].append(ind)
        return ({field : inds[0] for field, inds in temp_rtn.items() if len(inds)==1},
                [field for field, inds in temp_rtn.items() if len(inds) == 0],
                [field for field, inds in temp_rtn.items() if len(inds) > 1])


    def write_file(self, tic_dat, file_path, allow_overwrite = False):
        """
        write the ticDat data to an excel file
        :param tic_dat: the data object to write (typically a TicDat)
        :param file_path: The file path of the excel file to create
                          Needs to end in either ".xls" or ".xlsx"
                          The latter is capable of writing out larger tables,
                          but the former handles infinity seamlessly.
                          If ".xlsx", then be advised that +/- float("inf") will be replaced
                          with +/- 1e+100
        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?
        :return:
        caveats: None may be written out as an empty string. This reflects the behavior of xlwt.
        """
        self._verify_differentiable_sheet_names()
        verify(utils.stringish(file_path) and
               (file_path.endswith(".xls") or file_path.endswith(".xlsx")),
               "file_path argument needs to end in .xls or .xlsx")
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid xls file path")
        verify(allow_overwrite or not os.path.exists(file_path),
               "The %s path exists and overwrite is not allowed"%file_path)
        if self.tic_dat_factory.generic_tables:
            dat, tdf = utils.create_generic_free(tic_dat, self.tic_dat_factory)
            return tdf.xls.write_file(dat, file_path, allow_overwrite)
        if file_path.endswith(".xls"):
            self._xls_write(tic_dat, file_path)
        else:
            self._xlsx_write(tic_dat, file_path)
    def _xls_write(self, tic_dat, file_path):
        verify(xlwt, "Can't write .xls files because xlwt package isn't installed.")
        tdf = self.tic_dat_factory
        book = xlwt.Workbook()
        for t in  sorted(sorted(tdf.all_tables),
                         key=lambda x: len(tdf.primary_key_fields.get(x, ()))) :
            sheet = book.add_sheet(t[:_longest_sheet])
            for i,f in enumerate(tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t, ())) :
                sheet.write(0, i, f)
            _t = getattr(tic_dat, t)
            if utils.dictish(_t) :
                for row_ind, (p_key, data) in enumerate(_t.items()) :
                    for field_ind, cell in enumerate( (p_key if containerish(p_key) else (p_key,)) +
                                        tuple(data[_f] for _f in tdf.data_fields.get(t, ()))):
                        sheet.write(row_ind+1, field_ind, cell)
            else :
                for row_ind, data in enumerate(_t if containerish(_t) else _t()) :
                    for field_ind, cell in enumerate(tuple(data[_f] for _f in tdf.data_fields[t])) :
                        sheet.write(row_ind+1, field_ind, cell)
        if os.path.exists(file_path):
            os.remove(file_path)
        book.save(file_path)
    def _xlsx_write(self, tic_dat, file_path):
        verify(xlsx, "Can't write .xlsx files because xlsxwriter package isn't installed.")
        tdf = self.tic_dat_factory
        if os.path.exists(file_path):
            os.remove(file_path)
        book = xlsx.Workbook(file_path)
        def clean_inf(x):
            if x == float("inf"):
                return _xlsx_hack_inf
            if x == -float("inf"):
                return -_xlsx_hack_inf
            return x
        for t in sorted(sorted(tdf.all_tables),
                         key=lambda x: len(tdf.primary_key_fields.get(x, ()))) :
            sheet = book.add_worksheet(t)
            for i,f in enumerate(tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t, ())) :
                sheet.write(0, i, f)
            _t = getattr(tic_dat, t)
            if utils.dictish(_t) :
                for row_ind, (p_key, data) in enumerate(_t.items()) :
                    for field_ind, cell in enumerate( (p_key if containerish(p_key) else (p_key,)) +
                                        tuple(data[_f] for _f in tdf.data_fields.get(t, ()))):
                        sheet.write(row_ind+1, field_ind, clean_inf(cell))
            else :
                for row_ind, data in enumerate(_t if containerish(_t) else _t()) :
                    for field_ind, cell in enumerate(tuple(data[_f] for _f in tdf.data_fields[t])) :
                        sheet.write(row_ind+1, field_ind, clean_inf(cell))
        book.close()
