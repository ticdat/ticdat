"""
Read/write ticDat objects from xls files. Requires the xlrd/xlrt module.
PEP8
"""
import utils as utls
from utils import freezable_factory, TicDatError, verify, containerish, do_it, FrozenDict
import os
from collections import defaultdict
from itertools import product


try:
    import xlrd
    import xlwt
    import_worked=True
except:
    import_worked=False

class XlsTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Excel files with ticDat objects.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A XlsTicDatFactory will
        automatically be associated with the parent TicDatFactory if your system
        has the required xlrd, xlwt packages.
        :param tic_dat_factory:
        :return:
        """
        assert import_worked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, xls_file_path, row_offsets={}):
        """
        Create a TicDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :param row_offsets: (optional) A mapping from table names to initial
                            number of rows to skip
        :return: a TicDat object populated by the matching sheets.
        caveats: Missing sheets resolve to an empty table, but missing fields
                 on matching sheets throw an Exception.
        """
        return self.tic_dat_factory.TicDat(**self._create_tic_dat(xls_file_path, row_offsets))
    def create_frozen_tic_dat(self, xls_file_path, row_offsets={}):
        """
        Create a FrozenTicDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :param row_offsets: (optional) A mapping from table names to initial
                            number of rows to skip
        :return:
        """
        return self.tic_dat_factory.FrozenTicDat(**self._create_tic_dat(xls_file_path, row_offsets))
    def _get_sheets_and_fields(self, xls_file_path, all_tables, row_offsets):
        try :
            book = xlrd.open_workbook(xls_file_path)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xls_file_path, e.message))
        sheets = defaultdict(list)
        for table, sheet in product(all_tables, book.sheets()) :
            if table == sheet.name :
                sheets[table].append(sheet)
        duplicated_sheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicated_sheets, "The following sheet names were duplicated : " +
               ",".join(duplicated_sheets))
        sheets = FrozenDict({k:v[0] for k,v in sheets.items() })
        field_indicies, bad_fields = {}, {}
        for table, sheet in sheets.items() :
            field_indicies[table], bad_fields[table] = self._get_field_indicies(
                                                        table, sheet, row_offsets[table])
        verify(not any(_ for _ in bad_fields.values()),
               "The following field names could not be found : \n" +
               "\n".join("%s : "%t + ",".join(bf) for t,bf in bad_fields.items() if bf))
        return sheets, field_indicies
    def _create_generator_obj(self, xlsFilePath, table, row_offset):
        tdf = self.tic_dat_factory
        def tableObj() :
            sheets, field_indicies = self._get_sheets_and_fields(xlsFilePath,
                                                                 (table,), {table:row_offset})
            if table in sheets :
                sheet = sheets[table]
                table_len = min(len(sheet.col_values(field_indicies[table][field]))
                               for field in tdf.data_fields[table])
                for x in (sheet.row_values(i) for i in range(table_len)[row_offset+1:]) :
                    yield self._sub_tuple(tdf.data_fields[table], field_indicies[table])(x)
        return tableObj

    def _create_tic_dat(self, xls_file_path, row_offsets):
        verify(utls.dictish(row_offsets) and
               set(row_offsets).issubset(self.tic_dat_factory.all_tables) and
               all(utls.numericish(x) and (x>=0) for x in row_offsets.values()),
               "row_offsets needs to map from table names to non negative row offset")
        row_offsets = dict({t:0 for t in self.tic_dat_factory.all_tables}, **row_offsets)
        tdf = self.tic_dat_factory
        rtn = {}
        sheets, fieldIndicies = self._get_sheets_and_fields(xls_file_path,
                                    set(tdf.all_tables).difference(tdf.generator_tables),
                                    row_offsets)
        for table, sheet in sheets.items() :
            fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
            indicies = fieldIndicies[table]
            table_len = min(len(sheet.col_values(indicies[field])) for field in fields)
            if tdf.primary_key_fields.get(table, ()) :
                tableObj = {self._sub_tuple(tdf.primary_key_fields[table], indicies)(x) :
                            self._sub_tuple(tdf.data_fields.get(table, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in
                                        range(table_len)[row_offsets[table]+1:])}
            else :
                tableObj = [self._sub_tuple(tdf.data_fields.get(table, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in
                                        range(table_len)[row_offsets[table]+1:])]
            rtn[table] = tableObj
        for table in tdf.generator_tables :
            rtn[table] = self._create_generator_obj(xls_file_path, table, row_offsets[table])
        return rtn

    def _sub_tuple(self, fields, field_indicies) :
        assert set(fields).issubset(field_indicies)
        def rtn(x) :
            if len(fields) == 1 :
                return x[field_indicies[fields[0]]]
            return tuple(x[field_indicies[field]] for field in fields)
        return rtn

    def _get_field_indicies(self, table, sheet, row_offset) :
        fields = self.tic_dat_factory.primary_key_fields.get(table, ()) + \
                 self.tic_dat_factory.data_fields.get(table, ())
        if sheet.nrows - row_offset <= 0 :
            return {}, fields
        temp_rtn = {field:list() for field in fields}
        for field, (ind, val) in product(fields, enumerate(sheet.row_values(row_offset))) :
            if field == val :
                temp_rtn[field].append(ind)
        rtn = {field : inds[0] for field, inds in temp_rtn.items() if len(inds)==1}
        if len(rtn) == len(fields):
            return rtn, []
        return {}, [field for field, inds in temp_rtn.items() if len(inds)!=1]

    def write_file(self, tic_dat, file_path, allow_overwrite = False):
        """
        write the ticDat data to an excel file
        :param tic_dat: the data object to write (typically a TicDat)
        :param file_path: the file path of the excel file to create
        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?
        :return:
        caveats: None may be written out as an empty string. This reflects the behavior of xlwt.
        """
        tdf = self.tic_dat_factory
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid xls file path")
        verify(allow_overwrite or not os.path.exists(file_path),
               "The %s path exists and overwrite is not allowed"%file_path)
        book = xlwt.Workbook()
        for t in  sorted(sorted(tdf.all_tables),
                         key=lambda x: len(tdf.primary_key_fields.get(x, ()))) :
            sheet = book.add_sheet(t)
            for i,f in enumerate(tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t, ())) :
                sheet.write(0, i, f)
            _t = getattr(tic_dat, t)
            if utls.dictish(_t) :
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


