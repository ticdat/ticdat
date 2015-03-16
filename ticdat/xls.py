"""
Read/write ticDat objects from xls files. Requires the xlrd/xlrt module
"""
import utils as utls
from utils import freezableFactory, TicDatError, verify, containerish, doIt
import os
from collections import defaultdict
from itertools import product

try:
    import xlrd
    import xlwt
    importWorked=True
except:
    importWorked=False

class XlsTicFactory(freezableFactory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Excel files with ticDat objects.
    """
    def __init__(self, ticDatFactory):
        """
        Don't call this function explicitly. A XlsTicDatFactory will automatically be associated with the parent
        TicDatFactory if your system has the required xlrd, xlwt packages.
        :param ticDatFactory:
        :return:
        """
        assert importWorked, "don't create this otherwise"
        self.tic_dat_factory = ticDatFactory
        self._isFrozen = True
    def create_tic_dat(self, xlsFilePath):
        """
        Create a TicDat object from an Excel file
        :param xlsFilePath: An Excel file containing sheets whose names match the table names in the schema.
        :return: a TicDat object populated by the matching sheets.
        """
        return self.tic_dat_factory.TicDat(**self._createTicDat(xlsFilePath))
    def create_frozen_tic_dat(self, xlsFilePath):
        """
        Create a FrozenTicDat object from an Excel file
        :param xlsFilePath: An Excel file containing sheets whose names match the table names in the schema.
        :return: a TicDat object populated by the matching sheets.
        """
        return self.tic_dat_factory.FrozenTicDat(**self._createTicDat(xlsFilePath))
    def _getSheetsAndFields(self, xlsFilePath, allTables):
        try :
            book = xlrd.open_workbook(xlsFilePath)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xlsFilePath, e.message))
        sheets = defaultdict(list)
        for table, sheet in product(allTables, book.sheets()) :
            if table == sheet.name :
                sheets[table].append(sheet)
        duplicatedSheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicatedSheets, "The following sheet names were duplicated : " + ",".join(duplicatedSheets))
        sheets = utls.FrozenDict({k:v[0] for k,v in sheets.items() })
        fieldIndicies, badFields = {}, defaultdict(list)
        for table, sheet in sheets.items() :
            fieldIndicies[table] = self._getFieldIndicies(table, sheet, badFields[table] )
        verify(not any(_ for _ in badFields.values()), "The following field names could not be found : \n" +
               "\n".join("%s : "%t + ",".join(bf) for t,bf in badFields.items() if bf))
        return sheets, fieldIndicies
    def _createGeneratorObj(self, xlsFilePath, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            sheets, fieldIndicies = self._getSheetsAndFields(xlsFilePath, (table,))
            if table in sheets :
                sheet = sheets[table]
                tableLen = min(len(sheet.col_values(fieldIndicies[table][field]))
                               for field in tdf.data_fields[table])
                for x in (sheet.row_values(i) for i in range(tableLen)[1:]) :
                    yield self._subTuple(tdf.data_fields[table], fieldIndicies[table])(x)
        return tableObj

    def _createTicDat(self, xlsFilePath):
        tdf = self.tic_dat_factory
        rtn = {}
        sheets, fieldIndicies = self._getSheetsAndFields(xlsFilePath,
                                    set(tdf.all_tables).difference(tdf.generator_tables))
        for table, sheet in sheets.items() :
            fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
            indicies = fieldIndicies[table]
            tableLen = min(len(sheet.col_values(indicies[field])) for field in fields)
            if tdf.primary_key_fields.get(table, ()) :
                tableObj = {self._subTuple(tdf.primary_key_fields[table], indicies)(x) :
                            self._subTuple(tdf.data_fields.get(table, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in range(tableLen)[1:])}
            else :
                tableObj = [self._subTuple(tdf.data_fields.get(table, ()), indicies)(x)
                            for x in (sheet.row_values(i) for i in range(tableLen)[1:])]
            rtn[table] = tableObj
        for table in tdf.generator_tables :
            rtn[table] = self._createGeneratorObj(xlsFilePath, table)
        return rtn

    def _subTuple(self, fields, fieldIndicies) :
        assert set(fields).issubset(fieldIndicies)
        def rtn(x) :
            if len(fields) == 1 :
                return x[fieldIndicies[fields[0]]]
            return tuple(x[fieldIndicies[field]] for field in fields)
        return rtn

    def _getFieldIndicies(self, table, sheet, badFieldsRtn = None) :
        fields = self.tic_dat_factory.primary_key_fields.get(table, ()) + self.tic_dat_factory.data_fields.get(table, ())
        if not sheet.nrows :
            doIt(badFieldsRtn.append(x) for x in fields)
            return None
        badFieldsRtn = badFieldsRtn if badFieldsRtn is not None else list()
        assert hasattr(badFieldsRtn, "append")
        tempRtn = {field:list() for field in fields}
        for field, (ind, val) in product(fields, enumerate(sheet.row_values(0))) :
            if field == val :
                tempRtn[field].append(ind)
        rtn = {field : inds[0] for field, inds in tempRtn.items() if len(inds)==1}
        doIt(badFieldsRtn.append(field) for field, inds in tempRtn.items() if len(inds)!=1)
        return rtn if len(rtn) == len(fields) else None

    def write_file(self, ticDat, xlsFilePath, allow_overwrite = False):
        """
        write the ticDat data to an excel file
        :param ticDat: the data object to write
        :param xlsFilePath: the file path of the excel file to create
        :param allow_overwrite: boolean - are we allowed to overwrite an existing file?
        :return:
        """
        tdf = self.tic_dat_factory
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(ticDat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(xlsFilePath), "A directory is not a valid xls file path")
        verify(allow_overwrite or not os.path.exists(xlsFilePath),
               "The %s path exists and overwrite is not allowed"%xlsFilePath)
        book = xlwt.Workbook()
        for t in  sorted(sorted(tdf.all_tables), key=lambda x: len(tdf.primary_key_fields.get(x, ()))) :
            sheet = book.add_sheet(t)
            for i,f in enumerate(tdf.primary_key_fields.get(t,()) + tdf.data_fields.get(t, ())) :
                sheet.write(0, i, f)
            _t = getattr(ticDat, t)
            if utls.dictish(_t) :
                for rowInd, (primaryKey, dataRow) in enumerate(_t.items()) :
                    for fieldInd, cellValue in enumerate( (primaryKey if containerish(primaryKey) else (primaryKey,)) +
                                        tuple(dataRow[_f] for _f in tdf.data_fields.get(t, ()))):
                        sheet.write(rowInd+1, fieldInd, cellValue)
            else :
                for rowInd, dataRow in enumerate(_t if containerish(_t) else _t()) :
                    for fieldInd, cellValue in enumerate(tuple(dataRow[_f] for _f in tdf.data_fields[t])) :
                        sheet.write(rowInd+1, fieldInd, cellValue)
        if os.path.exists(xlsFilePath):
            os.remove(xlsFilePath)
        book.save(xlsFilePath)


