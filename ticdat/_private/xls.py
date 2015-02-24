"""
Read/write ticDat objects from xls files. Requires the xlrd/xlrt module
"""
import ticdat._private.utils as utls
from ticdat._private.utils import freezableFactory, TicDatError, verify, containerish, doIt
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
    def __init__(self, ticDatFactory):
        assert importWorked, "don't create this otherwise"
        self.ticDatFactory = ticDatFactory
        assert not set(ticDatFactory.dataFields).difference(ticDatFactory.primaryKeyFields), \
            "not expecting tables with no primary key fields"
        self._isFrozen = True
    def createTicDat(self, xlsFilePath):
        return self.ticDatFactory.TicDat(**self._createTicDat(xlsFilePath))
    def createFrozenTicDat(self, xlsFilePath):
        return self.ticDatFactory.FrozenTicDat(**self._createTicDat(xlsFilePath))
    def _createTicDat(self, xlsFilePath):
        tdf = self.ticDatFactory
        try :
            book = xlrd.open_workbook(xlsFilePath)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xlsFilePath, e.message))
        sheets = defaultdict(list)
        for table, sheet in product(tdf.primaryKeyFields, book.sheets()) :
            if table == sheet.name :
                sheets[table].append(sheet)
        missingSheets = set(tdf.primaryKeyFields).difference(sheets)
        verify(not missingSheets, "The following sheet names could not be found : " + ",".join(missingSheets))
        duplicatedSheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicatedSheets, "The following sheet names were duplicated : " + ",".join(duplicatedSheets))
        sheets = utls.FrozenDict({k:v[0] for k,v in sheets.items() })
        fieldIndicies, badFields = {}, defaultdict(list)
        for table, sheet in sheets.items() :
            fieldIndicies[table] = self._getFieldIndicies(table, sheet, badFields[table] )
        verify(not any(_ for _ in badFields.values()), "The following field names could not be found : \n" +
               "\n".join("%s : "%t + ",".join(bf) for t,bf in badFields.items() if bf))
        rtn = {}
        for table, sheet in sheets.items() :
            fields = tdf.primaryKeyFields[table] + tdf.dataFields.get(table, ())
            indicies = fieldIndicies[table]
            tableLen = min(len(sheet.col_values(indicies[field])) for field in fields)
            tableDict = {self._subTuple(tdf.primaryKeyFields[table], indicies)(x) :
                         self._subTuple(tdf.dataFields.get(table, ()), indicies)(x)
                         for x in (sheet.row_values(i) for i in range(tableLen)[1:])}
            rtn[table] = tableDict
        return rtn

    def _subTuple(self, fields, fieldIndicies) :
        assert set(fields).issubset(fieldIndicies)
        def rtn(x) :
            if len(fields) == 1 :
                return x[fieldIndicies[fields[0]]]
            return tuple(x[fieldIndicies[field]] for field in fields)
        return rtn

    def _getFieldIndicies(self, table, sheet, badFieldsRtn = None) :
        fields = self.ticDatFactory.primaryKeyFields[table] + self.ticDatFactory.dataFields.get(table, ())
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

    def writeFile(self, ticDat, xlsFilePath, allowOverwrite = True):
        tdf = self.ticDatFactory
        msg = []
        if not self.ticDatFactory.goodTicDatObject(ticDat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(xlsFilePath), "A directory is not a valid xls file path")
        verify(allowOverwrite or not os.path.exists(xlsFilePath),
               "The %s path exists and overwrite is not allowed"%xlsFilePath)
        book = xlwt.Workbook()
        for t in  sorted(sorted(tdf.primaryKeyFields), key=lambda x: len(tdf.primaryKeyFields[x])) :
            sheet = book.add_sheet(t)
            for i,f in enumerate(tdf.primaryKeyFields[t] + tdf.dataFields.get(t, ())) :
                sheet.write(0, i, f)
            for rowInd, (primaryKey, dataRow) in enumerate(getattr(ticDat, t).items()) :
                for fieldInd, cellValue in enumerate( (primaryKey if containerish(primaryKey) else (primaryKey,)) +
                                        tuple(dataRow[_f] for _f in tdf.dataFields.get(t, ()))):
                    sheet.write(rowInd+1, fieldInd, cellValue)
        if os.path.exists(xlsFilePath):
            os.remove(xlsFilePath)
        book.save(xlsFilePath)


