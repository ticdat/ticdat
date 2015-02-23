"""
Read/write ticDat objects from xls files. Requires the xlrd/xlrt module
"""

try:
    import xlrd
    import xlwt
    _importWorked=True
except:
    _importWorked=False

