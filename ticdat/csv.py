"""
Read/write ticDat objects from/to csv files. Requires csv module (which is typically standard)
"""

try:
    import csv
    _importWorked=True
except:
    _importWorked=False

