# Running this script will generate the .xlsx files and thus allow you to reproduce the
# results in beer_promotion.ipynb

from ticdat import TicDatFactory
tdfHist = TicDatFactory(data =[[],
                    ['Product','Sales','Cost Per Unit','Easter Included','Carnival Included',
                     'Christmas Included', 'Some Other Holiday?', '4 Wk Avg Temp',
                     '4 Wk Avg Humidity', 'Sales M-1 weeks', 'Sales M-2 weeks', 'Sales M-3 weeks',
                     'Sales M-4 Weeks', 'Sales M-5 weeks']])

histDat = tdfHist.json.create_tic_dat("beer_sales_historical_data.json")
tdfHist.xls.write_file(histDat, "beer_sales_historical_data.xlsx")

tdfCarn = TicDatFactory(data = [[],
                    ['Product','Cost Per Unit','Easter Included','Carnival Included',
                     'Christmas Included', 'Some Other Holiday?', '4 Wk Avg Temp', '4 Wk Avg Humidity',
                     'Sales M-1 weeks', 'Sales M-2 weeks', 'Sales M-3 weeks', 'Sales M-4 Weeks',
                     'Sales M-5 weeks']])

carnDat = tdfCarn.json.create_tic_dat("carnivale_promotion_data.json")
tdfCarn.xls.write_file(carnDat, "carnivale_promotion_data.xlsx")