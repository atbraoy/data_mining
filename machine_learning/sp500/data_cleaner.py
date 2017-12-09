import pandas as pd
import os, sys
import time
from datetime import datetime
from time import mktime
import re

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import style
style.use("ggplot")#("dark_background")

#-------------------------------
# Importing from other folders, appending the path
sys.path.append('../')
from data.path_caller import path_return


def path_binder_():
    data_folder = "intraQuarter"
    path = path_return()+'/'+data_folder
    
    return path

def path_binder(folder, feature, option):
    if option == True:
        _path = path_return()+'/'+folder+'/'+feature
        # Pulling  out all files in each folder, one by one
        if os.path.exists(_path) == True:
            feature_list = [x[0] for x in os.walk(_path)]
            # for i in feature_list:
            #    print i
            num_features = len(feature_list) 
            
        elif os.path.exists(_path) == False:
                print "path to data files is not found!"
        
        return _path, num_features, feature_list
    elif option == False:
        _path = path_return()+'/'+folder+'/'
        
        return _path

def path_():
    #pass
    data_folder = "intraQuarter"
    feature_0 = '_KeyStats'
    try:
        stat_path, n_items, feature_list = path_binder(data_folder, feature_0, True)
        #print stat_path, items

    except Exception as e:
        print str(e), ":", "Check the path to data folder"
    

def Key_Stats(gather = ["Total Debt/Equity",
                      'Trailing P/E',
                      'Price/Sales',
                      'Price/Book',
                      'Profit Margin',
                      'Operating Margin',
                      'Return on Assets',
                      'Return on Equity',
                      'Revenue Per Share',
                      'Market Cap',
                        'Enterprise Value',
                        'Forward P/E',
                        'PEG Ratio',
                        'Enterprise Value/Revenue',
                        'Enterprise Value/EBITDA',
                        'Revenue',
                        'Gross Profit',
                        'EBITDA',
                        'Net Income Avl to Common ',
                        'Diluted EPS',
                        'Earnings Growth',
                        'Revenue Growth',
                        'Total Cash',
                        'Total Cash Per Share',
                        'Total Debt',
                        'Current Ratio',
                        'Book Value Per Share',
                        'Cash Flow',
                        'Beta',
                        'Held by Insiders',
                        'Held by Institutions',
                        'Shares Short (as of',
                        'Short Ratio',
                        'Short % of Float',
                        'Shares Short (prior ']):
    #pass
    folder = "intraQuarter"
    feature_0 = '_KeyStats'
    stat_path, n_items, stock_list = path_binder(folder, feature_0, True)
    print stat_path

    df = pd.DataFrame(columns = [
                                'Date',
                                 'Unix',
                                 'Ticker',
                                 'Price',
                                 'stock_p_change',
                                 'SP500',
                                 'sp500_p_change',
                                 'Difference',
                                 ##############
                                 'DE Ratio',
                                 'Trailing P/E',
                                 'Price/Sales',
                                 'Price/Book',
                                 'Profit Margin',
                                 'Operating Margin',
                                 'Return on Assets',
                                 'Return on Equity',
                                 'Revenue Per Share',
                                 'Market Cap',
                                 'Enterprise Value',
                                 'Forward P/E',
                                 'PEG Ratio',
                                 'Enterprise Value/Revenue',
                                 'Enterprise Value/EBITDA',
                                 'Revenue',
                                 'Gross Profit',
                                 'EBITDA',
                                 'Net Income Avl to Common ',
                                 'Diluted EPS',
                                 'Earnings Growth',
                                 'Revenue Growth',
                                 'Total Cash',
                                 'Total Cash Per Share',
                                 'Total Debt',
                                 'Current Ratio',
                                 'Book Value Per Share',
                                 'Cash Flow',
                                 'Beta',
                                 'Held by Insiders',
                                 'Held by Institutions',
                                 'Shares Short (as of',
                                 'Short Ratio',
                                 'Short % of Float',
                                 'Shares Short (prior ',                                
                                 ##############
                                 'Status'])
    
    sp500_df = pd.DataFrame.from_csv(path_binder(folder, feature_0, False)+'/'+"SP500.csv")
    ticker_list = []
    
    for _dir in stock_list[1:]:
        _file = os.listdir(_dir)
        ticker = _dir.split('/')[-1]
        ticker_list.append(ticker)
        
        starting_stock_value = False
        starting_sp500_value = False

        if len(_file) > 0:
            for file in _file:
                date_stamp = datetime.strptime(file, '%Y%m%d%H%M%S.html')
                unix_time = time.mktime(date_stamp.timetuple())

                _file_path = _dir+'/'+file
                _file_source = open(_file_path, 'r').read()
                
                try:
                    value_list = []
                    for item in gather:
                        try:
                            regex = re.escape(item) + r'.*?(\d{1,8}\.\d{1,8}M?B?|N/A)%?</td>'
                            value = re.search(regex, _file_source)
                            value = value.group(1)
                            if "B" in value:
                                value = float(value.replace("B", ''))*1000000000
                            elif "M" in value:
                                value = float(value.replace("M", ''))*1000000
                            
                            value_list.append(value)
                        #     value = float(_file_source.split(gather+':</td><td class="yfnc_tabledata1">')[1].split('</td>')[0])
                        # except Exception as e:
                        #     try:
                        #         try:
                        #             value = (_file_source.split(gather+':</td><td class="yfnc_tabledata1">')[1].split('</td>')[0])
                        #             value = re.search(r'(\d{1,8}\.\d{1,8})', value)           
                        #             value = float(value.group(1))
                        #         except:
                        #             try:
                        #                 value = float(_file_source.split(gather+':</td>\n<td class="yfnc_tabledata1">')[1].split('</td>')[0])
                        #             except Exception as a:
                        #                 value = (_file_source.split(gather+':</td>\n<td class="yfnc_tabledata1">')[1].split('</td>')[0])
                        #                 value = re.search(r'(\d{1,8}\.\d{1,8})', value)           
                        #                 value = float(value.group(1))
                        #                 
                        #                 print "error 1", str(a) , ticker, file
                        except Exception as e:
                                value == 'N/A'
                                value_list.append(value)
                                    

                    try:
                        sp500_date = datetime.fromtimstamp(unix_time).strftime('%Y-%m-%d')
                        row = sp500_df[(sp500_df.index == sp500_date)]
                        sp500_value = float(row["SP500"])
                    except:
                        sp500_date = datetime.fromtimestamp(unix_time-259200).strftime('%Y-%m-%d')
                        row = sp500_df[(sp500_df.index == sp500_date)]
                        sp500_value = float(row["SP500"])
                        
                   #---------------------- error start in the segment below 
                    try:
                        stock_price = float(_file_source.split('</small><big><b>')[1].split('</b></big>')[0])
                    except Exception as e:        
                        try:
                            try:
                                stock_price = (_file_source.split('</small><big><b>')[1].split('</b></big>')[0])
                                stock_price = re.search(r'(\d{1,8}\.\d{1,8})', stock_price)           
                                stock_price = float(stock_price.group(1))
                            except:                              
                                try:
                                    # clearing <span id="yfs_l84_aa">12.04</span>
                                    stock_price = float(_file_source.split('<span id="yfs_l10_'+ticker+'">')[1].split('</span>')[0])
                                except Exception as a_:
                                    # clearing <span id="yfs_l10_aa">12.04</span>
                                    stock_price = (_file_source.split('<span id="yfs_l10_'+ticker+'">')[1].split('</span>')[0])
                                    stock_price = re.search(r'(\d{1,8}\.\d{1,8})', stock_price)           
                                    stock_price = float(stock_price.group(1))
                                    
                                    print "error 3", str(a_) , ticker, file

                                
                        except:
                            try:
                                # clearing <span id="yfs_l84_aa">12.04</span>
                                stock_price = float(_file_source.split('<span id="yfs_l84_'+ticker+'">')[1].split('</span>')[0])
                                #print "error (2):", str(b), ticker, file
                            except Exception as b_:
                                try:
                                    try:
                                        stock_price = (_file_source.split('<span id="yfs_l84_'+ticker+'">')[1].split('</span>')[0])
                                        stock_price = re.search(r'(\d{1,8}\.\d{1,8})', stock_price)          
                                        stock_price = float(stock_price.group(1))
                                    except:
                                        stock_price = (source.split('<span class="time_rtq_ticker">')[1].split('</span>')[0])
                                        #print(stock_price)
        
                                        stock_price = re.search(r'(\d{1,8}\.\d{1,8})', stock_price)
                        
                                        stock_price = float(stock_price.group(1))
                                        #print(stock_price)

                                except:
                                    print "error 4", str(b_) , ticker, file
                                    #time.sleep(5)
                    
                    if not starting_stock_value: # means if starting_stock_value == False
                        starting_stock_value = stock_price     
                    if not starting_sp500_value: # means if starting_sp500_value == False
                        starting_sp500_value = sp500_value
                        
                    stock_p_change = ((stock_price - starting_stock_value)/starting_stock_value)*100.0
                    sp500_p_change = ((sp500_value - starting_sp500_value)/starting_sp500_value)*100.0
                    
                    # Labelling the data
                    difference = (stock_p_change - sp500_p_change)
                    if difference > 0:
                        status = "BUY" #"outperform"
                    else:
                        status = "SELL" #"underperform"
                    
                    if value_list.count("N/A") > 0:
                        pass
                    else:
                        try:

                            df = df.append({'Date':date_stamp,
                                            'Unix':unix_time,
                                            'Ticker':ticker,
                                            
                                            'Price':stock_price,
                                            'stock_p_change':stock_p_change,
                                            'SP500':sp500_value,
                                            'sp500_p_change':sp500_p_change,
                                            'Difference':difference,
                                            'DE Ratio':value_list[0],
                                            #'Market Cap':value_list[1],
                                            'Trailing P/E':value_list[1],
                                            'Price/Sales':value_list[2],
                                            'Price/Book':value_list[3],
                                            'Profit Margin':value_list[4],
                                            'Operating Margin':value_list[5],
                                            'Return on Assets':value_list[6],
                                            'Return on Equity':value_list[7],
                                            'Revenue Per Share':value_list[8],
                                            'Market Cap':value_list[9],
                                            'Enterprise Value':value_list[10],
                                            'Forward P/E':value_list[11],
                                            'PEG Ratio':value_list[12],
                                            'Enterprise Value/Revenue':value_list[13],
                                            'Enterprise Value/EBITDA':value_list[14],
                                            'Revenue':value_list[15],
                                            'Gross Profit':value_list[16],
                                            'EBITDA':value_list[17],
                                            'Net Income Avl to Common ':value_list[18],
                                            'Diluted EPS':value_list[19],
                                            'Earnings Growth':value_list[20],
                                            'Revenue Growth':value_list[21],
                                            'Total Cash':value_list[22],
                                            'Total Cash Per Share':value_list[23],
                                            'Total Debt':value_list[24],
                                            'Current Ratio':value_list[25],
                                            'Book Value Per Share':value_list[26],
                                            'Cash Flow':value_list[27],
                                            'Beta':value_list[28],
                                            'Held by Insiders':value_list[29],
                                            'Held by Institutions':value_list[30],
                                            'Shares Short (as of':value_list[31],
                                            'Short Ratio':value_list[32],
                                            'Short % of Float':value_list[33],
                                            'Shares Short (prior ':value_list[34],
                                            'Status':status},
                                           ignore_index=True)

                        except Exception as e:
                            print(str(e),'df creation')
                except Exception as s:
                    pass
                    print "error (global): ", str(s) , ticker, file
                #print  ticker, ': ', value, '--', file
    # for _ticker in ticker_list:
    #     try:
    #         plot_df = df[(df["Ticker"] == _ticker)]
    #         plot_df = plot_df.set_index(["Date"])
    #         if plot_df['Status'][-1] == "SELL": #"underperform":
    #             color = 'r'
    #         else:
    #             color = 'g'
    #         plot_df["Difference"].plot(label = _ticker, color=color)
    #         #plt.legend()
    #     except:
    #         print str(e)
    # plt.show()
        
    save = "key_stats.csv"
    #gather.replace(' ', '').replace(')','').replace('(','').replace('/','')+'.csv'
    df.to_csv(save)
    print "Done, data saved at:", save
    #time.sleep(10)

    
Key_Stats()
path_()
print "Done!"





