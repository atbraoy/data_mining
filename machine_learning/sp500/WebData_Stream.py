import urllib
import re


symbolefile =  open("stock_symbols.txt")
symbolslist = symbolefile.read()# ["aapl", "spy", "goog", "nflx"]
                                     #for symbol in symbolslist:
                                     #print symbol
newsymbolslist = symbolslist.split("\n")
i = 0
while i < len(newsymbolslist):
    url = "http://finance.yahoo.com/q?s=" +newsymbolslist[i] + "&type=2button&fr=uh3_finance_web&uhb=uhb2"
    htmlfile = urllib.urlopen(url)
    htmltxt = htmlfile.read()
    #regex = '<span id="yfs_l84_'+ newsymbolslist[i] +'">(.+?)</span>'
    regex = '<span id="yfs_l84_[^.]*">(.+?)</span>'


    pattern = re.compile(regex)
    price = re.findall(pattern, htmltxt)

    print "The price of", newsymbolslist[i], "is", price # to change format of [x.xx] to just x.xx use price[0]
    i+=1
