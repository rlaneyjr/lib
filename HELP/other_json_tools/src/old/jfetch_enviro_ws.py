#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

# to do: mmake sure this outputs valid JSON array, e.g. [ { record1 }, { record2 } ... ]

import sys
import argparse
import json
import requests
import time
import xmltodict

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description='Fetch json from EPA SDWIS, optionally retrieving all rows')
    parser.add_argument('-r', '--row', default=1, help="first row to retrieve")
    parser.add_argument('-o', '--outputfile', help="filename for serialization")
    parser.add_argument('uri', help="uri to the web service, without row specification")
    args = parser.parse_args()
    
    # get row count
    nCount = 0
    url = args.uri + 'COUNT/'
    res = requests.get(url)
    if(res.ok):
        # response is in xml format... ick...
        xDoc = xmltodict.parse(res.content)
        nCount = int(xDoc[u'Envirofacts'][u'RequestRecordCount'])
    else:
        res.raise_for_status()
        # someone should check this
        sys.exit()    
    res.close()
    
    # initialize
    nRecords = 0
    xPAGE = 99
    xSLEEP = 20
    nCurrent = int(args.row)

    # calculate number of pages, add one for any remainder
    nPages = nCount / xPAGE + 1
    
    print "Requesting", nPages
    
    # for each page:
    for nPage in range(0,nPages):
        
        # construct the uri
        # service format: https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM/STATE_CODE/MA/       
        
        if nCurrent + xPAGE + 1 > nCount:
            url = args.uri + 'JSON/ROWS/' + str(nCurrent) + ':' + str(nCurrent + (nCount - nCurrent))
        else:
            url = args.uri + 'JSON/ROWS/' + str(nCurrent) + ':' + str(nCurrent + xPAGE)
        
        print "jfetch.py: requesting page:", nPage, "->", url
                 
        # request it
        res = requests.get(url)
        if(res.ok):
            jData = json.loads(res.content)
                 
            # if requested, write the file out
            if args.outputfile:
                try:
                    f = open(args.outputfile, 'ab')
                except Exception, e:                
                    f.close()
                    sys.exit(e)
         
                print "jfetch.py: updating:", args.outputfile
                 
                try:
                    json.dump(jData, f, sort_keys=True, indent=4, separators=(',', ': '))
                except Exception, e:
                    sys.exit(e)
                f.close()
        else:
            res.raise_for_status()
             
        # done reading
        res.close()
            
        # update counts
        nRecords = nRecords + (xPAGE + 1)
        
        # start next page
        nCurrent = nCurrent + xPAGE + 1 # new start
        
        # pause to be nice
        time.sleep(xSLEEP)
        
    # end for

    print "jfetch.py: done!"

    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end