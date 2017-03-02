#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

#############################################    
# fetch SDWIS data for a given state code, in json format, and write it out as json
# example usage: python jfetch_sdwis_json.py -o WS_MA_ALL.json https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM/STATE_CODE/MA/

# to do: make sure this produces valid JSON, e.g. [ {record1}, {record2], ... {recordN} ]
# to do: use json to get count

#############################################    

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
    parser.add_argument('-o', '--outputfile', help="filename to write results to, in json format")
    parser.add_argument('-d', '--debug', help="provide debug information")
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
    xSLEEP = 2
    nCurrent = int(args.row)

    # calculate number of pages, add one for any remainder
    nPages = nCount / xPAGE + 1
    
    print "jfetch_sdwis_json.py: requesting", nPages
    
    # for each page:
    for nPage in range(0,nPages):
        
        # construct the uri
        # service format: https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM/STATE_CODE/MA/       
        
        if nCurrent + xPAGE + 1 > nCount:
            url = args.uri + 'JSON/ROWS/' + str(nCurrent) + ':' + str(nCurrent + (nCount - nCurrent))
        else:
            url = args.uri + 'JSON/ROWS/' + str(nCurrent) + ':' + str(nCurrent + xPAGE)
        
        print "jfetch_sdwis_json.py: requesting page:", nPage, "->", url
        
         
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
                print "jfetch_sdwis_json.py: updating l1:", args.outputfile
                try:
                    json.dump(jData, f, sort_keys=True, indent=4, separators=(',', ': '))
                except Exception, e:
                    sys.exit(e)
                f.close()
            else:
                time.sleep(120)
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
                        print "jfetch_sdwis_json.py: updating l2:", args.outputfile
                        try:
                            json.dump(jData, f, sort_keys=True, indent=4, separators=(',', ': '))
                        except Exception, e:
                            sys.exit(e)
                        f.close()
                else:
                    time.sleep(500)
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
                            print "jfetch_sdwis_json.py: updating l3:", args.outputfile
                            try:
                                json.dump(jData, f, sort_keys=True, indent=4, separators=(',', ': '))
                            except Exception, e:
                                sys.exit(e)
                            f.close()
                    else:
                        # ok, enough
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

    print "jfetch_sdwis_json.py: done!"

    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end