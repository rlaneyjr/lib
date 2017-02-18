#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

import sys
import argparse
import glob
import json
import operator
from lib2to3.fixer_util import is_list

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description="Process WATER_SYSTEM json...")
    parser.add_argument('-o', '--outputfile', help="Filename for serialization")
    parser.add_argument('-d', '--debug', help="Debug option")
    parser.add_argument('inputfile', help="Path to json file to load")
    args = parser.parse_args()
    
    # read the input file
    if args.debug:
        print "proc_ws_json.py: reading", args.inputfile
        
    try:
        fi = open(args.inputfile, 'r')
    except Exception, e:
        print "proc_ws_json.py: error opening:", e
        
    try:
        lstRecords = json.load(fi)
    except Exception, e:
        print "proc_ws_json.py: error reading:", e
        fi.close()
        
    fi.close()
        
    # create sort key
    for jRecord in lstRecords:
        
        if jRecord['CITY_NAME'] == None:
            jRecord['CITY_NAME'] = "UNKNOWN"
            
        jRecord[u'SP_SORT_KEY'] = jRecord[u'STATE_CODE'] + jRecord[u'CITY_NAME'].replace(u' ', u'_')
    
    # sort records
    lstNew = sorted(lstRecords, key=operator.itemgetter(u'SP_SORT_KEY'))    

    # create output file, if requested
    if args.outputfile:
        try:
            fo = open(args.outputfile, 'wb')
        except Exception, e:                
            fo.close()
            sys.exit(e)

    # construct CSV header
    sHeader = '"SP_SORT_KEY","STATE_CODE","CITY_NAME","PWSID","PWS_NAME","PWS_TYPE_CODE","PWS_ACTIVITY_CODE","PRIMARY_SOURCE_CODE","PHONE_NUMBER","ZIP_CODE","POPULATION_SERVED_COUNT","POP_CAT_11_CODE","POP_CAT_2_CODE","POP_CAT_3_CODE","POP_CAT_4_CODE","POP_CAT_5_CODE","OUTSTANDING_PERFORMER","OUTSTANDING_PERFORM_BEGIN_DATE","SEASON_BEGIN_DATE","SEASON_END_DATE"\n'

    # write it, if requested
    if args.outputfile:  
        try:
            fo.write(sHeader)
        except Exception, e:
            sys.exit(e)
    
    if args.debug:
        print sHeader
    
    # initialize the count
    nRecords = 0
    
    # evaluate & construct the new record
    for jRecord in lstNew:
        
        # filter
        if jRecord['PWS_TYPE_CODE'] != u'CWS':
            # skip it
            continue
        
        if jRecord['PWS_ACTIVITY_CODE'] != u'A':
            # skip it
            continue
        
        # construct new record
        sRecord = '"' + str(jRecord['SP_SORT_KEY']) + '",'
        sRecord = sRecord + '"' + str(jRecord['STATE_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['CITY_NAME']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PWSID']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PWS_NAME']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PWS_TYPE_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PWS_ACTIVITY_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PRIMARY_SOURCE_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['PHONE_NUMBER']) + '",'
        sRecord = sRecord + str(jRecord['ZIP_CODE']) + ','
        sRecord = sRecord + str(jRecord['POPULATION_SERVED_COUNT']) + ','
        sRecord = sRecord + '"' + str(jRecord['POP_CAT_11_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['POP_CAT_2_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['POP_CAT_3_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['POP_CAT_4_CODE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['POP_CAT_5_CODE']) + '",'    
        sRecord = sRecord + '"' + str(jRecord['OUTSTANDING_PERFORMER']) + '",'
        sRecord = sRecord + '"' + str(jRecord['OUTSTANDING_PERFORM_BEGIN_DATE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['SEASON_BEGIN_DATE']) + '",'
        sRecord = sRecord + '"' + str(jRecord['SEASON_END_DATE']) + '"\n'
        
        # increment the count
        nRecords = nRecords + 1
        
        if args.debug:
            print sRecord
        
        # write the new record, if requested
        if args.outputfile:    
            try:
                fo.write(sRecord)
            except Exception, e:
                sys.exit(e)
        
        # end for
     
    # close outputfile, if requested
    if args.outputfile:  
        print "proc_ws_json.py: wrote", nRecords, "records to:", args.outputfile
        fo.close()
    
    # report
    print "proc_ws_json.py: done!"

    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end