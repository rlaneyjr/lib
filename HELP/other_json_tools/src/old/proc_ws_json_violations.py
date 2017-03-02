#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

import sys
import argparse
import json
import operator
import requests
import time

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description="Process WATER_SYSTEM json into CSV files")
    parser.add_argument('-o', '--outputfile', help="Filename for output CSV")
    parser.add_argument('-v', '--violationfile', help="Filename for violations")
    parser.add_argument('-e', '--enforcementfile', help="Filename for enforcements")
    parser.add_argument('-v', '--violationfile', help="Filename for violations")
    parser.add_argument('inputfile', help="Path to json file to load")
    args = parser.parse_args()
    
    xSLEEP = 10
    
    ##########
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

    ##########
    # open/create violations CSV output file, if requested
    if args.violationfile:
        try:
            fv = open(args.violationfile, 'ab')
        except Exception, e:                
            fv.close()
            sys.exit(e)
    # fv now open

    # construct CSV header for violations
    sHeader = '"PWSID","VIOLATION_CODE","VIOLATION_CATEGORY_CODE","IS_HEALTH_BASED_IND","CONTAMINANT_CODE","COMPLIANCE_STATUS_CODE","IS_MAJOR_VIOL_IND","SEVERITY_IND_CNT","PUBLIC_NOTIFICATION_TIER","COMPL_PER_BEGIN_DATE","COMPL_PER_END_DATE"\n'
    
    # write violation header, if requested
    if args.violationfile:  
        try:
            fv.write(sHeader)
        except Exception, e:
            sys.exit(e)
        fv.close()

    ##########
    # open/create enforcement CSV output file, if requested
    if args.enforcementfile:
        try:
            fe = open(args.enforcementfile, 'ab')
        except Exception, e:                
            fe.close()
            sys.exit(e)
    # fv now open

    # construct CSV header for enforcement
    sHeader = '"PWSID","VIOLATION_CODE","VIOLATION_CATEGORY_CODE","IS_HEALTH_BASED_IND","CONTAMINANT_CODE","COMPLIANCE_STATUS_CODE","IS_MAJOR_VIOL_IND","SEVERITY_IND_CNT","PUBLIC_NOTIFICATION_TIER","COMPL_PER_BEGIN_DATE","COMPL_PER_END_DATE"\n'
    
    # write enforcement header, if requested
    if args.enforcementfile:  
        try:
            fe.write(sHeader)
        except Exception, e:
            sys.exit(e)
        fv.close()
    
    ##########    
    # create sort key
    for jRecord in lstRecords:
        
        if jRecord['CITY_NAME'] == None:
            jRecord['CITY_NAME'] = "UNKNOWN"
            
        jRecord[u'SP_SORT_KEY'] = jRecord[u'STATE_CODE'] + jRecord[u'CITY_NAME'].replace(u' ', u'_')

    ##########    
    # sort records
    
    lstNew = sorted(lstRecords, key=operator.itemgetter(u'SP_SORT_KEY'))    

    # create/open water system CSV output file, if requested
    if args.outputfile:
        try:
            fo = open(args.outputfile, 'wb')
        except Exception, e:                
            fo.close()
            sys.exit(e)

    # construct CSV header for water system info
    sHeader = '"SP_SORT_KEY","STATE_CODE","CITY_NAME","PWSID","PWS_NAME","PWS_TYPE_CODE","PWS_ACTIVITY_CODE","PRIMARY_SOURCE_CODE","PHONE_NUMBER","ZIP_CODE","POPULATION_SERVED_COUNT","POP_CAT_11_CODE","POP_CAT_2_CODE","POP_CAT_3_CODE","POP_CAT_4_CODE","POP_CAT_5_CODE","OUTSTANDING_PERFORMER","OUTSTANDING_PERFORM_BEGIN_DATE","SEASON_BEGIN_DATE","SEASON_END_DATE"\n'

    # write header, if requested
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
        
        ##########
        # write the new record, if requested
        if args.outputfile:    
            try:
                fo.write(sRecord)
            except Exception, e:
                sys.exit(e)
        
        ##########
        # now pull the violations for each PWSID, if requested, and write to different file

        if args.violationfile:
            
            try:
                fv = open(args.violationfile, 'ab')
            except Exception, e:                
                fv.close()
                sys.exit(e)
            # fv now open

            # construct the uri
            # service: 
            url = "https://iaspub.epa.gov/enviro/efservice/VIOLATION/PWSID/" + jRecord['PWSID'] + "/JSON"
            if args.debug:
                print "jfetch.py: requesting violations:", url
            res = requests.get(url)
            if(res.ok):
                jData = json.loads(res.content)
                nViolations = 0
                for jRow in jData:
                    nViolations = nViolations + 1
                    # assemble into a new structure
                    sViolation = '"' + str(jRecord['PWSID']) + '",'
                    sViolation = sViolation + str(jRow['VIOLATION_CODE']) + ','
                    sViolation = sViolation + '"' + str(jRow['VIOLATION_CATEGORY_CODE']) + '",'
                    sViolation = sViolation + '"' + str(jRow['IS_HEALTH_BASED_IND']) + '",'
                    sViolation = sViolation + str(jRow['CONTAMINANT_CODE']) + ','
                    sViolation = sViolation + '"' + str(jRow['COMPLIANCE_STATUS_CODE']) + '",'
                    sViolation = sViolation + '"' + str(jRow['IS_MAJOR_VIOL_IND']) + '",'
                    sViolation = sViolation + '"' + str(jRow['SEVERITY_IND_CNT']) + '",'
                    sViolation = sViolation + '"' + str(jRow['PUBLIC_NOTIFICATION_TIER']) + '",'
                    sViolation = sViolation + '"' + str(jRow['COMPL_PER_BEGIN_DATE']) + '",'
                    sViolation = sViolation + '"' + str(jRow['COMPL_PER_END_DATE']) + '"\n'
                    # write the new record, if requested
                    try:
                        fv.write(sViolation)
                    except Exception, e:
                        sys.exit(e)
                # end for
            else:
                res.raise_for_status()
                
            # close the request
            res.close()
            
            # close the violations file
            fv.close()
            
            # sleep for a bit since we made a request to the web service
            time.sleep(xSLEEP)
            
        ##########
        # now pull the enforcements for each PWSID, if requested, and write to different file

        if args.enforcementfile:
            
            try:
                fe = open(args.enforcementfile, 'ab')
            except Exception, e:                
                fe.close()
                sys.exit(e)
            # fv now open

            # construct the uri
            # service: 
            url = "https://iaspub.epa.gov/enviro/efservice/ENFORCEMENT_ACTION/PWSID/" + jRecord['PWSID'] + "/JSON"
            if args.debug:
                print "jfetch.py: requesting enforcements:", url
            res = requests.get(url)
            if(res.ok):
                jData = json.loads(res.content)
                nEnforcements = 0
                for jRow in jData:
                    nEnforcements = nEnforcements + 1
                    # assemble into a new structure
                    sEnforcement = '"' + str(jRecord['PWSID']) + '",'
                    sEnforcement = sEnforcement + '"' + str(jRow['ENFORCEMENT_DATE']) + '",'
                    sEnforcement = sEnforcement + '"' + str(jRow['ENFORCEMENT_ACTION_TYPE_CODE']) + '",'
                    sEnforcement = sEnforcement + '"' + str(jRow['ENFORCEMENT_COMMENT_TEXT']) + '"\n'
                    # write the new record, if requested
                    try:
                        fv.write(sEnforcement)
                    except Exception, e:
                        sys.exit(e)
                # end for
            else:
                res.raise_for_status()
                
            # close the request
            res.close()
            
            # close the violations file
            fe.close()
            
            # sleep for a bit since we made a request to the web service
            time.sleep(xSLEEP)

    # end for (each record)
     
    # report!!
        
    # close outputfile, if requested
    if args.outputfile:  
        print "proc_ws_json.py: wrote", nRecords, "records to:", args.outputfile
        fo.close()

    if args.violationfile:
        print "proc_ws_json.py: wrote", nViolations, "records to:", args.violationfile
    
    if args.enforcementfile:
        print "proc_ws_json.py: wrote", nEnforcements, "records to:", args.enforcementfile

    print "proc_ws_json.py: done!"

    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end