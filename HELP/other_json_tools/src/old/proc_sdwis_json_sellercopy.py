#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

#############################################    
# sort and enrich SDWIS json, then write it out as json
# example usage: python proc_sdwis_json.py -o WS_MA_KYW.json MS_WA_ALL.json

import sys
import argparse
import json
import operator
import requests
import time
from sdwis.sdwis import dictSDWISObjectives, dictSDWISTreatments

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description="Process SDWIS json into KYW data model and write out as json")
    parser.add_argument('-o', '--outputfile', help="filename to write processed output to, in json format")
    parser.add_argument('-d', '--debug', action='store_true', help="provide debug information")
    parser.add_argument('-r', '--resume', help="SORT_KEY to start with")
    parser.add_argument('inputfile', help="path to input file in json format")

    args = parser.parse_args()
    
    xSLEEP = 5
    
    #############################################    
    # read the input file
    
    print "proc_sdwis_json.py: reading", args.inputfile
        
    try:
        fi = open(args.inputfile, 'r')
    except Exception, e:
        print "proc_sdwis_json.py: error opening:", e
        sys.exit(e)
        
    try:
        lstInput = json.load(fi)
    except Exception, e:
        print "proc_sdwis_json.py: error reading:", e
        fi.close()
        
    fi.close()
    
    ##########    
    # create sort key
    for jRecord in lstInput:
        
        if jRecord['CITY_NAME'] == None:
            jRecord['CITY_NAME'] = "UNKNOWN"
            if args.debug:
                print "proc_sdwis_json.py: missing CITY_NAME for", jRecord['PWSID'], "assuming UNKNOWN"
            
        jRecord[u'SP_SORT_KEY'] = jRecord[u'STATE_CODE'] + jRecord[u'CITY_NAME'].replace(u' ', u'_')

    #############################################    
    # sort records
    lstSorted = sorted(lstInput, key=operator.itemgetter(u'SP_SORT_KEY'))    

    #############################################    
    # process records
        
    # initialize
    nRecords = 0
    lstNew = []
    url = ""
    dictWSFCache = {}
    bSkip = False
    if args.resume:
        bSkip = True

    # evaluate & construct the new record
    for jRecord in lstSorted:
        
        ##########
        # skip
        
        if bSkip:
            if jRecord['PWSID'] == args.resume:
                bSkip = False
            else:
                if args.debug:
                    print "proc_sdwis_json.py: skipping", jRecord['PWSID']
                continue

        #############################################    
        # filter
        
        if jRecord['PWS_TYPE_CODE'] != u'CWS':
            # skip it
            continue
        
        if jRecord['PWS_ACTIVITY_CODE'] != u'A':
            # skip it
            continue
        
        #############################################    
        # process into KYW data model
        
        sKYWRecord = {}
        sKYWRecord['PWSID'] = str(jRecord['PWSID'])
        sKYWRecord['PWS_NAME'] = str(jRecord['PWS_NAME'])
        sKYWRecord['SORT_KEY'] = jRecord['SP_SORT_KEY']
        sKYWRecord['SOURCE_URI'] = "https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM/STATE_CODE/MA/"
        # to do: make sure SOURCE_URI  is passed into the input JSON
        sKYWRecord['SOURCE_LAST_UPDATED'] = "2016-07-13T16:00:00"
        sKYWRecord['STATE_CODE'] = jRecord['STATE_CODE']
        sKYWRecord['COUNTY_NAME'] = "TBA"
        sKYWRecord['CITY_NAME'] = jRecord['CITY_NAME']
        sKYWRecord['ZIP_CODE'] = str(jRecord['ZIP_CODE'])
        sKYWRecord['POPULATION_SERVED'] = jRecord['POPULATION_SERVED_COUNT']
        sKYWRecord['WATER_SOURCE_TYPE'] = str(jRecord['PRIMARY_SOURCE_CODE'])
        sKYWRecord['IS_SCHOOL'] = str(jRecord['IS_SCHOOL_OR_DAYCARE_IND'])
        sKYWRecord['OUTSTANDING_PERFORMER'] = jRecord['OUTSTANDING_PERFORM_BEGIN_DATE']
        # to do: should we add an end date?
        
        # org info
        sKYWRecord['ORG_NAME'] = str(jRecord['ORG_NAME'])
        sKYWRecord['ORG_ADDRESS_LINE1'] = jRecord['ADDRESS_LINE1']
        sKYWRecord['ORG_ADDRESS_LINE2'] = jRecord['ADDRESS_LINE2']
        sKYWRecord['ORG_PHONE_NUMBER'] = str(jRecord['PHONE_NUMBER'])
        sKYWRecord['ORG_URI'] = 'http://localhost/kyw/stub1'
        sKYWRecord['ORG_CCR_REPORT_URI'] = 'http://localhost/kyw/stub2'
 
        #############################################    
        # WATER_SYSTEM_FACILITY
        
        # default
        sKYWRecord['SOURCE_PURCHASED_PWSID'] = ""
        sKYWRecord['SOURCE_RESERVOIR_INFO'] = []
        sKYWRecord['SOURCE_TREATMENTPLANT_INFO'] = []
        
        # note: don't forget the /JSON...
        url = "https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM_FACILITY/FACILITY_ACTIVITY_CODE/A/PWSID/" + sKYWRecord['PWSID'] + '/JSON/'
        
        if args.debug:
            print "proc_sdwis_json.py: getting WATER_SYSTEM_FACILITY data for", sKYWRecord['PWSID']
            print url
        
        # to do: codes other than CC may be required here
        # to do: for CC, RS, TP and possibly others check if there is a SELLER_PWSID; if there is, get SOURCE_RESERVOIR_INFO and SOURCE_TREATMENTPLAN_INFO from that PWSID
        # note: we already use SELLER_PWSID for TREATMENTS, below
        
        res = requests.get(url)
        if(res.ok):
            jData = json.loads(res.content)
            
            for jItem in jData:
                # purchased water
                if jItem['FACILITY_TYPE_CODE'] in ["CC", "NP"]:
                    sKYWRecord['SOURCE_PURCHASED_PWSID'] = jItem['SELLER_PWSID']
                    # to do: what to do if there are multiples?!?
            # end for
            
            if sKYWRecord['SOURCE_PURCHASED_PWSID'] == "":
                # no purchased water, continue extracting
                
                for jItem in jData:
                    # "reservoir(s)"
                    # for details, refer to page 59 of this: http://www.exchangenetwork.net/schema/SDWA/2/ListValidationsByElement.pdf
                    if jItem['FACILITY_TYPE_CODE'] in [ "IG", "IN", "RC", "RS", "SP", "WL", "NN" ]:
                        sKYWRecord['SOURCE_RESERVOIR_INFO'].append(str(jItem['FACILITY_NAME']) + "|" + str(jItem['STATE_FACILITY_ID']))
                
                    # treatment plants
                    if jItem['FACILITY_TYPE_CODE'] == "TP":
                        sKYWRecord['SOURCE_TREATMENTPLANT_INFO'].append(str(jItem['FACILITY_NAME']) + "|" + str(jItem['STATE_FACILITY_ID']))
                        
                # end for
            
        else:
            res.raise_for_status()
             
        # done reading res
        res.close()
        
        # pause to be nice
        time.sleep(xSLEEP)
        
        # if there is purchased water, re-do the above
        # cache results, since this may be common
        
        if sKYWRecord['SOURCE_PURCHASED_PWSID'] != "":
            
            # re-do the above for the purchased PWSID
            
            # check and see if we have this PWSID cached
            if dictWSFCache.has_key(sKYWRecord['SOURCE_PURCHASED_PWSID']):
                # retrieve from cache
                if args.debug:
                    print "proc_sdwis_json.py: WSFCacheHit:", sKYWRecord['SOURCE_PURCHASED_PWSID']
                jData = dictWSFCache[sKYWRecord['SOURCE_PURCHASED_PWSID']]
            else:
                # fetch it
                # note: don't forget the /JSON...
                url = "https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM_FACILITY/FACILITY_ACTIVITY_CODE/A/PWSID/" + sKYWRecord['SOURCE_PURCHASED_PWSID'] + '/JSON/'
                
                if args.debug:
                    print "proc_sdwis_json.py: getting WATER_SYSTEM_FACILITY data for", sKYWRecord['SOURCE_PURCHASED_PWSID']
                    print url
                
                # to do: codes other than CC may be required here
                # to do: for CC, RS, TP and possibly others check if there is a SELLER_PWSID; if there is, get SOURCE_RESERVOIR_INFO and SOURCE_TREATMENTPLAN_INFO from that PWSID
                # note: we already use SELLER_PWSID for TREATMENTS, below
                
                res = requests.get(url)
                if(res.ok):
                    jData = json.loads(res.content)
                    dictWSFCache[sKYWRecord['SOURCE_PURCHASED_PWSID']] = jData
                else:
                    res.raise_for_status()
                     
                # done reading res
                res.close()
                
                # pause to be nice
                time.sleep(xSLEEP)
            # end if
            
            # extract reservoir and treatment plant info from the SOURCE_PURCHASED_PWSID
            for jItem in jData:
                # reservoir(s)
                if jItem['FACILITY_TYPE_CODE'] in [ "IG", "IN", "RC", "RS", "SP", "WL", "NN" ]:
                    sKYWRecord['SOURCE_RESERVOIR_INFO'].append(str(jItem['FACILITY_NAME']) + "|" + str(jItem['STATE_FACILITY_ID']))
            
                # treatment plants
                if jItem['FACILITY_TYPE_CODE'] == "TP":
                    sKYWRecord['SOURCE_TREATMENTPLANT_INFO'].append(str(jItem['FACILITY_NAME']) + "|" + str(jItem['STATE_FACILITY_ID']))
            # end for
    
        #############################################    
        # CONTAMINANTS
        
        # default
        sKYWRecord['CONTAMINANTS'] = []

        # query LCR_SAMPLE to get the list
        # note: this automatically filters to items 3 or fewer years old
        url = "https://iaspub.epa.gov/enviro/efservice/LCR_SAMPLE/SAMPLING_END_DATE/>/01-JAN-2013/PWSID/" + sKYWRecord['PWSID'] + '/JSON/'
        
        if args.debug:
            print "proc_sdwis_json.py: getting LCR_SAMPLE data for", sKYWRecord['PWSID']
            print url
        
        res = requests.get(url)
        if(res.ok):
            jDataSample = json.loads(res.content)
        else:
            res.raise_for_status()
             
        # done reading res
        res.close()
        
        # pause to be nice
        time.sleep(xSLEEP)

        # query LCR_SAMPLE_RESULT
        url = "https://iaspub.epa.gov/enviro/efservice/LCR_SAMPLE_RESULT/PWSID/" + sKYWRecord['PWSID'] + '/JSON/'
 
        if args.debug:
            print "proc_sdwis_json.py: getting LCR_SAMPLE_RESULT data for", sKYWRecord['PWSID']
            print url 
        
        res = requests.get(url)
        if(res.ok):
            jDataResult = json.loads(res.content)
        else:
            res.raise_for_status()
             
        # done reading res
        res.close()
        
        # pause to be nice
        time.sleep(xSLEEP)
           
        # construct the CONTAMINANTS by linking the two result sets
        # assume relationship between LCR_SAMPLE and LCR_SAMPLE_RESULT is 1:1
        
        for jItemSample in jDataSample:
            dictContaminant = {}
            dictContaminant['DATE'] = jItemSample['SAMPLING_END_DATE']
            for jItemResult in jDataResult:
                if jItemResult['SAMPLE_ID'] == jItemSample['SAMPLE_ID']:
                    # found the associated record
                    dictContaminant['CONTAMINANT'] = jItemResult['CONTAMINANT_CODE']
                    dictContaminant['LEVEL'] = jItemResult['SAMPLE_MEASURE']
                    dictContaminant['UNITS'] = jItemResult['UNIT_OF_MEASURE']
                    dictContaminant['SIGN'] = jItemResult['RESULT_SIGN_CODE']
                    # since we found it, store it
                    sKYWRecord['CONTAMINANTS'].append(dictContaminant)
                    # done here!
                    break
            # end for
        # end for
        
        #############################################    
        # AGGREGATES
        
        sKYWRecord['AGGREGATES'] = []
        
        dictStub = {}
        dictStub['AGGREGATE'] = "STATE"
        dictStub['CONTAMINANT'] = "PB90"
        dictStub['LEVEL'] = ".0035"
        dictStub['UNITS'] = "mg/L"
        dictStub['DATE'] = "2015-12-15T08:00:00"
        sKYWRecord['AGGREGATES'].append(dictStub)
        dictStub = {}
        dictStub['AGGREGATE'] = "COUNTY"
        dictStub['CONTAMINANT'] = "PB90"
        dictStub['LEVEL'] = ".0045"
        dictStub['UNITS'] = "mg/L"
        dictStub['DATE'] = "2015-12-15T08:00:00"
        sKYWRecord['AGGREGATES'].append(dictStub)
        dictStub = {}
        dictStub['AGGREGATE'] = "STATE"
        dictStub['CONTAMINANT'] = "CU86"
        dictStub['LEVEL'] = ".0035"
        dictStub['UNITS'] = "mg/L"
        dictStub['DATE'] = "2015-12-15T08:00:00"
        sKYWRecord['AGGREGATES'].append(dictStub)
        dictStub = {}
        dictStub['AGGREGATE'] = "COUNTY"
        dictStub['CONTAMINANT'] = "CU86"
        dictStub['LEVEL'] = ".0045"
        dictStub['UNITS'] = "mg/L"
        dictStub['DATE'] = "2015-12-15T08:00:00"
        sKYWRecord['AGGREGATES'].append(dictStub)
        
        #############################################    
        # TREATMENTS
        
        sKYWRecord['TREATMENTS'] = []
 
        # query TREATMENTS to get the list
        # note: if there is a SELLER_PWSID, use that instead
        if sKYWRecord['SOURCE_PURCHASED_PWSID'] == "":
            url = "https://iaspub.epa.gov/enviro/efservice/TREATMENT/PWSID/" + sKYWRecord['PWSID'] + '/JSON/'
        else:
            url = "https://iaspub.epa.gov/enviro/efservice/TREATMENT/PWSID/" + sKYWRecord['SOURCE_PURCHASED_PWSID'] + '/JSON/'
        
        if args.debug:
            print "proc_sdwis_json.py: getting TREATMENT data for", sKYWRecord['PWSID']
            print url
        
        res = requests.get(url)
        if(res.ok):
            jData = json.loads(res.content)
            # construct the TREATMENTS
            for jItem in jData:
                dictTreatment = {}
                dictTreatment['COMMENTS'] = jItem['COMMENTS_TEXT']
                
                dictTreatment['OBJECTIVE'] = jItem['TREATMENT_OBJECTIVE_CODE']
                # look up this code
                if dictSDWISObjectives.has_key(str(dictTreatment['OBJECTIVE'])):
                    dictTreatment['OBJECTIVE'] = str(dictTreatment['OBJECTIVE']) + "|" + dictSDWISObjectives[str(dictTreatment['OBJECTIVE'])]
                    
                dictTreatment['TREATMENT'] = jItem['TREATMENT_PROCESS_CODE']
                # look up this code
                if dictSDWISTreatments.has_key(str(dictTreatment['TREATMENT'])):
                    dictTreatment['TREATMENT'] = str(dictTreatment['TREATMENT']) + "|" + dictSDWISTreatments[str(dictTreatment['TREATMENT'])]
                    
                # store the TREATMENTS
                sKYWRecord['TREATMENTS'].append(dictTreatment)
        else:
            res.raise_for_status()
             
        # done reading res
        res.close()
        
        # pause to be nice
        time.sleep(xSLEEP)

        #############################################    
        # violations/enforcements
        # to do: grab from...
        sKYWRecord['VIOLATIONS'] = []
        
        #############################################    
        # save the record        
        lstNew.append(sKYWRecord)
        
        # increment the count
        nRecords = nRecords + 1
        
        if args.debug:
            print "proc_sdwis_json.py: stored enriched record for", sKYWRecord['PWSID']
        
        #############################################    
        # write out entire output after each record - if requested
        if args.outputfile:
            
            # create the file
            try:
                fo = open(args.outputfile, 'wb')
            except Exception, e:                
                fo.close()
                sys.exit(e)
            
            # write the file
            try:
                json.dump(lstNew, fo, sort_keys=True, indent=4, separators=(',', ': '))
            except Exception, e:
                sys.exit(e)
                
            fo.close()
            
    # end for

    # report            
    print "proc_sdwis_json.py: wrote", nRecords, "records to:", args.outputfile
    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end