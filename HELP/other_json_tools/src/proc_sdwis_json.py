#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

#############################################    
# process & link SDWIS WATER_SYSTEM data, then write flattened records out

import sys
import argparse
import json
import operator
import csv
import requests
import time

# to do: review messages, migrate to logging
# to do: it would be nice to show matches (from CCR) in non-debug mode
# to do: improve options vs bleow
# to do: resolve question about CITIES_SERVED - seems like we should just aggregate them
# to do: after reading the input json file, say how many records, and periodically report on progress
# to do: add option to query for json perhaps just single PWSID

from sdwis import dictSDWISObjectives, dictSDWISTreatments

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description="Process SDWIS json into KYW data model and write out as json")
    parser.add_argument('-w', '--wsf', help="path to JSON with WATER_SYSTEM_FACILITY information")
    parser.add_argument('-t', '--treatment', help="path to JSON with TREATMENT information")
    parser.add_argument('-s1', '--sample', help="path to JSON with TREATMENT information")
    parser.add_argument('-s2', '--result', help="path to JSON with TREATMENT information")
    parser.add_argument('-o', '--outputfile', help="filename to write processed output to, in json format")
    parser.add_argument('-d', '--debug', action='store_true', help="provide debug information")
    parser.add_argument('-r', '--resume', help="SORT_KEY to start with")
    parser.add_argument('-l', '--list', action='store_true', help="list PWSIDs only")
    parser.add_argument('-x', '--xcities', action='store_true', help="process records excluded by legacy CITIES_SERVED logic")
    parser.add_argument('inputfile', help="path to input file in json format")
    args = parser.parse_args()
        
    #############################################    
    # read the input file
    
    print "proc_sdwis_json.py: reading", args.inputfile
    
    lstInput = []
    
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
            jRecord['CITY_NAME'] = u"UNKNOWN"
            if args.debug:
                print "proc_sdwis_json.py: missing CITY_NAME for", jRecord['PWSID'], "assuming UNKNOWN"
        if isinstance(jRecord[u'CITY_NAME'], unicode) or isinstance(jRecord[u'CITY_NAME'], str):
            # handle string
            jRecord[u'SP_SORT_KEY'] = jRecord[u'STATE_CODE'] + jRecord[u'CITY_NAME'].replace('\\xa0', ' ')
        else:
            # handle integer
            jRecord[u'SP_SORT_KEY'] = jRecord[u'STATE_CODE'] + str(jRecord[u'CITY_NAME']).strip()
            
    #############################################    
    # sort records

    lstSorted = sorted(lstInput, key=operator.itemgetter(u'SP_SORT_KEY'))    
    
    if args.list:
        for record in lstSorted:
            print record['PWSID']
        sys.exit()
    # end if 

    #############################################    
    # load the CCR_WSW file
    
    print "proc_sdwis_json.py: reading CCR_WSW.csv"

    dictCCRWSW = {}
    
    try:
        fi = open("CCR_WSW.csv", 'r')
    except Exception, e:
        print "proc_sdwis_json.py: error reading CCR_WSW.csv:", e
        sys.exit(e)
    
    try:
        csvReader = csv.reader(fi)   
       
        for lstRow in csvReader:

            # skip any record where CCR is blank
            if lstRow[0] == "":
                # skip
                continue
            
            if lstRow[1] == "":
                # skip
                continue
            
            # handle , in CCR
            sCCRKey = lstRow[0]
            if sCCRKey.find(',') >= -1:
                sCCRKey = sCCRKey[:sCCRKey.find(',')]
            
            if dictCCRWSW.has_key(sCCRKey):
                # ignore
                continue
            
            # handle total BS in WSW...
            # "<a href=\""javascript:popURL('http://www.auburnalabama.org/wrm/');\"">http://www.auburnalabama.org/wrm/</a>&nbsp;<a class=\""exit-disclaimer\"" title=\""EPA's External Link Disclaimer\"" href=\""http://www2.epa.gov/home/exit-epa\"">Exit</a>"
            sWSWKey = lstRow[1]
            if sWSWKey.find("'") == -1:
                # ignore
                continue
            nWSWStart = sWSWKey.find("'") + 1
            sWSWKey = sWSWKey[nWSWStart:sWSWKey.find("'",nWSWStart + 1)]
            
            # store the pair
            dictCCRWSW[sCCRKey] = sWSWKey
    
        # end for

    except Exception, e:
        print "proc_sdwis_json.py: error reading CCR_WSW.csv:", e
        fi.close()
        
    fi.close()

    #############################################    
    # load the CCR file

    print "proc_sdwis_json.py: reading CCR.csv"

    lstCCR = []
    
    try:
        fi = open("CCR.csv", 'r')
    except Exception, e:
        print "proc_sdwis_json.py: error opening CCR.csv:", e
        sys.exit(e)
    
    try:
        csvReader = csv.reader(fi)   

        # first, store the header
        lstHeader = csvReader.next()
        
        xLINK = 0
        xCITY = 1
        xPWSID = 2
        xDATE = 3
        
        for lstRow in csvReader:

            # skip any record where city is blank
            if lstRow[xCITY] == "":
                # skip
                continue
            
            # new ccr record
            dictCCR = {}
            dictCCR['ORG_CCR_REPORT_URI'] = lstRow[xLINK]
            
            # lookup wsw record, if any
            if dictCCRWSW.has_key(dictCCR['ORG_CCR_REPORT_URI']):
                dictCCR['ORG_URI'] = dictCCRWSW[dictCCR['ORG_CCR_REPORT_URI']]
            else:
                dictCCR['ORG_URI'] = None

            dictCCR['CITY_NAME'] = lstRow[xCITY].upper()
            dictCCR['PWSID'] = lstRow[xPWSID]
            dictCCR['CONTAMINANTS'] = []
            # now iterate across remaining columns
            for c in range(5,len(lstHeader)):
                if lstRow[c] != "":
                    dictContaminant = {}
                    dictContaminant['DATE'] = str(lstRow[xDATE]) # year collected
                    dictContaminant['CITY_NAME'] = dictCCR['CITY_NAME'].upper()
                    dictContaminant['CONTAMINANT'] = str(lstHeader[c])
                    sSign = None
                    sLevel = str(lstRow[c])
                    if (sLevel.find("and") > -1) or (sLevel.lower() == "nd") or (sLevel == "N/A") or (sLevel == "NA") or (sLevel.find('-') > -1) or (sLevel.find('. .') > -1) or (sLevel.find('..') > -1):
                        dictContaminant['LEVEL'] = sLevel
                    else:    
                        if sLevel[:1] == "<":
                            nLevel = float(sLevel[1:])
                            sSign = "<"
                        else:
                            nLevel = float(sLevel)
                            dictContaminant['LEVEL'] = nLevel
                    dictContaminant['UNITS'] = None
                    dictContaminant['SIGN'] = sSign
                    # append to list of contaminants
                    dictCCR['CONTAMINANTS'].append(dictContaminant)
                # end if
            # end for
            # store it
            lstCCR.append(dictCCR)
            
        # end for
    except Exception, e:
        print "proc_sdwis_json.py: error reading CCR.csv:", e
        fi.close()
        
    fi.close()

    #############################################    
    # process records
        
    # initialize
    nRecords = 0
    lstNew = []
    url = ""
    sRunDate = time.strftime("%Y-%m-%d")
    bSkip = False
    if args.resume:
        bSkip = True
    xSLEEP = 2        
    
    #############################################    
    # MAIN LOOP: enrich and model to KYW model

    for jRecord in lstSorted:
        
        ##########
        # skip
        if bSkip:
            if isinstance(jRecord['PWSID'], int):
                jRecord['PWSID'] = str(jRecord['PWSID'])
            if jRecord['PWSID'].lower() == args.resume.lower():
                bSkip = False
            else:
                continue

        #############################################    
        # filter
        
        if jRecord['PWS_TYPE_CODE'] != u'CWS':
            # skip it
            continue
        
        if jRecord['PWS_ACTIVITY_CODE'] != u'A':
            # skip it
            continue
               
        if args.xcities:
        # if set, process ONLY records which were previously excluded by the CITIES_SERVED != CITY_NAME clause
            if jRecord['CITIES_SERVED'] == jRecord['CITY_NAME']:
                # skip it!!
                print "proc_sdwis_json.py: warning: xcities mode, skipping PWSID:", jRecord['PWSID']
                continue
            if jRecord['CITIES_SERVED'].upper() == "NOT REPORTED":
                # skip it!!
                print "proc_sdwis_json.py: warning: xcities mode, skipping PWSID (NR):", jRecord['PWSID']
                continue
                
        #############################################    
        # process into KYW data model

        if args.debug:
            print "proc_sdwis_json.py: processing:", str(jRecord['PWSID'])
        
        dictKYWRecord = {}
        dictKYWRecord['PWSID'] = str(jRecord['PWSID'])
        dictKYWRecord['PWS_NAME'] = str(jRecord['PWS_NAME'])
        dictKYWRecord['SORT_KEY'] = jRecord['SP_SORT_KEY']
        dictKYWRecord['SDWIS_URI'] = "https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM/PWSID/" + dictKYWRecord['PWSID'] 
        dictKYWRecord['SDWIS_PULLDATE'] = sRunDate
        dictKYWRecord['STATE_CODE'] = jRecord['STATE_CODE']
        dictKYWRecord['COUNTY_NAME'] = jRecord['COUNTIES_SERVED']
        
        dictKYWRecord['CITY_NAME'] = jRecord['CITY_NAME'].upper()
        
        dictKYWRecord['CITY_NAMES'] = []
        dictKYWRecord['CITY_NAMES'].append(jRecord['CITY_NAME'].upper())

        if jRecord['CITIES_SERVED'] != jRecord['CITY_NAME']: 
            if jRecord['CITIES_SERVED'].upper() != "NOT REPORTED":
                # aggregate
                dictKYWRecord['CITY_NAMES'].append(jRecord['CITIES_SERVED'].upper())
        
        dictKYWRecord['ZIP_CODE'] = str(jRecord['ZIP_CODE'])
        dictKYWRecord['POPULATION_SERVED'] = jRecord['POPULATION_SERVED_COUNT']
        dictKYWRecord['WATER_SOURCE_TYPE'] = str(jRecord['PRIMARY_SOURCE_CODE'])
        dictKYWRecord['IS_SCHOOL'] = str(jRecord['IS_SCHOOL_OR_DAYCARE_IND'])
        dictKYWRecord['OUTSTANDING_PERFORMER'] = jRecord['OUTSTANDING_PERFORM_BEGIN_DATE']
        if jRecord['ORG_NAME'] == None:
            dictKYWRecord['ORG_NAME'] = None
        else:
            dictKYWRecord['ORG_NAME'] = str(jRecord['ORG_NAME'].encode("utf-8","ignore"))
        dictKYWRecord['ORG_ADDRESS_LINE1'] = jRecord['ADDRESS_LINE1']
        dictKYWRecord['ORG_ADDRESS_LINE2'] = jRecord['ADDRESS_LINE2']
        dictKYWRecord['ORG_PHONE_NUMBER'] = str(jRecord['PHONE_NUMBER'])

        #############################################    
        # URIs
        
        dictKYWRecord['ORG_CCR_REPORT_URI'] = None
        dictKYWRecord['ORG_URI'] = None
         
        #############################################    
        # WATER_SYSTEM_FACILITY
        
        # initialize
        dictKYWRecord['SOURCE_PURCHASED_PWSID'] = None
        dictKYWRecord['SOURCE_RESERVOIR_INFO'] = []
        dictKYWRecord['SOURCE_TREATMENTPLANT_INFO'] = []
        dictFacilities = {}     # for later use by TREATMENTS
        
        if not args.wsf:
            # pull live from envirofacts
            url = "https://iaspub.epa.gov/enviro/efservice/WATER_SYSTEM_FACILITY/FACILITY_ACTIVITY_CODE/A/PWSID/" + dictKYWRecord['PWSID'] + '/JSON/'
            if args.debug:
                print "proc_sdwis_json.py: getting WATER_SYSTEM_FACILITY data for", dictKYWRecord['PWSID'], "from", url
            res = requests.get(url)
            if(res.ok):
                jData = json.loads(res.content)
            else:
                time.sleep(120)
                res = requests.get(url)
                if(res.ok):
                    jData = json.loads(res.content)
                else:
                    time.sleep(500)
                    res = requests.get(url)
                    if(res.ok):
                        jData = json.loads(res.content)
                    else:
                        # ok, enough
                        res.raise_for_status()
            # end if
            res.close()
            time.sleep(xSLEEP)
        else:
            # load from json file specified in args.wsf
            if args.debug:
                print "proc_sdwis_json.py: getting WATER_SYSTEM_FACILITY data for", dictKYWRecord['PWSID'], "from", args.wsf
            try:
                fi = open(args.wsf, 'r')
            except Exception, e:
                print "proc_sdwis_json.py: error opening:", e
                sys.exit(e)
            try:
                jData = json.load(fi)
            except Exception, e:
                print "proc_sdwis_json.py: error reading:", e
                fi.close()
            fi.close()
        # end if
            
        # now iterate on jData, regardless of source...
        for jItem in jData:
            # purchased water
            if jItem['FACILITY_TYPE_CODE'] in ["CC", "NP"]:
                dictKYWRecord['SOURCE_PURCHASED_PWSID'] = jItem['SELLER_PWSID']
            # per SDWIS, IS_SOURCE_IND is set for all source types [ "IG", "IN", "RC", "RS", "SP", "WL", "NN" ]
            # more details on page 59 of this: http://www.exchangenetwork.net/schema/SDWA/2/ListValidationsByElement.pdf
            if jItem['IS_SOURCE_IND'] == "Y":
                dictTmp = {}
                if jItem['STATE_FACILITY_ID'] == None:
                    dictTmp['FACILITY_ID'] = None
                else:
                    if isinstance(jItem['STATE_FACILITY_ID'], int):
                        dictTmp['FACILITY_ID'] = str(jItem['STATE_FACILITY_ID'])
                    else:
                        dictTmp['FACILITY_ID'] = str(jItem['STATE_FACILITY_ID'].encode("utf-8","ignore"))
                if isinstance(jItem['FACILITY_NAME'],int):
                    dictTmp['FACILITY_NAME'] = str(jItem['FACILITY_NAME'])
                else:
                    if jItem['FACILITY_NAME'] == None:
                        dictTmp['FACILITY_NAME'] = None
                    else:
                        dictTmp['FACILITY_NAME'] = str(jItem['FACILITY_NAME'].encode("utf-8","ignore"))
                # store it
                dictKYWRecord['SOURCE_RESERVOIR_INFO'].append(dictTmp)
            # treatment plants
            if jItem['FACILITY_TYPE_CODE'] == "TP":
                dictTmp = {}
                dictTmp['FACILITY_ID'] = str(jItem['STATE_FACILITY_ID'])
                dictTmp['FACILITY_NAME'] = str(jItem['FACILITY_NAME'].encode("utf-8","ignore"))
                # store it
                dictKYWRecord['SOURCE_TREATMENTPLANT_INFO'].append(dictTmp)
                # confusing below: use WATER_SYSTEM_FACILITY.FACILITY_ID (via jItem) to retrieve the STATE_FACILITY_ID, NO RELATION to 'FACILITY_ID' constructed above
                dictFacilities[jItem['FACILITY_ID']] = str(jItem['STATE_FACILITY_ID'])
        # end for
            
        #############################################    
        # CONTAMINANTS
        
        # initialize
        dictKYWRecord['CONTAMINANTS'] = []

        if not args.sample:
            # pull live from envirofacts
            url = "https://iaspub.epa.gov/enviro/efservice/LCR_SAMPLE/SAMPLING_END_DATE/>/01-JAN-2013/PWSID/" + dictKYWRecord['PWSID'] + '/JSON/'
            if args.debug:
                print "proc_sdwis_json.py: getting LCR_SAMPLE data for", dictKYWRecord['PWSID'], "from", url
            res = requests.get(url)
            if(res.ok):
                jDataSample = json.loads(res.content)
            else:
                time.sleep(120)
                res = requests.get(url)
                if(res.ok):
                    jDataSample = json.loads(res.content)
                else:
                    time.sleep(500)
                    res = requests.get(url)
                    if(res.ok):
                        jDataSample = json.loads(res.content)
                    else:
                        # ok, enough
                        res.raise_for_status()
        else:
            # load from json file specified in args.treatment
            if args.debug:
                print "proc_sdwis_json.py: getting LCR_SAMPLE data for", dictKYWRecord['PWSID'], "from", args.sample
            try:
                fi = open(args.treatment, 'r')
            except Exception, e:
                print "proc_sdwis_json.py: error opening:", e
                sys.exit(e)
            try:
                jDataSample = json.load(fi)
            except Exception, e:
                print "proc_sdwis_json.py: error reading:", e
                fi.close()
            fi.close()
        # end if

        if not args.result:
            # pull live from envirofacts
            url = "https://iaspub.epa.gov/enviro/efservice/LCR_SAMPLE_RESULT/PWSID/" + dictKYWRecord['PWSID'] + '/JSON/'
            if args.debug:
                print "proc_sdwis_json.py: getting LCR_SAMPLE_RESULT data for", dictKYWRecord['PWSID'], "from", url
            res = requests.get(url)
            if(res.ok):
                jDataResult = json.loads(res.content)
            else:
                time.sleep(120)
                res = requests.get(url)
                if(res.ok):
                    jDataResult = json.loads(res.content)
                else:
                    time.sleep(500)
                    res = requests.get(url)
                    if(res.ok):
                        jDataResult = json.loads(res.content)
                    else:
                        # ok, enough
                        res.raise_for_status()
        else:
            # load from json file specified in args.treatment
            if args.debug:
                print "proc_sdwis_json.py: getting LCR_SAMPLE_RESULT data for", dictKYWRecord['PWSID'], "from", args.result
            try:
                fi = open(args.treatment, 'r')
            except Exception, e:
                print "proc_sdwis_json.py: error opening:", e
                sys.exit(e)
            try:
                jDataResult = json.load(fi)
            except Exception, e:
                print "proc_sdwis_json.py: error reading:", e
                fi.close()
            fi.close()
        # end if
           
        # construct the CONTAMINANTS by linking; relationship between LCR_SAMPLE and LCR_SAMPLE_RESULT is 1:1
        for jItemSample in jDataSample:
            # create record for sample
            dictContaminant = {}
            dictContaminant['DATE'] = jItemSample['SAMPLING_END_DATE']
            # link to the result, and copy it
            for jItemResult in jDataResult:
                if jItemResult['SAMPLE_ID'] == jItemSample['SAMPLE_ID']:
                    dictContaminant['CONTAMINANT'] = jItemResult['CONTAMINANT_CODE']
                    dictContaminant['LEVEL'] = jItemResult['SAMPLE_MEASURE']
                    dictContaminant['UNITS'] = jItemResult['UNIT_OF_MEASURE']
                    dictContaminant['SIGN'] = jItemResult['RESULT_SIGN_CODE']
                    # store it
                    dictKYWRecord['CONTAMINANTS'].append(dictContaminant)
                    # don't need to keep looking
                    break
            # end for
        # end for
        
        #############################################    
        # now add contaminants from lstCCR
                
        for CCR in lstCCR:
            if CCR['PWSID'] == dictKYWRecord['PWSID']:
                # hit
                print "proc_sdwis_json.py: matched:", CCR['PWSID']
                # aggregate CITY_NAME
                dictKYWRecord['CITY_NAMES'].append(CCR['CITY_NAME'].upper())
                # sort unique
                dictKYWRecord['CITY_NAMES'] = sorted(set(dictKYWRecord['CITY_NAMES']))
                # copy the URIs - shouldn't vary across dupes
                dictKYWRecord['ORG_CCR_REPORT_URI'] = CCR['ORG_CCR_REPORT_URI']
                dictKYWRecord['ORG_URI'] = CCR['ORG_URI']
                # append the contaminant records
                for Contaminant in CCR['CONTAMINANTS']:
                    dictKYWRecord['CONTAMINANTS'].append(Contaminant)
                
        #############################################    
        # TREATMENTS
        
        # initialize
        dictKYWRecord['TREATMENTS'] = []
 
        if not args.wsf:
            # pull live from envirofacts
            url = "https://iaspub.epa.gov/enviro/efservice/TREATMENT/PWSID/" + dictKYWRecord['PWSID'] + '/JSON/'
            if args.debug:
                print "proc_sdwis_json.py: getting TREATMENT data for", dictKYWRecord['PWSID'], "from", url
            res = requests.get(url)
            if(res.ok):
                jData = json.loads(res.content)
            else:
                time.sleep(120)
                res = requests.get(url)
                if(res.ok):
                    jData = json.loads(res.content)
                else:
                    time.sleep(500)
                    res = requests.get(url)
                    if(res.ok):
                        jData = json.loads(res.content)
                    else:
                        # ok, enough
                        res.raise_for_status()
        else:
            # load from json file specified in args.treatment
            if args.debug:
                print "proc_sdwis_json.py: getting TREATMENT data for", dictKYWRecord['PWSID'], "from", args.treatment
            try:
                fi = open(args.treatment, 'r')
            except Exception, e:
                print "proc_sdwis_json.py: error opening:", e
                sys.exit(e)
            try:
                jData = json.load(fi)
            except Exception, e:
                print "proc_sdwis_json.py: error reading:", e
                fi.close()
            fi.close()
        # end if
        
        # now iterate on jData, regardless of source
        for jItem in jData:
            # omit any treatment not in dictFacilities
            if dictFacilities.has_key(jItem['FACILITY_ID']):
                # create record for treatment
                dictTreatment = {}
                if jItem.has_key('COMMENTS_TEXT'):
                    dictTreatment['COMMENTS'] = jItem['COMMENTS_TEXT']
                else:
                    dictTreatment['COMMENTS'] = None
                # copy the STATE_FACILITY_ID which is the payload from dictFacilities (FACILITY_ID is the key)
                dictTreatment['FACILITY_ID'] = dictFacilities[jItem['FACILITY_ID']]
                if jItem.has_key('TREATMENT_OBJECTIVE_CODE'):
                    dictTreatment['OBJECTIVE'] = str(jItem['TREATMENT_OBJECTIVE_CODE'])
                else:
                    dictTreatment['OBJECTIVE'] = None
                # look up this code
                if dictSDWISObjectives.has_key(str(dictTreatment['OBJECTIVE'])):
                    dictTreatment['OBJECTIVE_EXPLAIN'] = dictSDWISObjectives[str(dictTreatment['OBJECTIVE'])]
                dictTreatment['TREATMENT'] = str(jItem['TREATMENT_PROCESS_CODE'])
                # look up this code
                if dictSDWISTreatments.has_key(str(dictTreatment['TREATMENT'])):
                    dictTreatment['TREATMENT_EXPLAIN'] = dictSDWISTreatments[str(dictTreatment['TREATMENT'])]
                # store it
                dictKYWRecord['TREATMENTS'].append(dictTreatment)
        # end for

        #############################################    
        # violations/enforcements

        # initialize
        dictKYWRecord['VIOLATIONS'] = []
        
        # to be continued... 
                
        #############################################    
        # store the record        
 
        lstNew.append(dictKYWRecord)
        nRecords = nRecords + 1
        
        if args.debug:
            print "proc_sdwis_json.py: stored enriched record!"
        
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
    # MAIN LOOP

    # report            
    print "proc_sdwis_json.py: wrote", nRecords, "records to:", args.outputfile
    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end
