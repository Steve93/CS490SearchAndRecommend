"""
metaSearch.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:
  
2/10/15-SM-Modularized lab 2 and commented.
2/10/15-SM-New.
"""

import json
import time

from urllib.parse   import urlencode
from urllib.request import urlopen

sleepCount = 1
sleepTime  = 120

def googleSearch ( query, start=0 ):
  """
  Send query to google via ajax request and fetch 8 results beginning at
    start parameter.
  Throttles at a length twice as long as previous sleep time for every 403
    response from Google.
  """
  
  resStatus = 403
  data      = None

  while ( resStatus == 403 ):
    qStr = urlencode( { 'q': query } )
    sStr = urlencode( { 'start': start } )
    rStr = urlencode( { 'rsz': 8 } )
    url  = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0'
    url += '&%s&%s&%s' % (qStr, sStr, rStr)
    
    search_response = urlopen( url )
    search_results  = search_response.read( ).decode( 'utf-8', 'replace' )
    
    results   = json.loads( search_results )
    resStatus = results[ 'responseStatus' ]
    data      = results[ 'responseData' ]
    
    # Throttle requests to Google.
    if ( resStatus == 403 ):
      global sleepCount, sleepTime
      print( "Sleep #" + str(sleepCount) + ".",
             "Going to sleep for", str( sleepTime/60 ), "minutes." )
      time.sleep( sleepTime )
      sleepCount += 1
      sleepTime  *= 2
  
  hits = data['results']
  
  for hit in hits:
    yield hit['url']


def executeQuery ( db, query, count, start ):
  """
  Use meta search engine of choice to query for media item. Build a list
    of len(count) urls for media item. Return list and next start value.
  """

  startInc  = 8
  queryList = []

  while ( count > 0 ):
    for url in googleSearch( query, start ):
      urlId = db.lookupCachedUrl_byUrl( url )

      if ( urlId ):
        continue
      else:
        queryList.append( url )
        count -= 1

    start += startInc

  return queryList, start






