"""
spider.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:

**********************************************************************
  Resource 'corpora/stopwords' not found.  Please use the NLTK
  Downloader to obtain the resource:  >>> nltk.download()
  Searched in:
    - '/Users/stephen/nltk_data'
    - '/usr/share/nltk_data'
    - '/usr/local/share/nltk_data'
    - '/usr/lib/nltk_data'
    - '/usr/local/lib/nltk_data'
**********************************************************************

2/17/15-SM-Added code to remove punctuation when parsing. This was done
  to fix phrase queries. Added strip() to titles when parsing.
2/10/15-SM-Modularized lab 2 and commented.
2/10/15-SM-Replaced downloadWebPage function with fetch function.
2/4/15-SM-Moved startup and "procedural glue" code to
  mediaObjectSearchEngine.py. Moved tokenize and stem code to
  naturalLanguageProcessing tokenize and stem files, respectively.
2/3/15-SM-Check-in with Doug. Refactor to remove style, link, and script tags.
  Add majority of nltk tokenizers and stemmers.
1/25/15-SM-Integrate with MLParser class.
1/23/15-SM-New.
"""

# Built-in imports.
import re
import os
import string

import urllib.request

from urllib.request import urlopen
from urllib.error   import URLError
from http           import cookiejar
from bs4            import BeautifulSoup, Comment

# File imports.
from naturalLanguageProcessing          import stem as stemlib
from naturalLanguageProcessing.tokenize import tokenize
from configurationLoader import loadJsonFile, dictToTuplePairsList


spiderHeadersDict = loadJsonFile( 'spiderRequestHeaders.json' )
spiderHeadersList = dictToTuplePairsList( spiderHeadersDict )


def parse ( rawHtml, ipTokenizer ):
  """
  This function should strip out HTML tags, remove punctuation, and break up
  the string of character into tokens. Also, extract the title of the of the
  web page by find the text between the <title> opening and </title> closing
  tags.
  """

  tokens = None
  soup   = BeautifulSoup( rawHtml, "lxml" )

  [ s.extract( ) for s in soup( ['iframe', 'script', 'style', 'link'] ) ]

  # Remove script tags
  rawText = removeComments( soup.get_text( ) )
  title   = ""
  if ( soup.title and soup.title.string ):
    title = soup.title.string.strip( )

  tokens = tokenize( ipTokenizer, rawText )

  punct = set( string.punctuation )
  for token in tokens:
    if token in punct:
      tokens.remove( token )

  return title, tokens


def removeComments ( ipHtmlText ):
  """
  Uses a regular expression to extract comments from html.
  """

  p = re.compile( r'<!--[\s\S]*?-->' )

  return p.sub( '', ipHtmlText )


def lower ( tokens ):
  """
  The function returns a lower case version of each token.
  """

  for i in range( len( tokens ) ):
    tokens[i] = tokens[i].lower( )

  return tokens


def stem ( tokens, ipStemmer ):
  """
  This function applies the Porter Stemmer to stem each token.
  """

  stemmer = stemlib.constructStemmer( ipStemmer )

  terms = list( )

  for token in tokens:
    terms.append( stemmer.stem( token ) )

  return terms


def getTerms ( ipTokens ):
  """
  Returns a sorted list of (unique) terms found in the list of input tokens.
  Order preserving uniquifier.
  """

  seen = set( )

  return [ token for token in ipTokens
           if token not in seen
           and not seen.add( token ) ]


def fetch ( ipUrl ):
  """
  This function downloads the given website and returns page and header
    information. Uses cookies and headers to spoof a real client browser.
  """

  p = re.compile( '\S+?:\/\/' )
  if ( p.search( ipUrl ) == None ):
    ipUrl = "http://" + ipUrl

  cookiefile = 'data/cookies.txt'

  cj = cookiejar.MozillaCookieJar( )
  if ( os.path.isfile( cookiefile ) ):
    cj.load( cookiefile )

  opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
  opener.addheaders = spiderHeadersList
  urllib.request.install_opener( opener )

  try:
    response = urlopen( ipUrl, timeout=3 )
    cj.save( cookiefile )
    page   = response.read( ).decode( 'utf-8', 'replace' )
    header = str( response.info( ) )
    return page, header
  except URLError as UrlE:
    print ( "Url Error: %s" % UrlE )
    with open( 'data/badUrl.log', 'a' ) as log:
      log.write( ipUrl + '\n' )
  except Exception as e:
    print ( "Exception: %s" % e )
  else:
    pass

  return None, None
