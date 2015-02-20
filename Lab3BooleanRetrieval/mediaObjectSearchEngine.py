"""
mediaObjectSearchEngine.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:
  Lab 1 - Text Pre-Processing: The first step for an IR system is to to read in
    the text and decide what will be the processing unit to be worked with. For
    your first assignment, you will write a program that removes HTML tags and
    then will tokenize a web documents. We will experiment with different
    tokenization and token normalization techniques. You will see how these
    different techniques affect the dictionary size.
  Lab 2 - Web Crawling: You will be assigned a search engine (Google) and given
    a list of M=25 popular items from three domain (Books, Music Artist,
    Movies). For each item, you will be asked to download and tokenize the top
    n=10 web pages.
  Lab 3 - Indexing & Boolean Retrieval: Using the tokenized web documents, you
    will create a positional index from the corpus of MxN (250) documents. You
    will then created a text-based interface where a user can enter text-based
    boolean queries (AND, OR, PHRASAL, NEAR) to find relevant documents. In
    addition, you will also return the most relevant media objects (e.g.,
    movies or books or artists) by counting the number of relevant web pages
    that are associated with with each of the M items.
  Lab 4 - Ranked Retrieval: Similar to the previous lab, you will implement a
    ranked retrieval system that can be used to find relevant web pages and
    media objects.
  Lab 5 - Evaluation: Once we have a fully-implemented system, we quantify how
    well the system works using a number of standard evaluation metrics.

2/20/15-SM-Comments for submission to Doug and class.
2/17/15-SM-Code to build positional index. UI for lab 3 boolean retrieval
  queries.
2/10/15-SM-Modularized lab 2 and commented.
2/9/15-SM-Added metaSearch, build and delete directory functions, and
  executeQuery function. Updated main to complete lab 2.
2/4/15-SM-New.
"""

# TODO: Python documentation - DocStrings
# TODO: Python unit testing
# TODO: Clean Architecture
# TODO: Add comments to def's

# Built in imports.
import os
import shutil
import re
import time

# File imports.
import spider
import configurationLoader
import metaSearch
from query import Query
from webDb import WebDb


db     = None
config = None
args   = None
pIndex = None

def buildDirectoryStructure ( ):
  """
  Sets up data directory with the following sub-directories:
    clean
    header
    item
    raw
  """

  if not os.path.exists( 'data' ):
    os.makedirs( 'data' )
  if not os.path.exists( 'data/clean' ):
    os.makedirs( 'data/clean' )
  if not os.path.exists( 'data/header' ):
    os.makedirs( 'data/header' )
  if not os.path.exists( 'data/item' ):
    os.makedirs( 'data/item' )
  if not os.path.exists( 'data/raw' ):
    os.makedirs( 'data/raw' )


def deleteDirectoryStructure ( deleteWebPages=False ):
  """
  Delete tokenized and nomralized files in clean directory.
    If deleteWebPages is True, then delete files in header and raw
    directories, as well as the database cache and cookie file.
  """
  if ( deleteWebPages ):
    if os.path.exists( 'data/header' ):
      shutil.rmtree( 'data/header' )
    if os.path.exists( 'data/raw' ):
      shutil.rmtree( 'data/raw' )
    if os.path.isfile ( 'data/cache.db' ):
      os.remove( 'data/cache.db' )
    if os.path.isfile ( 'data/cookies.txt' ):
      os.remove( 'data/cookies.txt' )

  if os.path.exists( 'data/clean' ):
    shutil.rmtree( 'data/clean' )


def makeFileName ( ipInt ):
  """
  Based on input integer, append leading 0's to make a fileName
  6 numbers long.
  """

  string = str( ipInt )
  while ( len(string) < 6 ):
    string = "0" + string
  
  return string


def mediaItems ( ipDir ):
  """
  Generator function to return query string, media item name,
    and media type from files in input directory.
  """

  for mediaFile in os.listdir( ipDir ):
    firstLine = True
    itemType  = mediaFile.strip( '.txt' )
    query     = ""

    for item in open( ipDir + '/' + mediaFile ):
      itemName = item.strip()

      if ( firstLine ):
        query     = itemName
        firstLine = False
        continue
      else:
        yield query.replace( "(0)", itemName ), itemName, itemType


def writeToFile ( fileName, data, isList=False ):
  """
  Writes input data to fileName file. Able to handle list data,
  where each entry is written on a new line.
  """
  
  with open( fileName, 'w' ) as f:
    if ( isList ):
      for entry in data:
        f.write( entry + '\n' )
    else:
      f.write( data )


def storePageAndHeader ( url, webPage, headerInfo, itemName, itemType ):
  """
  Adds reford for url, item, and urlToItem tables in database.
  Writes webPage, header, and tokens to files.
  """

  title, tokens = spider.parse( webPage, args.tokenizer )
  lowerTokens   = spider.lower( tokens )
  stemTokens    = spider.stem( lowerTokens, args.stemmer )

  reStr  = "content-type:(?P<ct>(.*))"
  result = re.search( reStr, headerInfo, re.IGNORECASE )

  if not title:
    title = ""

  urlId  = db.insertCachedUrl( url, result.group( "ct" ), title )
  itemId = db.insertItem( itemName, itemType )
  u2iId  = db.insertUrlToItem( urlId, itemId )

  fileNum = makeFileName( urlId )
  writeToFile( "data/raw/"    + fileNum + ".html", webPage )
  writeToFile( "data/header/" + fileNum + ".txt",  headerInfo )
  writeToFile( "data/clean/"  + fileNum + ".txt",  stemTokens, True )

  print( urlId )


def restoreClean ( ):
  """
  Opens every file in data/raw directory, parsing, lowercasing,
  and stemming. Stores results in data/clean directory.
  """

  for htmlFileName in os.listdir( 'data/raw' ):
    with open( 'data/raw/' + htmlFileName ) as htmlFile:
      webPage = htmlFile.read( )

      title, tokens = spider.parse( webPage, args.tokenizer )
      lowerTokens   = spider.lower( tokens )
      stemTokens    = spider.stem( lowerTokens, args.stemmer )

      cleanFileName = htmlFileName.strip( '.html' )
      writeToFile( "data/clean/"  + cleanFileName + ".txt",  stemTokens, True )

      print( int(cleanFileName) )


def downloadFreshDocuments ( ):
  """
  Based on the media items in data/item directory, download 10 webpages
  with a query from the first line in each item file. Keep querying until
  10 pages have been successfullly download. Sleep for 60 seconds before
  requesting url results from the metaSearch engine. This throttling was
  successful with Google's ajax api on 2/10/15.

  Once pages are downloaded, parse, stem, and store them with the
  storePageAndHeader function.
  """

  for query, itemName, itemType in mediaItems( 'data/item' ):
    count = 10 - len(db.lookupUrlsForItem( itemName, itemType ))
    start = 0
    while ( count > 0 ):
      time.sleep( 60 )
      urlList, start = metaSearch.executeQuery( db, query, count, start )
      
      for url in urlList:
        webPage, headers = spider.fetch( url )

        if ( webPage ):
          storePageAndHeader( url, webPage, headers, itemName, itemType )
          
          count -= 1
          if ( count <= 0 ):
            break


def buildPositionalIndex ( ):
  """
  Opens every file in data/clean directory and creates the positional index.
  """
  print( "Building positional index..." )

  global pIndex

  pIndex = dict( )

  for cleanFileName in os.listdir( 'data/clean' ):
    stemNum = 0
    with open( 'data/clean/' + cleanFileName ) as cleanFile:
      cleanFileName = cleanFileName.strip( '.txt' )
      for stem in cleanFile:
        stem = stem.strip( )
        if stem not in pIndex:
          pIndex[stem] = dict( )
        if cleanFileName not in pIndex[stem]:
          pIndex[stem][cleanFileName] = list( )
        pIndex[stem][cleanFileName].append( stemNum )
        stemNum += 1


  # If we want to print the index, uncomment code below.

  # for stem in pIndex:
  #   print( stem + ":" )
  #   for stemDoc in pIndex[stem]:
  #     print( "\t" + stemDoc + ":", end=" " )
  #     stemLocations = ""
  #     for stemLoc in pIndex[stem][stemDoc]:
  #       stemLocations += str( stemLoc ) + ', '
  #     stemLocations = stemLocations.strip( ', ' )
  #     print( stemLocations )
  #   print( )


def displayMenu ( ):
  print( "Command Options" )
  print( '  1)', "Token query" )
  print( '  2)', "AND query" )
  print( '  3)', "OR query" )
  print( '  4)', "Phrase query" )
  print( '  5)', "Near query" )
  print( '  6)', "QUIT" )
  print( )

  return getNumber( "Please type a number (1-6): " )


def getNumber ( prompt ):
  """
  Prompts user until integer input has been entered.
  """

  retVal = 0
  try:
    retVal = int( input( prompt ) )
  except:
    print( "Not a number, try again." )
    retVal = getNumber( prompt )
  finally:
    return retVal


def getWord ( ):
  """
  Prompt user for text input, return first group from space delimiter.
  """

  word = input( "Please enter a single word: " )
  return word.split( )[0]


def displayResults ( docList ):
  """
  With a list of document integer keys, lookup information in database
  to display to user. While looking up information, build a dictionary
  of items that the results were based on to display popular results in
  query.
  """

  print( "Results:", end="" )

  if docList == []:
    print( "\n\tNone" )
    return

  itemDict = dict( )

  count = 0
  for doc in docList:
    count += 1
    url, docType, title = db.lookupCachedUrl_byId( int(doc) )
    itemId              = db.lookupItemId ( int(doc) )
    name, itemType      = db.lookupItem_byId( itemId )

    if name not in itemDict:
      itemDict[name]  = 1
    else:
      itemDict[name] += 1

    print( "\n" + str(count) + ".\t" + title.strip( ) )
    print( '\t' + url )
    print( '\t' + itemType + ":", name )

  popularity  = 0
  for itemName in itemDict:
    if itemDict[itemName] > popularity:
      popularity  = itemDict[itemName]

  popItemList = list( )
  for itemName in itemDict:
    if itemDict[itemName] == popularity:
      popItemList.append( itemName )

  print( 'Most popular items are', popItemList, "with a count of", popularity )

def textBasedUi ( ):
  """
  Offers 6 options to user. Handles input errors.
  """

  userChoice = 0
  query = Query( pIndex )

  while ( userChoice != 6 ):
    userChoice = displayMenu( )
    if ( userChoice == 1 ):
      displayResults( query.tokenQuery( getWord( ) ) )
    elif ( userChoice == 2 ):
      displayResults( query.andQuery( getWord( ), getWord( ) ) )
    elif ( userChoice == 3 ):
      displayResults( query.orQuery( getWord( ), getWord( ) ) )
    elif ( userChoice == 4 ):
      displayResults( query.phraseQuery( getWord( ), getWord( ) ) )
    elif ( userChoice == 5 ):
      nearness = getNumber( "How near (number)?: " )
      displayResults( query.nearQuery( getWord( ), getWord( ), nearness ) )
    elif ( userChoice == 6 ):
      pass
    else:
      print( "Number was not between 1 and 6.")


def main ( ):
  """
  1. Load configuration file.
  2. Read command line.
  3. Init database.
  4. Build directory structure, ensuring it exists.
  5. Download fresh documents, or stem new clean files from raw dir.
  6. Build positional index.
  7. Run the text-based UI.
  """

  global config, args

  config = configurationLoader.loadJsonFile( 'configuration.json' )
  args   = configurationLoader.readCmdLine( )

  if ( args.delete ):
    print( 'Deleting cache and exiting' )
    if ( args.delete == 'clean' ):
      deleteDirectoryStructure( )
    elif ( args.delete == 'all' ):
      deleteDirectoryStructure( True )
    exit( 0 )
  
  global db
  db = WebDb( 'data/cache.db' )

  buildDirectoryStructure( )

  if ( args.restore ):
    restoreClean( )
  # Uncomment else statement if we need to download docs with meta search.
  # else:
  #   downloadFreshDocuments( )

  buildPositionalIndex( )

  textBasedUi( )


if __name__=='__main__':
  main( )


