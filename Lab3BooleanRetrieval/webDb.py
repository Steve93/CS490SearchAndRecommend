"""
webDb.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:

sqllite3 wrapper for Search Engine Lab Sequence (Richard Wicentowski, Doug Turnbull, 2010-2015)
CS490: Search Engine and Recommender Systems
http://jimi.ithaca.edu/CourseWiki/index.php/CS490_S15_Schedule
https://github.com/dougturnbull/CS490SearchAndRecommend

2/17/15-SM-Added lookupItemId and lookupItem_byId functions.
2/6/15-SM-New.

"""


import sqlite3
import re


class WebDb ( object ):

  def __init__ ( self, dbfile ):
    """
    Connect to the database specified by dbfile.  Assumes that this
    dbfile already contains the tables specified by the schema.
    """
    self.dbfile = dbfile
    self.cxn = sqlite3.connect( dbfile )
    self.cur = self.cxn.cursor( )
    
    
    self._execute( """CREATE TABLE IF NOT EXISTS CachedUrl (
                     id  INTEGER PRIMARY KEY,
                     url VARCHAR,
                     title VARCHAR,
                     docType VARCHAR
                  );""" )
                        
    self._execute( """CREATE TABLE IF NOT EXISTS UrlToItem (
                     id  INTEGER PRIMARY KEY,
                     urlId INTEGER,
                     itemId INTEGER
                  );""" )
    
    self._execute( """CREATE TABLE IF NOT EXISTS Item (
                     id  INTEGER PRIMARY KEY,
                     name VARCHAR,
                     type VARCHAR
                  );""" )
                         
                              

  def _quote ( self, text ):
    """
    Properly adjusts quotation marks for insertion into the database.
    """

    return re.sub( "'", "''", text )

  def _unquote ( self, text ):
    """
    Properly adjusts quotations marks for extraction from the database.
    """

    return re.sub( "''", "'", text )

  def _execute ( self, sql ):
    """
    Execute an arbitrary SQL command on the underlying database.
    """
    res = self.cur.execute( sql )
    self.cxn.commit( )

    return res

  def lookupCachedUrl_byUrl ( self, url ):
    """
    Returns the id of the row matching url in CachedUrl.

    If there is no matching url, returns an None.
    """

    sql = "SELECT id FROM CachedUrl WHERE url='%s'" % ( url )
    res = self._execute( sql )
    reslist = res.fetchall( )
    
    if reslist == []:
      return None
    elif len( reslist ) > 1:
      raise RuntimeError( 'DB: Duplicate url failure on CachedUrl.' )
    else:
      return reslist[0][0]

  def lookupCachedUrl_byId ( self, cache_url_id ):
    """
    Returns a (url, docType, title) tuple for the row
    matching cache_url_id in CachedUrl.

    If there is no matching cache_url_id, returns an None.
    """

    sql = "SELECT url, docType, title FROM CachedUrl WHERE id=%d"\
          % (cache_url_id)
    res = self._execute( sql )
    reslist = res.fetchall( )
    
    if reslist == []:
      return None
    else:
      # reslist = (reslist[0][0], reslist[0][1], reslist[0][2])
      return reslist[0]

  def lookupItem ( self, name, itemType ):
    """
    Returns a Item id for the row
    matching name and itemType in the Item table.

    If there is no match, returns an None.
    """

    sql = "SELECT id FROM Item WHERE name='%s' AND type='%s'"\
          % ( self._quote( name ), self._quote( itemType ) )

    res = self._execute( sql )
    reslist = res.fetchall( )

    if reslist == []:
      return None
    else:
      return reslist[0][0]

  def lookupItem_byId( self, itemId ):
    """
    Returns (name, itemType) for the row
    matching itemId in the Item table.

    If there is no match, returns an None.
    """

    sql = "SELECT name, type FROM Item WHERE id=%d" % itemId

    res = self._execute( sql )
    reslist = res.fetchall( )

    if reslist == []:
      return None
    else:
      return reslist[0]
  
  def lookupItemId ( self, urlId ):
    """
    Returns (itemId) for the row
    matching urlId in the itemId table.

    If there is no match, returns an None.
    """

    sql = "SELECT itemId FROM UrlToItem WHERE urlId=%d" % urlId

    res = self._execute( sql )
    reslist = res.fetchall( )

    if reslist == []:
      return None
    else:
      return reslist[0][0]
          
  def lookupUrlToItem ( self, urlId, itemId ):
    """
    Returns a urlToItem.id for the row
    matching name and itemType in the Item table.

    If there is no match, returns an None.
    """

    sql = "SELECT id FROM UrlToItem WHERE urlId=%d AND itemId=%d"\
          % (urlId, itemId)
    res = self._execute( sql )
    reslist = res.fetchall( )
    
    if reslist == []:
      return None
    else:
      return reslist[0]

  def lookupUrlsForItem ( self, name, itemType ):
    """
    Returns urlIds and urls for item
    matching name and itemType in the Item table.

    If there is no match, returns an empty list.
    """

    itemId = self.lookupItem( name, itemType )
    
    reslist = []

    if ( itemId ):
      sql = "SELECT urlId FROM UrlToItem WHERE itemId=%d" % itemId

      res = self._execute( sql )
      reslist = res.fetchall( )

    return reslist

  def deleteCachedUrl_byId ( self, cache_url_id ):
    """
    Delete a CachedUrl row by specifying the cache_url_id.

    Returns the previously associated url if the integer id was in
    the database; returns None otherwise.
    """

    result = self.lookupCachedUrl_byId( cache_url_id )
    if result == None:
      return None

    (url, docType, title) = result

    sql = "DELETE FROM CachedUrl WHERE id=%d" % (cache_url_id)
    self._execute( sql )
    return url

  def insertCachedUrl ( self, url, docType="", title=None ):
    """
    Inserts a url into the CachedUrl table, returning the id of the
    row.
    
    Enforces the constraint that url is unique.
    """

    cache_url_id = self.lookupCachedUrl_byUrl( url )
    
    if cache_url_id is not None:
      return cache_url_id

    sql = """INSERT INTO CachedUrl (url, docType, title)
             VALUES ('%s', '%s','%s')""" %\
             (url, self._quote( docType ), self._quote( title ))

    res = self._execute( sql )

    return self.cur.lastrowid

  def insertItem ( self, name, itemType ):
    """
    Inserts a item into the Item table, returning the id of the
    row. 
    itemType should be something like "music", "book", "movie"
    
    Enforces the constraint that name is unique.
    """        

    item_id = self.lookupItem( name, itemType )
    if item_id is not None:
      return item_id

    sql = """INSERT INTO Item (name, type)
             VALUES ('%s', '%s')""" % (self._quote( name ),
                                       self._quote( itemType ))

    res = self._execute( sql )
    return self.cur.lastrowid

  def insertUrlToItem ( self, urlId, itemId ):
    """
    Inserts a item into the UrlToItem table, returning the id of the
    row.         
    Enforces the constraint that (urlId,itemId) is unique.
    """

    u2i_id = self.lookupUrlToItem( urlId, itemId )
    if u2i_id is not None:
      return u2i_id

    sql = """INSERT INTO UrlToItem (urlId, itemId)
             VALUES ('%s', '%s')""" % (urlId, itemId)

    res = self._execute( sql )
    return self.cur.lastrowid


if __name__=='__main__':
  db = WebDb( 'test.db' )
  
  
  urlId  = db.insertCachedUrl( "http://jimi.ithaca.edu/",
                               "text/html", "JimiLab's :: Ithaca College" )
  itemId = db.insertItem( "JimiLab", "Research Lab" )
  u2iId  = db.insertUrlToItem( urlId, itemId )

  (url, docType, title) =  db.lookupCachedUrl_byId(urlId);

  print( "Page Info: ", url, "\t", docType, "\t", title )
  



