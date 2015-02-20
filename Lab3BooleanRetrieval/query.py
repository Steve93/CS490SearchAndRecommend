"""
query.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:

2/20/15-SM-Comments for submission to Doug and class.
2/17/15-SM-New.
"""

# File imports.
import configurationLoader
from naturalLanguageProcessing          import stem as stemlib
from naturalLanguageProcessing.tokenize import tokenize


class Query ( ):
  def __init__ ( self, index ):
    """
    Point to positional index. Load stemmer and tokenizer to use from
    configuration file.
    """

    self.index  = index
    self.config = configurationLoader.loadJsonFile( 'configuration.json' )
    self.config = self.config['query']

  def _normalize ( self, term ):
    """
    Tokenize and stem input term.
    """
    term    = tokenize( self.config['tokenizer'], term )[0]
    stemmer = stemlib.constructStemmer( self.config['stemmer'] )

    return stemmer.stem( term )

  def tokenQuery ( self, term ):
    """
    Simple query to check if term exists in document.
    """

    term = self._normalize( term )

    if term in self.index:
      return list( self.index[term].keys( ) )
    else:
      return []

  def andQuery ( self, first, second ):
    """
    Boolean AND query. Perform two token queries and join results.
    """

    results = list( )

    firstTokenQuery  = self.tokenQuery( first )
    secondTokenQuery = self.tokenQuery( second )

    for firstResult in firstTokenQuery:
      if firstResult in secondTokenQuery:
        results.append( firstResult )

    return results

  def orQuery ( self, first, second ):
    """
    Boolean OR query. Perform two token queries and join results.
    """

    firstTokenQuery  = self.tokenQuery( first )
    secondTokenQuery = self.tokenQuery( second )

    results = firstTokenQuery
    
    for secondResult in secondTokenQuery:
      results.append( secondResult )

    return results

  def phraseQuery ( self, first, second ):
    """
    Grab document lists for two terms. Iterate over both lists
    simulaneously to see if position of second term is 1 after
    first term.
    """

    results = list( )

    first  = self._normalize( first )
    second = self._normalize( second )

    firstTermDocs  = dict( )
    secondTermDocs = dict( )

    if first in self.index:
      firstTermDocs = self.index[first]
    if second in self.index:
      secondTermDocs = self.index[second]

    for firstDoc in firstTermDocs:
      if firstDoc not in secondTermDocs:
        continue  # Stop processing and go back to enclosing loop

      pointer = 0
      secondDocPositionList = secondTermDocs[firstDoc]
      for position in firstTermDocs[firstDoc]:
        while position > secondDocPositionList[pointer]:
          pointer += 1
          if pointer >= len( secondDocPositionList ):
            break
        if pointer >= len( secondDocPositionList ):
          break
        if position == secondDocPositionList[pointer]-1:
          if firstDoc not in results:
            results.append(firstDoc)

    return results

  def nearQuery ( self, first, second, distance=1 ):
    """
    Grab document lists for two terms. Iterate over both lists
    simulaneously to see if position of terms is within distance.
    """

    results = list( )

    first  = self._normalize( first )
    second = self._normalize( second )

    firstTermDocs  = dict( )
    secondTermDocs = dict( )

    if first in self.index:
      firstTermDocs = self.index[first]
    if second in self.index:
      secondTermDocs = self.index[second]

    for firstDoc in firstTermDocs:
      if firstDoc not in secondTermDocs:
        continue  # Stop processing and go back to enclosing loop

      pointer = 0
      secondDocPositionList = secondTermDocs[firstDoc]
      for position in firstTermDocs[firstDoc]:
        distanceDiff = secondDocPositionList[pointer] - position
        if abs( distanceDiff ) < distance:
          if firstDoc not in results:
            results.append(firstDoc)
        while position > secondDocPositionList[pointer]:
          pointer += 1
          if pointer >= len( secondDocPositionList ):
            pointer -= 1
            break
          distanceDiff = secondDocPositionList[pointer] - position
          if abs( distanceDiff ) < distance:
            if firstDoc not in results:
              results.append(firstDoc)


    return results



