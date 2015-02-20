"""
stem.py
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

2/4/15-SM-New.
"""

from nltk.stem.porter    import PorterStemmer
from nltk.stem.lancaster import LancasterStemmer
# from nltk.stem.regexp    import RegexpStemmer
from nltk.stem.snowball  import EnglishStemmer
from nltk.stem.wordnet   import WordNetLemmatizer


def constructStemmer ( ipStemmer ):
  """
  Return stemmer with a stem function based on input string.
  """

  stemmer  = None

  if ( ipStemmer == "Lancaster" ):
    stemmer  = LancasterStemmer( )
  elif ( ipStemmer == "English" ):
    stemmer  = EnglishStemmer( )
  elif ( ipStemmer == "WordNetLemmatizer" ):
    stemmer      = WordNetLemmatizer( )
    stemmer.stem = stemmer.lemmatize
  else:
    stemmer  = PorterStemmer( )

  return stemmer

