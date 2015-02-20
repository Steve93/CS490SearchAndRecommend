"""
tokenize.py
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

TODO:
import string
#Remove Punctuation
        punct = set(string.punctuation)
        for x in tokenizedHTML:
           if x in punct:
               tokenizedHTML.remove(x)
TODO:
from nltk.tokenize import word_tokenize
from nltk.tokenize import wordpunct_tokenize

2/4/15-SM-New.
"""

# http://nlp.stanford.edu/software/tokenizer.shtml
# http://nlp.stanford.edu/software/tagger.shtml
# http://nlp.stanford.edu/software/lex-parser.shtml
from nltk.tokenize.stanford   import StanfordTokenizer
# TODO: try a regex togenizer nltk.tokenize.regexp
from nltk.tokenize.punkt      import PunktWordTokenizer
# from nltk.tokenize.texttiling import TextTilingTokenizer
from nltk.tokenize.treebank   import TreebankWordTokenizer
from nltk.tokenize            import word_tokenize


def stanfordTokenizer ( rawText ):
  """
  Uses Stanford University's natural language processing lab
    tokenizer to split raw text.
  """

  jarPath = "/usr/share/nltk_data/stanford-postagger.jar"
  stanfordOptions = {
    "americanize": True,
    "ptb3Escaping": False
  }

  stanfordTokenizer = StanfordTokenizer( jarPath, 'UTF-8', stanfordOptions )

  return stanfordTokenizer.tokenize( rawText )


def punktTokenizer ( rawText ):
  """
  Uses Punkt tokenizer to split raw text.
  """

  return PunktWordTokenizer( ).tokenize( rawText )


def treebankTokenizer ( rawText ):
  """
  Uses Treebank tokenizer to split raw text.
  """

  return TreebankWordTokenizer( ).tokenize( rawText )


def tokenize ( ipTokenizer, ipRawText ):
  """
  Chooses tokenizer based on input string. Tokenizes raw text.
  """

  if ( ipTokenizer == "Stanford" ):
    tokens = stanfordTokenizer( ipRawText )
  elif ( ipTokenizer == "Punkt" ):
    tokens = punktTokenizer( ipRawText )
  elif ( ipTokenizer == "Treebank" ):
    tokens = treebankTokenizer( ipRawText )
  elif ( ipTokenizer == "Word" ):
    tokens = word_tokenize( ipRawText )
  else:
    tokens = ipRawText.split( )

  return tokens

