"""
configurationLoader.py
===============================================================================
Stephen Meyerhofer
===============================================================================
NOTE:
  
2/10/15-SM-Modularized lab 2 and commented.
2/10/15-SM-New.
"""

import json

from argparse import ArgumentParser


def readCmdLine ( ):
  """
  Reads the following command Line Arguments (all optional):
    -d, --delete >> Deletes cache. Either only the clean directory, or all.
    -t, --tokenizer >> Choose between the Stanford, Punkt, Treebank, or
      Whitespace tokenizers. Defaults to Whitespace tokenizer.
    -s, --stemmer >> Choose between the Porter, Lancaster, English, or
      WordNetLemmatizer stemmers. Defaults to Porter stemmer.
  """

  argParser = ArgumentParser( description='Tokenize and Stem a WebPage.' )

  argParser.add_argument( '-d', '--delete',
                          help="delete the cache",
                          default=None,
                          choices=[ "clean", "all" ] )

  argParser.add_argument( '-r', '--restore',
                          help="restore from cached documents",
                          default=False,
                          action="store_true" )

  argParser.add_argument( '-t', '--tokenizer',
                          help="tokenizer to use on html text",
                          default="Whitespace",
                          choices=[ "Stanford", "Punkt",
                                    "Treebank", "Word",
                                    "Whitespace" ] )

  argParser.add_argument( '-s', '--stemmer',
                          help="stemmer to use on tokens",
                          default="Porter",
                          choices=[ "Porter", "Lancaster",
                                    "English", "WordNetLemmatizer" ] )

  return argParser.parse_args( )


def loadJsonFile ( jsonFileName ):
  """
  Loads Json file with specified name and stores into dictionary.
  """

  data = ""
  with open( jsonFileName, 'r' ) as f:
    data = f.read( ).replace( '\n', '' )
  
  return json.loads( data )


def dictToTuplePairsList( ipDict ):
  """
  Converts a dictionary to a list of tuple pairs. (key,value)
  """

  tuplePairsList = []

  for key in ipDict:
    tuplePairsList.append( ( key, ipDict[key] ) )

  return tuplePairsList


