"""
Lab 1: Web-Mining for CS490: Search Engines and Recommender Systems - Spring 2015
http://jimi.ithaca.edu/CourseWiki/index.php/CS490_S15_Lab1

Spider Class Code for obtaining a website from a given URL
The HTML, Script, and other content is stripped (using BeautifulSoup4)
The raw content is tokenized (using NLTK)
The list of tokens is normalized by lowercasing and porter stemming (using NLTK)

Dependencies: you may need to download the nltk importer:
 import nltk
 nltk.download('all')

Code by Chris Kondrat (Modified by Doug Turnbull)
"""

__author__ = 'Chris Kondrat'
import urllib.request
import string

from bs4 import BeautifulSoup, Comment
from nltk import word_tokenize
from nltk.stem import *

class Spider:
    """
    Class to download a web page and then create a list of (normalized) tokens
    """

    def __init__(self):
        """
        Empty Class Constructor. (You will need to add more code here for lab 2)
        """
        self.tokens = []
        self.title = ""

    def parser(self, urlIn):
        """
        This function should strip out HTML tags, remove punctuation,
        and break up the string of character into tokens. Also,
        extract the title of the of the web page by finding the text between the <title> opening and </title> closing tags.

        :param urlIn: Raw HTML Page
        :return: age Title, List or Tokens
        """


        page = urllib.request.urlopen(urlIn)
        page = page.read()
        soup = BeautifulSoup(page)
        self.title = soup.title.string

        # Remove iframes, scripts and links tags
        [s.extract() for s in soup(['iframe','script','link'])]

        # Remove all comments
        comments = soup.findAll(text=lambda text:isinstance(text, Comment))
        [comment.extract() for comment in comments]

        # Convert downloaded page into readable / elegant format
        clearedHTML = soup.get_text()

        #Tokenize all HTML elements
        tokenizedHTML = word_tokenize(clearedHTML)

        #Remove Punctuation
        punct = set(string.punctuation)
        for x in tokenizedHTML:
           if x in punct:
               tokenizedHTML.remove(x)

        return tokenizedHTML

    def lowecase(self, tokensIn):
        """
        returns a lower case version of each token.
        :param tokensIn: List of Tokens
        :return: list of lowercased tokens
        """

        lowerTokens = []
        for i in range(len(tokensIn)):
            lowerTokens.append(tokensIn[i].lower())

        return lowerTokens

    def stem(self, tokensIn):
        """
        applies the Porter Stemmer to stem each token.
        :param tokensIn: list of tokens
        :return: list of stemmed tokens
        """

        porterTokens = []
        for i in tokensIn:
            porterTokens.append(PorterStemmer().stem_word(i))

        return porterTokens

    def getTerms(self, tokensIn):
        """
        returns a list of (unique) terms found in the list of input tokens.
        :param tokensIn: list of tokens
        :return: list of (unique) terms - the vocabulary of the document
        """

        unique = []
        unique = set(tokensIn)
        return unique


if __name__=='__main__':
    """
    sample driver code for Spider Class
    """

    # Create spider object
    mySpider = Spider()

    # Parsed in passed website
    parsed = mySpider.parser('http://www.amazon.com/The-Mountain-Three-Short-Sleeve/dp/B002HJ377A')
    print(mySpider.title)
    print('Number of Tokens: ' + str(len(parsed)))

    # Print out unique terms
    uniqueCapital = mySpider.getTerms(parsed)
    print('Number of Terms: ' + str(len(uniqueCapital)))

    # Print out unique terms after lowercasing
    lowercased = mySpider.lowecase(parsed)
    uniqueLower = mySpider.getTerms(lowercased)
    print('Number of Terms after lowercase: ' + str(len(uniqueLower)))

    # Print out Porter Stemmed unique terms
    porter = mySpider.stem(uniqueLower)
    uniqueStemmed = mySpider.getTerms(porter)
    print('Number of Terms after Porter Stemmer: ' + str(len(uniqueStemmed)))

    for term in uniqueStemmed:
        print(term)





