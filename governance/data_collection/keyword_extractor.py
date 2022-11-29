
import os
from dotenv import load_dotenv

import github_md_parser

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer

class KeywordExtractor():
    '''This class is used to extract keywords from a given document.
    This becomes useful when we don't want to limit ourselves to a set of
    keywords that are associated with governance. The sample document to 
    be used should be a comprehensive document(s) on project governance 
    such as  the governance documentation from NumPy project. A more 
    comprehensive approach would require a corpus of governance related 
    documents and a keyworde extraction on the corpus.'''

    def __init__(self, doc):
        self.doc = doc
    
    def extract_keywords(self):
        '''Extracts keywords from a given document.
        Returns a list of keywords.'''

        n_gram_range = (1, 1)
        stop_words = "english"

        # Extract candidate words/phrases
        count = CountVectorizer(ngram_range=n_gram_range, stop_words=stop_words).fit([self.doc])

        # Candidates variable is a list of strings that includes candidate keywords.
        candidates = count.get_feature_names_out()
        return candidates
    
    def doc_embedding(self, doc):
        '''Returns sentence- and document-level embeddings 
        using `sentence-transformer` package and distilbert model.'''

        model = SentenceTransformer('distilbert-base-nli-mean-tokens')
        doc_embedding = model.encode([doc])
        candidate_embeddings = model.encode(candidates)
        return doc_embedding, candidate_embeddings
    
    def cosine_similarity(self, doc_embedding, candidate_embeddings):
        '''Returns a list of cosine similarity scores for top 5 candidate.'''

        top_n = 5
        distances = cosine_similarity(doc_embedding, candidate_embeddings)
        keywords = [candidates[index] for index in distances.argsort()[0][-top_n:]]
        return keywords

if __name__ == '__main__':
    
    with open('sample-governance-doc.txt') as f:
        doc = f.readlines()
        doc = ''.join(doc)
    extractor = KeywordExtractor(doc)
    keywords = extractor.extract_keywords()
    print(keywords)
