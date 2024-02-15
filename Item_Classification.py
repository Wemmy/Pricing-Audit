from gensim import corpora
from gensim import models
from gensim import similarities
from nltk.corpus import stopwords
import nltk
from gensim.models.phrases import Phrases
from collections import Counter
import spacy
import re
import pandas as pd

class MyCorpus:
    def __init__(
            self, 
            data_train, 
            data_test, 
            train_name_column = 'Full_Name_List', 
            test_name_column = 'Full_Name_Pos', 
            train_price_column = 'Item_Price', 
            test_price_column= 'singleitemprice',
            train_size_column = 'Size_List', 
            test_size_column = 'Size_Pos'
        ) -> None:
        '''
        train is list
        test is pos
        '''
        self.data_train = data_train.reset_index(drop=True)
        self.data_test = data_test.reset_index(drop=True)
        self.train_price_column = train_price_column
        self.test_price_column = test_price_column
        self.train_name_column = train_name_column
        self.test_name_column = test_name_column
        self.train_size_column = train_size_column
        self.test_size_column = test_size_column
        self.stop_words = set(stopwords.words('english'))
        self.stop_words.update(['plus','regular', 'pk', 'ea', 'pc', 'btl','can'])

    def create_dictionary(self):
        self.texts = [[word for word in row.split() if word not in self.stop_words and len(word)>1 ] for row in self.data_train[self.train_name_column]]
        # optional: add phrase to dictionary
        self.nlp = spacy.load('en_core_web_sm')
        text_phrase = [[str(phrase) for phrase in self.nlp(row).noun_chunks if len(phrase.text.split()) > 1 and len(phrase.text.split()) < 3] for row in self.data_train[self.train_name_column]]
        self.texts = [sublist1 + sublist2 for sublist1, sublist2 in zip(self.texts, text_phrase)]
        self.dictionary = corpora.Dictionary(self.texts)

    def create_model(self):
        self.corpus_train =[self.dictionary.doc2bow(text) for text in [[word for word in row.split()] for row in self.data_train[self.train_name_column]]]
        # optional: down weight by tf-idf
        self.tfidf_model = models.TfidfModel(self.corpus_train)
        self.corpus_train = self.tfidf_model[self.corpus_train]

        # optional: down weight by word tag
        self.corpus_train = self.deweight_by_wordtag(self.dictionary,  self.corpus_train)
        self.model = models.LsiModel(self.corpus_train, id2word=self.dictionary, num_topics=len(self.data_train))
        self.index = similarities.MatrixSimilarity(self.model[self.corpus_train])
    
    def create_index_text(self):
        self.texts_test = [[word for word in row.split() if word not in self.stop_words and len(word)>1] for row in self.data_test[self.test_name_column]]
         # optional: add phrase to dictionary
        text_phrase = [[str(phrase) for phrase in self.nlp(row).noun_chunks if len(phrase.text.split())>1 and len(phrase.text.split()) < 3] for row in self.data_test[self.test_name_column]]
        self.texts_test = [sublist1+ sublist2 for sublist1, sublist2 in zip(self.texts_test, text_phrase)]

        self.df_pos_bow = [self.dictionary.doc2bow(text) for text in self.texts_test]
        self.df_pos_index = [sorted(enumerate(self.index[self.model[doc]]), key=lambda item: -item[1])[:10] for doc in self.df_pos_bow]
    
    def find_best_match(self):
        '''Pay attention to the harded-coded price column!!!!!!!'''
        self.idx_matched = []
        self.sim_matched = []
        for idx_pos, row in self.data_test.iterrows():
            size_pos = row[self.test_size_column]
            price_pos = row[self.test_price_column]
            text_pos = self.texts_test[idx_pos]
            max_score = 0
            max_idx = 0
            for (idx_list, sim) in self.df_pos_index[idx_pos]:
                # compute the score
                if sim == 0:
                    break
                size_list = self.data_train[self.train_size_column][idx_list]
                text_list = self.texts[idx_list]
                
                # penalty on size
                if size_pos and size_list:
                    if self.compare_unit(size_pos, size_list):
                        score = sim
                    else:
                        score = max(0, sim - 0.5)
                else:
                    # check special size 
                    list_special_size = {'small', 'large', 'medium'}
                    special_size_pos = list_special_size & set(text_pos)
                    special_size_list = list_special_size & set(text_list)
                    if special_size_pos and special_size_list:
                        if special_size_pos == special_size_list:
                            score = sim
                        else:
                            score = max(0, sim - 0.5)
                    else:
                        # compare price
                        price_list = self.data_train[self.train_price_column][idx_list]
                        if  price_list * 0.7  < price_pos < price_list * 1.3:
                            score = sim
                        else:
                            score = max(0, sim - 0.5)
                
                # penalty on OOV
                oov_count = 0
                for token in text_pos:
                    if token not in self.dictionary.values():
                        oov_count += 1
                penalty_factor = (0.5 + 0.5 * (1 - oov_count/len(text_list)))
                score = score * penalty_factor

                # record score
                if score > max_score:
                    max_score = score
                    max_idx = idx_list
            self.idx_matched += [max_idx]
            self.sim_matched += [max_score]
    
    def construct_result(self):
        self.result=self.data_test
        self.result['matched_index'] = self.idx_matched
        self.result['similarity'] = self.sim_matched
        self.result = pd.merge(self.result, self.data_train, left_on='matched_index', right_index= True, how='left')

    @staticmethod
    def compare_unit(s1, s2):
        if s1 == s2:
            return True
        if s1 and s2:
            u1 = re.findall('\d+(\D+)', s1)[0] 
            u2 = re.findall('\d+(\D+)', s2)[0]
            if u1 == u2:
                return False
            # to ml
            if u1 == 'oz' and u2 == 'g':
                num_size_1 = int(re.findall('(\d+)oz', s1)[0])
                num_size_2 = int(re.findall('(\d+)g', s2)[0])
                if num_size_2*0.9 < num_size_1*28.35 < num_size_2*1.1:
                    return True
            if u1 == 'oz' and u2 == 'ml':
                num_size_1 = int(re.findall('(\d+)oz', s1)[0])
                num_size_2 = int(re.findall('(\d+)ml', s2)[0])
                if num_size_2*0.9 < num_size_1*28.41 < num_size_2*1.1:
                    return True
        else:
            return True
    
    @staticmethod
    def deweight_by_wordtag(corpus_dict,  corpus_bow, tag_weight_dict = {'NN': 2, 'NNS': 2, 'CD': 1, 'JJ': 1, 'VBN': 0.5, 'CC': 0.1, 'RB': 0.5, 'VB': 0.5, 'VBD': 0.5, 'MD': 0.1, 'VBG': 0.5}):
        tagged_dict = {}
        for key, value in corpus_dict.items():
            tagged_words = nltk.pos_tag([value])
            tagged_dict[key] = tagged_words[0][1]

        result = []
        for sublist in corpus_bow:
            multiplied_sublist = [(word, weight * tag_weight_dict[tagged_dict[word]]) for word, weight in sublist]
            result.append(multiplied_sublist)
        return result