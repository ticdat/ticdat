from textblob import TextBlob
import pandas
import sklearn
import cPickle
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import SVC, LinearSVC
from sklearn.metrics import classification_report, f1_score, accuracy_score, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import StratifiedKFold, cross_val_score, train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.learning_curve import learning_curve
from ticdat import TicDatFactory


dataFactory = TicDatFactory(messages = [[],["label", "message"]],
                            parameters = [["key"], ["value"]])

soln = TicDatFactory(predictions = [[],["label", "prediction"]],
                     parameters = [["key"], ["value"]])


def split_into_tokens(message):
    message = unicode(message, 'utf8')  # convert bytes into proper unicode
    return TextBlob(message).words


def split_into_lemmas(message):
    message = unicode(message, 'utf8').lower()
    words = TextBlob(message).words
    # for each word, take its "base form" = lemma
    return [word.lemma for word in words]


def run(td):
    assert dataFactory.good_tic_dat_object(td)
    messages = dataFactory.copy_to_pandas(td).messages
    params = dict({"mode":"predict"}, **{k:r["value"] for k,r in td.parameters.items()})
    assert params["mode"] in ["fit", "predict"]