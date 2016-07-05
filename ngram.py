# coding: utf-8

import csv
import re

from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem.snowball import SnowballStemmer


"""
import spss.pyspark.runtime
asContext = spss.pyspark.runtime.getContext()

if asContext.isComputeDataModelOnly():
        inputSchema = asContext.getSparkInputSchema()
        outputSchema = inputSchema
        asContext.setSparkOutputSchema(outputSchema)
else:
        inputData = asContext.getSparkInputData()
        outputData = inputData
        asContext.setSparkOutputData(outputData)

"""

STEMMER = SnowballStemmer('swedish')

NON_ALPHANUM = re.compile('[^\w\d]', re.U)
MULTISPACE = re.compile('\s\s+')


def chargram(xs):
    cv = CountVectorizer(ngram_range=(3, 3), max_df=0.5,
                         min_df=3, analyzer='char')
    t = cv.fit_transform(xs)
    extra_headers = cv.get_feature_names()
    return extra_headers, t


def wordgram(xs):
    cv = CountVectorizer(ngram_range=(1, 2), max_df=0.3,
                         min_df=3)
    t = cv.fit_transform(xs)
    extra_headers = cv.get_feature_names()
    return extra_headers, t


def clean(x):
    x = x.lower()
    x = NON_ALPHANUM.sub(' ', x)
    x = MULTISPACE.sub(' ', x)
    words = [STEMMER.stem(w) for w in x.split(' ')]

    return ' '.join(words)


def transform(fun, inf, outf):
    with open(inf) as f:
        c = csv.reader(f)
        headers = c.next()
        orig = list(c)
        xs = [clean(x[-1].decode('utf8')) for x in orig]
        extra_headers, data = fun(xs)

    print len(extra_headers)
    with open(outf, 'wt') as f:
        c = csv.writer(f)
        c.writerow(headers + [s.encode('utf8') for s in extra_headers])
        for i in xrange(len(orig)):
            extrarow = data[i].toarray()[0].tolist()

            c.writerow(orig[i] + extrarow)
