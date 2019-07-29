#!/usr/bin/env python
# coding: utf-8

# ## Notes
#     - Some docs are not in English, even though this criterion was always specified for each database search.

# In[2]:


"""
This module collates HTML exports of Proquest search result records in the parent directory and converts the collection into a table in a time-stamped Excel workbook.
"""

import datetime
import os
import re
import string
import pandas as pd
import numpy as np
import nltk
# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('wordnet')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup
from pathlib import Path
from lxml import html

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

# obtain data files
files = list()
for filename in Path('.').glob('**/?[Pro]*.html'):
    files.append(filename) 
files

# import list of features to filter docs
ftname = r"./features.txt"
with open(ftname, 'r', encoding='utf-8') as fh:
    ftrs = list()
    for line in fh:
        line = line.replace("\n","").replace(":",": ")
        ftrs.append(line)
fin_tab = list()
for i_ft, elem_ft in enumerate(ftrs):
    #iterate through each html source file
    for i_dt, elem_dt in enumerate(files):
        HtmlFile = open(elem_dt, 'r', encoding='utf-8')
        page = HtmlFile.read()
        # generate documents by splitting source file
        docs = page.split(
            """<div style="margin-bottom:20px;"""
            """border-bottom:2px solid #ccc;padding-bottom:5px">""")
        # delete table of contents and cover page
        docs.pop(0)     
        # iterate through each document 
        for i_dc, elem_dc in enumerate(docs):
            # remove all simple paragraph tags
            x = elem_dc #.replace("<p>","").replace("</p>","")
            div = html.fromstring(x) # read text as html
            # get list of available features:
            doc_ftrs = [x.text for x in (div.xpath('//strong'))]
            # get index of iteration feature in available features list
            doc_ftrs
            # append html text for iteration feature to final table
            try:
                fin_tab.append(
                    div.xpath('//strong')[doc_ftrs.index(ftrs[i_ft])]
                    .xpath("./following::text()[1]")[0])
            except:
                fin_tab.append("NA") 
    print("feature:", i_ft, end="  ")
print("original files:", i_dt+1)

# alternative/additional (unnamed) features
for i_dt, elem_dt in enumerate(files):
    HtmlFile = open(elem_dt, 'r', encoding='utf-8')
    page = HtmlFile.read()
    # generate documents by splitting source file
    docs = page.split(
        """<div style="margin-bottom:20px;"""
        """border-bottom:2px solid #ccc;padding-bottom:5px">""")
    # delete table of contents and cover page
    docs.pop(0) 
    for i_dc, elem_dc in enumerate(docs):
        # remove all simple paragraph tags
        soup = BeautifulSoup(elem_dc)
        h4s = soup.find_all("text")
        try:
            fin_tab.append(h4s[0].text)
        except:
            fin_tab.append("NA")

# alternative/additional (unnamed) features: Doc Titles
for i_dt, elem_dt in enumerate(files):
    HtmlFile = open(elem_dt, 'r', encoding='utf-8')
    page = HtmlFile.read()
    # generate documents by splitting source file
    docs = page.split(
        """<div style="margin-bottom:20px;"""
        """border-bottom:2px solid #ccc;padding-bottom:5px">""")
    # delete table of contents and cover page
    docs.pop(0) 
    for i_dc, elem_dc in enumerate(docs):
        # remove all simple paragraph tags
        soup = BeautifulSoup(elem_dc)
        h4s = soup.find_all("p")
        try:
            fin_tab.append(h4s[1].text)
        except:
            fin_tab.append("NA")
            
# append list of alternative/additional (unnamed) features for output
ftname = r"./featuresx_fintab.txt"
with open(ftname, 'r', encoding='utf-8') as fh:
    for line in fh:
        line = line.replace("\n","").replace(":",": ")
        ftrs.append(line)
        
# set safe dataframe names
ftrs = [x.replace(": ","").replace(" ","_") for x in ftrs]


# In[3]:


# export to excel       
n_obs = int((len(fin_tab)/len(ftrs)))
fin_tab = chunks(fin_tab, n_obs)
fin_tab = list(fin_tab)
fin_tab[3][0:4]

print("obs:", n_obs)

# Populate columns of a dataframe by feature
df = pd.DataFrame(fin_tab[0], columns = [ftrs[0]])
for i, elem in enumerate(ftrs):
    df[ftrs[i]] = fin_tab[i]


# In[4]:


df.to_excel("proquest_data_" 
            + str(datetime.datetime.now())[0:19].replace(":","_") 
            + ".xlsx")
df.head(n=4)


# In[5]:


# obtain tabular data files
files = list()
for filename in Path('.').glob('**/?[Prq]*.xlsx'):
    files.append(filename) 
latest_file = max(files, key=os.path.getctime)
latest_file


# In[6]:


# set stop words
en_stops = set(stopwords.words('english'))

# cleanup dataset
df1 = pd.read_excel(latest_file)
df1 = df1.drop(df1.columns[0], axis=1)
print(df1.shape)


# In[7]:


# drop documents with type "wire feeds"
df1 = df1[df1["Source_type"] != "Wire Feeds"]
print(df1.shape)
# drop blank documents
df1 = df1[df1["Full_Text2"].notnull()]
print(df1.shape)
# strip document trailing and leading whitespace
df1["Full_Text2"] = df1["Full_Text2"].str.strip()

# drop "caption only" documents
df1 = df1[df1["Full_Text2"].str.lower().str.count(
    "caption text only")==0]
print(df1.shape)
# keep documents that mention carnival more than once;
# then save word count
df1 = df1[df1["Full_Text2"].str.lower().str.count(
    "carnival|carnaval|carnavale")>1]
df1["carnival_count"] = df1["Full_Text2"].str.lower().str.count(
    "carnival|carnaval|carnavale")
print(df1.shape)
# keep documents with more than 300 characters;
# save character count
df1 = df1[df1["Full_Text2"].str.lower().str.len()>300]
df1["char_count"] = df1["Full_Text2"].str.lower().str.len()
print(df1.shape)
# create duplicate for comparison 
df1["doc"] = df1["Full_Text2"]

# extricate non-english documents
df_foreign = df1[df1["doc"].str.lower().str.count(
    "algun|cosas|tener|algumas|coisas")>=1]
df_foreign.to_excel("foreign_lang_data_" 
            + str(datetime.datetime.now())[0:19].replace(":","_") 
            + ".xlsx")
df1 = df1[df1["doc"].str.lower().str.count(
    "algun|cosas|tener|algumas|coisas")<1]
print(df1.shape)

# Extract additional helpful features
df1["Country"] = df['Country_of_publication'].str.extract('^(.+?),')
df1["Country"] = df1["Country"].str.replace("United Sta tes","United States")
df1["Country"] = df1["Country"].str.replace("New Yor k","United States")
df1["Country"] = df1["Country"].str.replace("London","United Kingdom")

## Year
df1['Publication_date'] = df1['Publication_date'].str.replace("201 8","2018")
df1["Year"] = df1['Publication_date'].str.extract('(\d{4})')
# df1["Year"] = df1['Publication_date'].str.extract(',(.+)')


# export R-ready dataset
df1.to_excel("R_ready_data" 
            + str(datetime.datetime.now())[0:19].replace(":","_") 
            + ".xlsx")


# In[8]:


# further pythonic pre-processing

# remove numbers 
df1["doc"] = [re.sub(r"\d+", "", doc, flags=re.MULTILINE) for doc in df1["doc"]]

# remove URLS
df1["doc"] = [re.sub(r"www\S+", "", doc, flags=re.MULTILINE) for doc in df1["doc"]]

# # remove punctuation, leaving apostrophied possessive 
# # and hyphenated words intact; make lowercase
# df1["doc"] = df1['doc'].apply(lambda x: " ".join(
#     [word.strip(string.punctuation) for word in x.split(" ")]).strip()).str.lower()

# remove punctuation; make lowercase
df1["doc"] = df1["doc"].str.replace('[^\w\s]','').str.lower()


# In[9]:


# drop stop words
df1["doc"] = df1['doc'].apply(lambda x: ' '.join(
    [word for word in x.split() if word not in (en_stops)]))
print(df1.shape)

# lemmatize words in each document
wordnet_lemmatizer = WordNetLemmatizer()
df1["docl"] = df1["doc"].apply(lambda x: ' '.join(
    [wordnet_lemmatizer.lemmatize(word) for word in nltk.word_tokenize(x)]))
print(df1.shape)

# MEM INTENSIVE: remove words that only appear once across the corpus
count_1 = sum(pd.Series(' '.join(df1["docl"]).split()).value_counts()==1)
freq = pd.Series(' '.join(df1["docl"]).split()).value_counts()[-count_1:]
freq = list(freq.index)
df1["docf"] = df1["docl"].apply(lambda x: " ".join(x for x in x.split() if x not in freq))
print(df1.shape)

# Remove words highly common across documents

freq = pd.Series(' '.join(df1["docl"]).split()).value_counts()[:25]
freq = list(freq.index)
df1["docf"] = df1["docf"].apply(lambda x: " ".join(x for x in x.split() if x not in freq))

# # repeat remove punctuation, leaving apostrophied possessive 
# # and hyphenated words intact; make lowercase
# df1["docl"] = df1['docl'].apply(lambda x: " ".join(
#     [word.strip(string.punctuation) for word in x.split(" ")]).strip()).str.lower()

df1.head(n=2)


# In[10]:


df1.to_excel("filtered_data_" 
             + str(datetime.datetime.now())[0:19].replace(":","_") 
            + ".xlsx")
df1.to_csv("filtered_data_" 
             + str(datetime.datetime.now())[0:19].replace(":","_") 
            + ".csv")

