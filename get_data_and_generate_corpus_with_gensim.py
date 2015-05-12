#Generate dat file with tweets
#@author Matias Hurtado - PUC Chile

#Import some modules for reading and getting data.
#If you don't have this modules, you must install them.
import csv
import MySQLdb
import re
import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
import os
from gensim import corpora, models, similarities #to create a dictionary

#Set years, this would be the timestamps
time_stamps = ['2009', '2010', '2011', '2012', '2013']
#Set the conference name to be analyzed
conference = ''

    
#DB MYSQL Connect. Put your credentials here.
db_host = 'localhost' #Host
db_user = 'user' #User
db_pass = 'password' #Password
db_database = 'twitter_conferences' #Database

##Conect...
db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass,db=db_database)

   
dat_outfile = open(os.path.join('data', conference, 'metadata.dat'), 'w')
dat_outfile.write('id\tdate\tcontent\n') #write header

tweets = list()
#Set total_tweets list per year, starting at 0
total_tweets_list = [0 for year in conferences_years]

#Analyze each year..

time_stamps_count = 0

for year in time_stamps: #For each year

    print('Analyzing year ' + str(year))
    
    #Set total_tweets to 0
    total_tweets = 0
    
    #Get tweets with mysql
    cursor = db.cursor()

    #Query
    query = "SELECT ttID, content, category_index FROM con_tweets_filtered WHERE conference = '" + conference + "' and category_index = " + year + " and relevant=1 and lang='en'"

    #Execute query
    cursor.execute(query)
    result = cursor.fetchall() #store results
    cursor.close()
    
    #For each result (tweet), get content and save it to the output file if it's not an empty line
    for line in result:

        #Remove @xxxx and #xxxxx
        content = [unicode(word.lower(), errors='ignore') for word in line[1].split() if word.find('@') == -1 and word.find('#') == -1 and word.find('http') == -1]
        
        #join words list to one string
        content = ' '.join(content)
        
        #remove symbols
        content = re.sub(r'[^\w]', ' ', content)
        
        #remove stop words
        content = [word for word in content.split() if word not in stopwords.words('english') and len(word) > 3 and not any(c.isdigit() for c in word)]
        
        #join words list to one string
        content = ' '.join(content)

        #Stemming and lemmatization
        lmtzr = WordNetLemmatizer()
        
        content = lmtzr.lemmatize(content)
        
        #Filter only nouns and adjectives
        tokenized = nltk.word_tokenize(content)
        classified = nltk.pos_tag(tokenized)

        content = [word for (word, clas) in classified if clas == 'NN' or clas == 'NNS' or clas == 'NNP' or clas == 'NNPS' or clas == 'JJ' or clas == 'JJR' or clas == 'JJS']
        #join words list to one string
        content = ' '.join(content)
        
        
        if len(content) > 0:
            tweets.append([line[0], content, line[2]])
            total_tweets += 1
            dat_outfile.write(str(line[0]) + '\t' + str(line[2]) + '\t' + content)
            dat_outfile.write('\n')
            
    #Add the total tweets to the total tweets per year list
    total_tweets_list[time_stamps_count] += total_tweets
            
    time_stamps_count+=1

dat_outfile.close() #Close the tweets file

#Write seq file
seq_outfile = open(os.path.join('data', conference, 'foo-seq.dat'), 'w')
seq_outfile.write(str(len(total_tweets_list)) + '\n') #number of TimeStamps

for count in total_tweets_list:
    seq_outfile.write(str(count) + '\n') #write the total tweets per year (timestamp)
    
seq_outfile.close()

print('Done collecting tweets and writing seq')



#Transform each tweet content to a vector.

stoplist = set('for a of the and to in'.split())

#Construct the dictionary

dictionary = corpora.Dictionary(line[1].lower().split() for line in tweets)

# remove stop words and words that appear only once
stop_ids = [dictionary.token2id[stopword] for stopword in stoplist
            if stopword in dictionary.token2id]
once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.iteritems() if docfreq == 1]
dictionary.filter_tokens(stop_ids + once_ids) # remove stop words and words that appear only once
dictionary.compactify() # remove gaps in id sequence after words that were removed

dictionary.save(os.path.join('data', conference, 'dictionary.dict')) # store the dictionary, for future reference

#Save vocabulary
vocFile = open(os.path.join('data', conference, 'vocabulary.dat'),'w')
for word in dictionary.values():
    vocFile.write(word+'\n')
    
vocFile.close()

print('Dictionary and vocabulary saved')


#Prevent storing the words of each document in the RAM
class MyCorpus(object):
     def __iter__(self):
         for line in tweets:
             # assume there's one document per line, tokens separated by whitespace
             yield dictionary.doc2bow(line[1].lower().split())


corpus_memory_friendly = MyCorpus()

multFile = open(os.path.join('data', conference, 'foo-mult.dat'),'w')

for vector in corpus_memory_friendly: # load one vector into memory at a time
    multFile.write(str(len(vector)) + ' ')
    for (wordID, weigth) in vector:
        multFile.write(str(wordID) + ':' + str(weigth) + ' ')

    multFile.write('\n')
    
multFile.close()

print('Mult file saved')
