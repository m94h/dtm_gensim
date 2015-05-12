import os
import csv
import tethne


### GENERATE CONFERENCES LIST AND YEARS ###

#Get conferences names to be analized.
conference = ''

#Create file to export
with open('OutputDTM.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    #Write header
    writer.writerow(['Conference', 'TopicID', 'Word', 'Year', 'Probability'])

    
    #Make DTM
    os.system('dtm-win64.exe ./main --ntopics=3 --mode=fit --rng_seed=0 --initialize_lda=true --corpus_prefix=data/' + conference + '/foo --outname=data/' + conference + '/output --top_chain_var=0.9 --alpha=0.01 --lda_sequence_min_iter=6 --lda_sequence_max_iter=20 --lda_max_em_iter=20')

    #Import to tethne
    dtm = tethne.model.corpus.dtmmodel.from_gerrish('data/' + conference + '/output/','data/' + conference + '/metadata.dat','data/' + conference + '/vocabulary.dat')

    for i in range(3):
        arr = dtm.topic_evolution(i,10)
        
        for key in arr[1].keys():
            for year_i in range(5):
                
                writer.writerow([conference, i, key, (year_i + 2009), arr[1][key][year_i]])
        
