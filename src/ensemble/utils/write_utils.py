# Methods to prepare the submission file

def prepareSubmission(recommendationForChallengeRdd,outFile):
    #x[0]: user_id, x[1]: (item_id, score) -> x[1][i][0] takes all item_ids predicted for a give user
    recLines = recommendationForChallengeRdd.map(lambda x: (str(x[0]) + "\t" + str(",".join([str(x[1][i][0]) for i in range(len(x[1]))]) ))).collect()
    out = open(outFile+'.csv',"w")
    out.write("user_id\trecommended_items\n")#headers
    for recLine in recLines:
        out.write(recLine + "\n")
    out.close()
    print "Submission file %s created successfully" % outFile
