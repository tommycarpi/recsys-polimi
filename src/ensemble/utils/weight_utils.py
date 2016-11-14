# Returns the dictionaries containing the weights for each user

def get_weight_dict(sc,filepath,target_user_list, _type):
    occ_rdd = sc.textFile(filepath,32)
    occ_first = occ_rdd.first()
    dict_weight = dict((occ_rdd.filter(lambda x: x != occ_first).map(lambda x: x.split(','))
		  .map(lambda x: (int(x[0]),[(_type(x[1]),int(x[2]))]))
                  .reduceByKey(lambda x,y: x+y)
                  .map(lambda x: (x[0],dict(x[1])))
                  .collect()))
    for user in dict_weight:
        maxValue = float(max(dict_weight[user].values()))
        for tag in dict_weight[user]:
            dict_weight[user][tag] = dict_weight[user][tag]/maxValue
    for user in target_user_list:
        if user not in dict_weight:
            dict_weight[user]={}
    return dict([(user,dict_weight[user]) for user in target_user_list])
