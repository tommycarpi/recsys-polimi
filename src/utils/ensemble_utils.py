import numpy as np
from operator import itemgetter

def recommendation_list_with_value_and_decay(string, value, decay):
    updating_value = value
    lista=[]
    split = string.split(',')
    for i in split:
        if i != '':
            updating_value -= decay
            lista.append((int(i),updating_value))
            
    return lista

def psv_decay(sc, path, value, decay=0.0001):
    rdd = sc.textFile(path)
    
    header = rdd.first()

    rdd = (rdd.filter(lambda x: x != header)
    .map(lambda x: x.split("\t"))
    .filter(lambda x: x[0] != '')
    .map(lambda x:(int(x[0]),recommendation_list_with_value_and_decay(x[1], value, decay)))
    )

    return rdd

def reduce_submissions(sc,submissions_list, topk):
    
    merge_rdd = sc.emptyRDD()
    
    for s in submissions_list:
        ls_rdd = s
            
        merge_rdd = merge_rdd.union(ls_rdd)
            
    
    merge_rdd = (merge_rdd.flatMap(lambda r: [(r[0], x) for x in r[1]])
                 .map(lambda x: ((x[0],x[1][0]),x[1][1]))
                 .reduceByKey(lambda x,y:x+y)
                 .map(lambda x: (x[0][0],[(x[0][1],x[1])]))
                 .reduceByKey(lambda x,y:x+y)
                 .map(lambda x: (x[0],sorted(x[1], key=itemgetter(1), reverse=True)[:topk]))
                 )
    
    return merge_rdd

def blend_submissions(sc,submissions_list,topk):
    
    merge_rdd = sc.emptyRDD()
    
    for s in submissions_list:
        ls_rdd = s
        merge_rdd = merge_rdd.union(ls_rdd)
    
    
    merge_rdd = (merge_rdd.flatMap(lambda r: [(r[0], x) for x in r[1]])
                 .map(lambda x: ((x[0],x[1][0]),x[1][1]))
                 .reduceByKey(lambda x,y:max(x,y))
                 .map(lambda x: (x[0][0],[(x[0][1],x[1])]))
                 .reduceByKey(lambda x,y:x+y)
                 .map(lambda x: (x[0],sorted(x[1], key=itemgetter(1), reverse=True)[:topk]))
                 )
    
    return merge_rdd

def recommendation_list_with_value_and_exp_decay(string,number_of_item, value, decay):
    updating_value = value
    lista=[]
    split = string.split(',')
    rank=1
    for i in split[:number_of_item]:
        if i != '':
            updating_value = value**(1 - decay*(rank-1) )
            rank +=1
            lista.append((int(i),updating_value))
            
    return lista

def psv_exp_decay(sc, path, value, decay=0.0001):
    rdd = sc.textFile(path)
    
    header = rdd.first()

    rdd = (rdd.filter(lambda x: x != header)
    .map(lambda x: x.split("\t"))
    .filter(lambda x: x[0] != '')
    .map(lambda x:(int(x[0]),recommendation_list_with_value_and_exp_decay(x[1], len(x[1]), value, decay)))
    )

    return rdd

def from_string_to_list(stringa,kNN=20):
    lista_stringa = stringa.replace("[","").replace("]","").replace("(","").split("),")
    lista_stringa = [i.replace(")","").split(", ") for i in lista_stringa]
    final=[]
    for tup in lista_stringa:
        if tup[0]!='':# or tup[1]!='':
            final.append((int(tup[0]),float(tup[1])))
    
    return sorted(final,key=itemgetter(1),reverse=True)[:kNN]

def rdd_no_header(rdd):
    header = rdd.first()
    return rdd.filter(lambda x: x!=header)

def get_sim_dict(sim_rdd):
    return dict(rdd_no_header(sim_rdd)
                .map(lambda x:x.split("\t"))
                .filter(lambda x:x[0]!='' or x[1]!='')
                .map(lambda x:(int(x[0]),from_string_to_list(x[1], KNN )))
                .collect()
               )

def map_list(items):
    rec_list = []
    for i in range(0,len(items)):
        rec_list.append((items[i][0], i+1))
    return rec_list

def map_dict(items):
    return dict(items)