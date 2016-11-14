# Methods for merging different submissions

from  more_itertools import unique_everseen
import csv
from operator import itemgetter

def recommendation_list(string):
    lista=[]
    split = string.split(',')
    for i in split:
        if i != '':
            lista.append(int(i))
    return list(set(lista))

def parse_submissions(sc, primary_path, secondary_path):
    primary_rdd = sc.textFile(primary_path)
    secondary_rdd = sc.textFile(secondary_path)
    
    header_1 = primary_rdd.first()
    header_2 = secondary_rdd.first()

    primary_rdd = (primary_rdd.filter(lambda x: x != header_1)
    .map(lambda x: x.split("\t"))
    .map(lambda x:(int(x[0]),
    recommendation_list(x[1])))
    )
    
    secondary_rdd = (secondary_rdd.filter(lambda x: x != header_2)
    .map(lambda x: x.split("\t"))
    .map(lambda x:(int(x[0]),recommendation_list(x[1])))
    )

    return primary_rdd,secondary_rdd


def append_submissions(sc,filepath_primary,filepath_secondary):
    """This method will simply fill the element of file_1 to reach the 30 values, without duplicates"""
    
    primary_rdd, secondary_rdd = parse_submissions(sc,filepath_primary,filepath_secondary)
    primary_list = primary_rdd.collect()
    secondary_list = primary_rdd.collect()
    
    merge_rdd = (primary_rdd.union(secondary_rdd)
                    .reduceByKey(lambda x,y:[i for i in x]+[i for i in y if i not in x])
                    .map(lambda x: (x[0], [(i,1) for i in x[1]][:30])))
    
    return merge_rdd


def substitute_submissions(sc,filepath_primary,filepath_secondary,n_items_primary,n_items_secondary):
    """This method will simply append to the first n elements of file_1 the first m elements of file_2"""
    if n_items_primary + n_items_secondary != 30:
        print "The sum of the given values is not 30"
        
    primary_rdd, secondary_rdd = parse_submissions(sc,filepath_primary,filepath_secondary)
    merge_rdd = (primary_rdd.union(secondary_rdd)
             .reduceByKey(lambda x,y:[(i,1) for i in x][:n_items_primary]+[(i,1) for i in y if i not in x][:n_items_secondary]))

    return merge_rdd

def append_submissions_ordered_graphlab(gl,filepath_primary,filepath_secondary,filename):
    """This method will simply fill the element of file_1 to reach the 30 values, without duplicates"""
    
    primary = gl.SFrame.read_csv(filepath_primary, sep="\t")
    secondary = gl.SFrame.read_csv(filepath_secondary, sep="\t")
    
    primary.rename({primary.column_names()[0]:"user_id",primary.column_names()[1]:"rec1"})
    secondary.rename({secondary.column_names()[0]:"user_id",secondary.column_names()[1]:"rec2"})
    
    merge = primary.join(secondary, how="outer", on="user_id")      

    merge["rec1"] = merge["rec1"].apply(lambda x : x.replace(" ","").split(","))
    merge["rec2"] = merge["rec2"].apply(lambda x : x.replace(" ","").split(","))

    merge["rec1"] = merge["rec1"].fillna([])
    merge["rec2"] = merge["rec2"].fillna([])
    
    merge["recommendation"] = merge.apply(lambda x: list(unique_everseen(x["rec1"]+ x["rec2"]))[:30])
    merge["recommendation"] = merge["recommendation"].apply(lambda x : str(x).replace("\"","").replace("[","") \
                                                            .replace("]","").replace("\'","").replace(" ",""))

    merge.remove_columns(['rec1','rec2'])
    merge.export_csv(filename, delimiter='\t', line_terminator='\n',quote_level=csv.QUOTE_NONE)
    
    return merge

def recommendation_list_with_value(string, value):
    lista=[]
    split = string.split(',')
    for i in split:
        if i != '':
            lista.append((int(i),value))
    return list(set(lista))

def parse_submission_with_value(sc, path, value):
    rdd = sc.textFile(path)
    
    header = rdd.first()

    rdd = (rdd.filter(lambda x: x != header)
    .map(lambda x: x.split("\t"))
    .filter(lambda x: x[0] != '')
    .map(lambda x:(int(x[0]),recommendation_list_with_value(x[1], value)))
    )

    return rdd


def append_submissions_3(sc,filepath_primary,filepath_secondary,filepath_ternary):
    """This method will simply fill the element of file_1 to reach the 30 values, without duplicates"""
    
    primary_rdd = parse_submission_with_value(sc,filepath_primary,3)
    secondary_rdd = parse_submission_with_value(sc,filepath_secondary,2)
    ternary_rdd = parse_submission_with_value(sc,filepath_ternary,1)
    
    merge_rdd = (primary_rdd.union(secondary_rdd)
                 .union(ternary_rdd)
                 .flatMap(lambda r: [(r[0], x) for x in r[1]])
                 .map(lambda x: ((x[0],x[1][0]),x[1][1]))
                 .reduceByKey(lambda x,y:x+y)
                 .map(lambda x: (x[0][0],[(x[0][1],x[1])]))
                 .reduceByKey(lambda x,y:x+y)
                 .map(lambda x: (x[0],sorted(x[1], key=itemgetter(1), reverse=True)[:30]))
                 )
    
    return merge_rdd