from operator import itemgetter
def remove_header(rdd_file):
    header = rdd_file.first()
    return rdd_file.filter(lambda x: x!=header)

def parse_sub_with_rate(spark_context,submission):
    sub_rdd = (remove_header(spark_context.textFile(submission)).map(lambda x: x.split("\t"))
        .map(lambda x:(x[0],x[1].split(","))).map(lambda x:(x[0],[(i.split(" ")) for i in x[1]])))
    return sub_rdd


def compute_average(spark_context,submission_list,weight_list=None,top_K=30):
    ensemble_rdd = spark_context.parallelize([])
    weight_list = weight_list if weight_list!=None else [1/len(submission_list) for x in range(len(submission_list))]

    for submission_file,weight in zip(submission_list,weight_list):
        submission_rdd = parse_sub_with_rate(spark_context,submission_file)
        submission_rdd = submission_rdd.flatMap(lambda x: ([((x[0],i[0]),weight*float(i[1])) for i in x[1]]))
        ensemble_rdd = ensemble_rdd.union(submission_rdd)

    ensemble_rdd = (ensemble_rdd.reduceByKey(lambda x,y:x+y)
                .map(lambda x:(x[0][0],[(x[0][1],x[1])]))
                .reduceByKey(lambda x,y:x+y)
                .map(lambda x:(x[0],sorted(x[1],key=itemgetter(1),reverse=True)[:top_K])))
    return ensemble_rdd