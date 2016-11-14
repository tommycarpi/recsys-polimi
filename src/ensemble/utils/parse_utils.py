# Methods for parsing the data

from collections import namedtuple
from operator import itemgetter
from datetime import datetime
import time
import pickle
import json
import math
import random
ItemDataRow = namedtuple("itemdata", ["itemId","industryId", "disciplineId","careerlevelId","country","latitude","longitude","region","employment","ts","titleIds","tagIds","active_during_test"])
UserDataRow = namedtuple("userdata", ["userId", "rolesIds", "careerlevelId","disciplineId","industryId","country","region","experience_n_entries_class","experience_years_experience","experience_years_in_current","edu_degree","edu_fieldofstudies"])
InteractionRow = namedtuple("interaction", ["userId", "itemId", "ts","interactionType"])

def map_country_to_region (country):
    if country=="non_dach":
        return 19
    if country=="ch":
        return 17
    if country=="at":
        return 18



def parseUserData(sc,filepath,number_partition=32,skipFirstLine=True):
    rawRdd = sc.textFile(filepath,number_partition)
    if skipFirstLine:
        header = rawRdd.first()
        rawRdd = rawRdd.filter(lambda x: x != header)
        print rawRdd.count()
        return rawRdd.map(lambda line: line.replace("NULL","0")).map(lambda line: line.split("\t")).map(lambda x: (int(x[0]),UserDataRow(
        userId = int(x[0]),
        rolesIds = [int(i) for i in x[1].split(",")],
        careerlevelId = 3 if len(x[2]) == 0 or int(x[2])==0 else int(x[2]),
        disciplineId = int(x[3]),
        industryId = int(x[4]),
        country = x[5],
        region = int(x[6]) if x[5]=="de" else (18 if x[5]=="at" else (19 if x[5]=="non_dach" else 17)),
        experience_n_entries_class = int(x[7]),
        experience_years_experience = int(x[8]),
        experience_years_in_current = int(x[9]),
        edu_degree = int(x[10]) if x[10]!="" else 0,
        edu_fieldofstudies = [] if x[11] == "" else [int(i) for i in x[11].split(",")]
    )))
def parseItemData(sc,filepath,number_partition=32,skipFirstLine=True):
    rawRdd = sc.textFile(filepath,number_partition)
    if skipFirstLine:
        header = rawRdd.first()
        rawRdd = rawRdd.filter(lambda x: x != header)

    return rawRdd.map(lambda line: line.replace("NULL","0")).map(lambda line: line.split("\t")).map(lambda x: (int(x[0]),ItemDataRow(
        itemId=int(x[0]),
        industryId=int(x[4]),
        disciplineId = int(x[3]),
        careerlevelId= 3 if len(x[2]) == 0 else int(x[2]),
        country=x[5],
        latitude=float(x[7]),
        longitude=float(x[8]),
        region=int(x[6]) if x[5]=="de" else (18 if x[5]=="at" else (19 if x[5]=="non_dach" else 17)),
        employment=int(x[9]),
        ts=int(x[11]),
        titleIds = [int(i) for i in x[1].split(",") if i != ""],
        tagIds = [int(i) for i in x[10].split(",") if i != ""],
        active_during_test = int(x[12])
    )))
def parseTargetUsers(sc,filepath,number_partition=32,skipFirstLine=True):
    rawRdd = sc.textFile(filepath,number_partition)
    if skipFirstLine:
        header = rawRdd.first()
        rawRdd = rawRdd.filter(lambda x: x != header)
    return rawRdd.map(lambda x: int(x))

def parseInteractions(sc,filepath,number_of_partions=32,skipFirstLine=True):
    rawRdd = sc.textFile(filepath,number_of_partions)
    if skipFirstLine:
        header = rawRdd.first()
        rawRdd = rawRdd.filter(lambda x: x != header)

    return rawRdd.map(lambda line: line.split("\t")).map(lambda x: InteractionRow(
        userId=int(x[0]),itemId=int(x[1]),ts=int(x[3]),interactionType=int(x[2])))

ImpressionRow = namedtuple("impression", ["year","week","userId", "itemIds"])
def parseImpressions(sc,filepath,partition=32,skipFirstLine=True):
    rawRdd = sc.textFile(filepath,partition)
    if skipFirstLine:
        header = rawRdd.first()
        rawRdd = rawRdd.filter(lambda x: x != header)

    return rawRdd.map(lambda x: x.split("\t")).map(lambda x: ImpressionRow(
        year = int(x[1]),
        week = int(x[2]),
        userId = int(x[0]),
        itemIds = [int(i) for i in x[3].split(",")]
    ))
