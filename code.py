
!pip install pyspark py4j

Degrees = ['Computer Science','Finance','Marketing','Chemistry','Accounting','Graphic Design','Anthropology','Food Technology'
        'Architecture','Biochemistry','Biology','Biomedical Engineering','Biotechnology','Civil Engineering','Electrical Engineering',
        'History','Journalism','Sports Management','Nursing','Psychology','Political Science','Mathematics','Physics',
        'IT','Fashion Design','Mechanical Engineering','Philosophy','Interior Design','Statistics','Sociology','Manufacturing Engineering',
        'Aeronautical Engineering','Aerospace Engineering','Retail Management','Naturopathy','Bachelor Of Surgery','law',
        'Physiotherapy', 'Business Administration','Environmental Science', 'Astronomy', 'Human Resource Management', 'Pharmacy','Graphic'
         'Occupational Therapy','Agricultural Engineering', 'Atmospheric Science', 'Events Management', 'Textile Engineering', 'Petroleum Engineering',     
      ]


import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark=SparkSession.builder.appName('local').getOrCreate()

sc=spark.sparkContext

rdd=sc.parallelize(Degrees)

rdd.count()

rdd.filter(lambda x: Degrees[19] in x).collect()

rdd.map(lambda x:x.upper()).collect()

rdd.groupBy(lambda x: x[0]).map(lambda x : (x[0], list(x[1]))).collect()

g=sc.textFile('./sample_data/data.txt')

import string
def tokenize(line):
    table = dict.fromkeys(map(ord, string.punctuation))
    return line.translate(table).lower().split()

"""Step by Step"""

words = g.flatMap(lambda line: tokenize(line))
words.take(10)

words = words.map(lambda x: (x, 1))
words.take(10)

counts = words.reduceByKey(lambda x, y: x+y)

"""Combine the above code into one piece of code """

(
g.flatMap(lambda line: tokenize(line))
                .map(lambda word: (word, 1))
               .reduceByKey(lambda x, y: x + y)
               .takeOrdered(10, key=lambda x: -x[1])
)

