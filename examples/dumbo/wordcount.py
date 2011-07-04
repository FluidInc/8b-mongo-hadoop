import dumbo

def mapper(key, value):
    for word in value.split(" "):
        yield str(word), 1

def reducer(key, values):
    yield key, sum(values)

if __name__ == "__main__":
    job = dumbo.Job()
    job.additer(mapper, reducer)
    job.run()
    
    '''TO RUN:
    dumbo start wordcount.py -hadoop /usr/lib/hadoop -libjar /usr/lib/hadoop/mongo-hadoop.jar -libjar /usr/lib/hadoop/mongo-java-driver-2.6.3.jar \
    -inputformat com.mongodb.hadoop.typedbytes.MongoInputFormat -input mongodb://dashboard.dev/test.in \
    -outputFormat com.mongodb.hadoop.typedbytes.MongoOutputFormat -output test.out -conf mongo-wordcount.xml
    '''