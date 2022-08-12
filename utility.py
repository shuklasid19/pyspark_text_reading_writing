from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading text file to dataframe
"""
sample_text= spark.read.format('csv').options(header='True', inferSchema='True', delimiter= '\t') \
                  .load('sample3.txt')


sample_text.show(truncate=False)

sample_text.printSchema()

print('Shape of df:', sample_text.count(), ' * ', len(sample_text.columns))

"""
writing dataframe to text file 
"""

print("Writing tab'\t'")
sample_text.write.mode('overwrite').csv(path='./Output/user_Text', header='True', sep= '\t')
print("Writing | seperated txt File")
sample_text.write.mode('overwrite').csv(path='./Output/user_Text1', header='True', sep= '|')


