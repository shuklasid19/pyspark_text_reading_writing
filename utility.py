from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('text').getOrCreate()
print('Sparksession:',spark)

#using tab as delimiter and reading the text file with given parameters
sample_text= spark.read.format('csv').options(header='True', inferSchema='True', delimiter= '\t').load('sample3.txt')


#it will show all text in non truncated way
sample_text.show(truncate=False)

#print the schema of text file
sample_text.printSchema()

#the number of rows and columns
print('Rows:', sample_text.count(), ' columns ', len(sample_text.columns))



print("Writinig file in given folder and saving with sep as tab'")
sample_text.write.mode('overwrite').csv(path='./Output/user_text_overwritten', header='True', sep= '\t')
print("Writing file in given folder and saving it with sep = | ")
sample_text.write.mode('overwrite').csv(path='./Output/user_text1_overwritten', header='True', sep= '|')


