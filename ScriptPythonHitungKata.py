from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

#Untuk mengganti file yang akan dihitung
lines = sc.textFile("file.txt")

#Untuk menghitung jumlah kata
words = lines.flatMap(lambda line: line.split(" "))
wordCounts = words.countByValue()

#Untuk menghitung jumlah seluruh kata
totalWords = words.count()

#Untuk menampilkan jumlah kata
for word, count in wordCounts.items():
    print(f"{word}: {count}")

#Untuk menampilkan jumlah seluruh kata
print(f"Total Words: {totalWords}")

sc.stop()