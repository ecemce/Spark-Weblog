Review one of the .log files in the directory. Note the format of the lines:

Copy the weblogs directory from the local filesystem to the /user/zeppelin/your_name HDFS directory

In spark, create an RDD from the uploaded web logs data files in the /yourname/weblogs/ directory in HDFS.

Create an RDD containing only those lines that are requests for JPG files. Use the filter operation with a transformation function that takes a string RDD element and returns a boolean value

Use take to return the first five lines of the data in jpglogsRDD. The return value is a list of strings (in Python) or array of strings (in Scala)

loop through and display the strings returned by take.

Now try using the map transformation to define a new RDD. Start with a simple map function that returns the length of each line in the log file. This results in an RDD of integers

Loop through and display the first five elements (integers) in the RDD. Calculating line lengths is not very useful. Instead, try mapping each string in logsRDD by splitting the strings based on spaces. The result will be an RDD in which each element is a list of strings (in Python) or an array of strings (in Scala). Each string represents a “field” in the web log line.

Return the first five elements of lineFieldsRDD. The result will be a list of lists of strings (in Python) or an array of arrays of strings (in Scala).

Display the contents of the return from take. Unlike in examples above, which returned collections of simple values (strings and ints), this time you have a set of compound values (arrays or lists containing strings). Therefore, to display them properly, you will need to loop through the arrays/lists in lineFields, and then loop through each string in the array/list. To make it easier to read the output, use ------- to separate each set of field values.

Now that you know how map works, create a new RDD containing just the IP addresses from each line in the log file. (The IP address is the first space-delimited field in each line.)


