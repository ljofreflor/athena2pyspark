copy_to_s3:
	zip athena2pyspark *
	aws s3 cp athena2pyspark.zip s3://library.exalitica.com/
	rm athena2pyspark.zip
	git commit -a -m "publicacion en s3"
	

clean:
	rm *~

