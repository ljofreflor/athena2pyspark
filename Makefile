copy_library_to_s3:
	zip athena2pyspark ./athena2pyspark/*
	aws s3 cp ./athena2pyspark.zip s3://library.exalitica.com/
	rm ./athena2pyspark.zip
	git commit -a -m "publicacion en s3"
	
copy_etl_scripts_to_s3:
	aws s3 cp --recursive ./etl_scripts s3://leonardo.exalitica.com/etl_scripts/
clean:
	rm *~

