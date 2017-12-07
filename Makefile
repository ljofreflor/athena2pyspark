all: clean library_to_s3 etl_scripts_to_s3

library_to_s3:
	zip -r athena2pyspark ./athena2pyspark/*
	aws s3 cp ./athena2pyspark.zip s3://library.exalitica.com/
	rm ./athena2pyspark.zip
	git commit -a -m "publicacion en s3"
	
etl_scripts_to_s3:
	aws s3 cp --recursive ./etl_scripts s3://leonardo.exalitica.com/etl_scripts/

clean:
	find . -name \*~ -delete
	find . -name \*.pyc -delete

update_readme:
	jupyter-nbconvert --to markdown README.ipynb
