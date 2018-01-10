all: clean update_readme library_to_s3 etl_scripts_to_s3

library_to_s3:
	zip -r athena2pyspark ./athena2pyspark/*
	aws s3 cp ./athena2pyspark.zip s3://library.exalitica.com/
	rm ./athena2pyspark.zip

library_to_egg:
	zip -r athena2pyspark ./athena2pyspark/*
	mv athena2pyspark.zip athena2pyspark.egg
	zip -r awsglue ./awsglue/*
	mv awsglue.zip awsglue.egg
	

etl_scripts_to_s3:
	# aws s3 cp --recursive ./etl_scripts s3://cencosud.exalitica.com/prod/etl_scripts/

clean:
	find . -name \*~ -delete
	find . -name \*.pyc -delete

update_readme:
	jupyter-nbconvert --to markdown README.ipynb

software_quality:
	#autogenerar documentacion de la libreria, coverade, testing, autoformat pep-8
