

all: clean update_readme library_to_s3 etl_scripts_to_s3

library_to_s3_dev:
	make clean
	(cd src; zip -r ./../athena2pyspark.zip *)
	aws s3 cp athena2pyspark.zip s3://ba-workspace/ --profile fox
	rm ./athena2pyspark.zip

egg:
	make clean
	(cd src; zip -r ./../athena2pyspark.egg *)

clean:
	find . -name \*~ -delete
	find . -name \*.pyc -delete

update_readme:
	jupyter-nbconvert --to markdown README.ipynb

software_quality:
	#autogenerar documentacion de la libreria, coverade, testing, autoformat pep-8
