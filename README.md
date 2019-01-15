
athena2pyspark
==

## Instalación
```
pip install --upgrade git+http://git@github.com/ljofre/aws-athena-tools.git@master
```

## Zip file to AWS Glue

```
wget https://github.com/ljofre/aws-athena-tools/archive/master.zip site-packages.zip
aws s3 cp site-packages.zip s3://bucket/prefix/site-packages.zip
```

todo list

- [x] Leer credenciales desde el archivo init de ~/.aws
- [] Integrar con roles aws
- [] Integrar con serverless
- [] Compatibilidad python3.7
- [] Dockerfile con pruebas unitarias
- [] Carpeta de ejemplos
- [] Construccion de querys con django o sqlalchemys
