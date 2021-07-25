# TODO: look for pyspark image without jupyter
FROM jupyter/pyspark-notebook

# install dependencies
# TODO: add req.txt
RUN pip install \
    kafka-python  \
    sseclient \
    pyspark