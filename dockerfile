FROM python:latest

# Install dependencies
RUN apt-get update && \
 apt-get install -y python3-dask && \
 apt-get install python3-distributed

# Any working directory can be chosen as per choice like '/' or '/home' etc
WORKDIR /usr/app/src

#COPY the remote file at working directory in container
COPY dask_distributed_analysis_ec2_ecs.py ./
# Now the structure looks like this '/usr/app/src/dask_distributed_analysis_ec2_ecs.py'

#CMD instruction should be used to run the software
#contained by your image, along with any arguments.
CMD [ "python", "./dask_distributed_analysis_ec2_ecs.py"]
