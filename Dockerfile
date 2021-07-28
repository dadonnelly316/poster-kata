#base image to install python
FROM python:3.9 

#create virtual working directory for container
WORKDIR /user/source/app

#copy relevant files into the the current working directory (/user/source/app). This docker file must sit in the same local directory as requierments.txt and app.py in order to find them since we're looking ./
COPY requierments.txt .
COPY app.py .


#run a pip install on the requierments.txt that was just copied into the virtual working directory (/user/source/app)
RUN pip install -r requierments.txt


#run app.py that was just copied into the virtual directory 
# CMD [ "python", "./app.py"]




#running this file will build the docker image

#open cmd and run <docker build -t 'ibmchallenge' . >
#to BUILD the image,  this in the same directory as the docker file. -t 'ibmchallenge' allows me to name the image. the ./ at the end looks for the docker filel in the current directory




#to RUN the image to create the CONTAINER, use the following command <docker run -ti --name ibmChallengeContainer ibmchallenge >

