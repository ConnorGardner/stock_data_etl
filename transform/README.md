## This serves as notes on the ETL as well as my own thoughts for potential additions #

The initial goal of this project was to demonstrate how one can fetch stock data from an API, tranform it from .json into readable dataframes, and do some transformations on the data to standardize it.

In the beginning, this project ran entirely locally on my machine. Although, I quickly learned that I needed to configure a runtime schedule as I wanted the API data every minute through market hours. AWS came in handy here and I decided to utilize both Lambda functions (for deployment) and S3 storage act as my 'data-lake'. Because of this, this code will NOT run locally if you just download it to your machine, it is set up to be placed into Lambdas. 

The benefit of AWS is that it allowed me to run the code every minute when using EventBridge - you can set up your own type of cron expression to whatever you desire. In addition, I saw no use of keeping a massive amount of .json files on my computer, so it was handy for that as well.

Currently, the api_request.py function sits as my first lambda function in aws. That file is responsible for fetching the .json and sending it off to a folder in S3. From there, standardize.py retrieves the files from that folder, and creates one large pandas dataframe. After some additional standadization (UTC conversion, duplicate removal, setting up columns), the .csv's are saved back out in S3.

The last stage I have opted for now to run locally. This keeps things easy when I want to open files on my own machine and inspect them with excel. It is also a work in progress and I may consider more transformations on the data. In the future once I am content with the transformations, I will incorperate the file as an additional lamda function, and create an extract.py file to simply retrieve the files I want from S3.