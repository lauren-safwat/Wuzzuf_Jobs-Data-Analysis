# Wuzzuf_Jobs RESTful Web Service using Apache Spark and Spring Boot
## Introduction:

This is our final project of the Java and UML for Machine Learning Course, where we were required to create a RESTful web service and implement several functions on the following dataset:
https://www.kaggle.com/omarhanyy/wuzzuf-jobs

## Project Structure:
The project's structure can be inferred from the following diagram:
![This is an image](https://github.com/lauren-safwat/Wuzzuf_Jobs/blob/dc01d14c5c8d3ae9f9c4388403b9d4a59819fb95/Blank%20diagram.png)

Starting from the bottom, the following highlights the function of each layer:

### Database Layer:
This comprises the .csv file obtained from Kaggle.com 
### Data Access Object Layer:
This layer is responsible for creating a class of 'Job' objects, thus encapsulating all the attributes of each row in the .csv file. The DAO object presents an interface where an abstraction of the functions to be implemented on the dataset are created.
### Service Layer: 
Perhaps the most essential layer, this layer comprises the use of apache spark in order to perform the required operations on the 'Job' instances at hand, namely:
- Displaying the structure and summary of the dataset
- Exploratory Data Analysis and data cleaning 
- Counting the jobs for each company and displaying the results in a pie chart
- Obtaining the most popular job titles and displaying the results in a pie chart as well
- Obtaining the most popular job location and displaying the results in a pie chart 
- Printing the required skills one by one, computing how many are repeated and sorting the output to obtain the most important skills required
### Controller Layer:
This is where the implemented functions are converted into services that can be sent by a client, where the client gets to choose the service as needed.
### Test Client:
Although not part of the project structure, the test client aims to mock a RESTful service in order to ensure all required functionalities are in order. 
