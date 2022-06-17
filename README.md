# BigDataProcessing
A simple Big Data processing project that utilizes Spark, developed for the purposes of the Big Data module.

The project utilizes a simple data containing three tables:
- Sales
- Products
- Sellers

The tables contain information regarding a retail store, and more specifically about the orders completed.
Each order contains information on the seller and the products sold. Products and sellers are also entities
of this dataset and have their own unique attributes.

The goal of the project was to develop a system to process big data in a simulated environment.
Thus, although the dataset was in the 6GB range, the spark configuration was adjusted to match the scale
and limit the available resources.

The resources would vary acoording to the processing demands of a specific task (aka question to answer).