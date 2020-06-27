MapReduce Practice Questions.
Download this text file and write MapReduce programs to find:

1. The frequency of the following words(case insensitive): "Julius Caesar", "Romeo","Juliet", "Cleopatra", "Hamlet"

a) Using counters

b) Using distributed cache

 

2. The weight of a word is defined as the sum of the ASCII values of its characters, e.g. the weight of "GOOGLE" equals 71+79+79+76+69=374. Using a MapReduce program, compute the weight of the entire document, i.e. the sum of the weights of all the contained words.

 

3. Create the following tables as csv files according to the given schema:

EMPLOYEE (ID: int, Name: String, ClientID: String)

MANAGER (EmployeeID: int, Reportee ID: int)

CLIENT(ID: String, Name: String, Country: String)

 

Enter 10 records each into these tables such that there is scope for performing a three-way join.

Now, write a MapReduce program to perform a three-way join which will help in answering questions like:

How many employees serving clients in China report to Manager A?  