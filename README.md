# pyspark-assignment
Pyspark Assignment 
1. Question: 
1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 
Dataset: Column names["customer", "product_model"] : 
(1, "iphone13"), 
 (1, "dell i5 core"), 
 (2, "iphone13"), 
 (2, "dell i5 core"), 
 (3, "iphone13"), 
 (3, "dell i5 core"), 
 (1, "dell i3 core"), 
 (1, "hp i5 core"), 
 (1, "iphone14"), 
 (3, "iphone14"), 
 (4, "iphone13") 
Dataset: Column Names: ["product_model"] 
("iphone13",), 
("dell i5 core"), 
 ("dell i3 core"), 
 ("hp i5 core"), 
 ("iphone14") 

2.Find the customers who have bought only iphone13 

3.Find customers who upgraded from product iphone13 to product iphone14 

4.Find customers who have bought all models in the new Product Data 
 
 
 
 
 
2.Question: 
DataSet:Column(“card_number”) 
("1234567891234567",), 
("5678912345671234",), 
("9123456712345678",), 
("1234567812341122",), 
("1234567812341342",) 
1.Create a Dataframe as credit_card_df with different read methods 

2. print number of partitions 

3. Increase the partition size to 5 

4. Decrease the partition size back to its original partition size 

5.Create a UDF to print only the last 4 digits marking the remaining digits as * 
Eg: ************4567 


6.output should have 2 columns as card_number, masked_card_number(with output of question 2) 


3. Question: 

1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field 
data = [ 
4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its 
data type 
5. Write the data frame as a CSV file with different write options except (merge condition) 
6. Write it as a managed table with the Database name as user and table name as login_details with overwrite 
mode. 
(1, 101, 'login', '2023-09-05 08:30:00'), 
(2, 102, 'click', '2023-09-06 12:45:00'), 
(3, 101, 'click', '2023-09-07 14:15:00'), 
(4, 103, 'login', '2023-09-08 09:00:00'), 
(5, 102, 'logout', '2023-09-09 17:30:00'), 
(6, 101, 'click', '2023-09-10 11:20:00'), 
(7, 103, 'click', '2023-09-11 10:15:00'), 
(8, 102, 'click', '2023-09-12 13:10:00') 
] 
Column names: log id, user$id, action, and timestamp.

2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 


3. Write a query to calculate the number of actions performed by each user in the last 7 days 

4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its 
data type 

5. Write the data frame as a CSV file with different write options except (merge condition) 

6. Write it as a managed table with the Database name as user and table name as login_details with overwrite 
mode. 



(1, 101, 'login', '2023-09-05 08:30:00'), 
(2, 102, 'click', '2023-09-06 12:45:00'), 
(3, 101, 'click', '2023-09-07 14:15:00'), 
(4, 103, 'login', '2023-09-08 09:00:00'), 
(5, 102, 'logout', '2023-09-09 17:30:00'), 
(6, 101, 'click', '2023-09-10 11:20:00'), 
(7, 103, 'click', '2023-09-11 10:15:00'), 
(8, 102, 'click', '2023-09-12 13:10:00') 
] 
Column names: log id, user$id, action, and timestamp. 


2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 


3. Write a query to calculate the number of actions performed by each user in the last 7 days 

4.Question 

1. Read JSON file provided in the attachment using the dynamic function 

2. flatten the data frame which is a custom schema 

3. find out the record count when flattened and when it's not flattened(find out the difference why you are 
getting more count) 

4. Differentiate the difference using explode, explode outer, posexplode functions 

5. Filter the id which is equal to 0001  

6. convert the column names from camel case to snake case 

7. Add a new column named load_date with the current date 

8. create 3 new columns as year, month, and day from the load_date column 

9. write data frame to a table with the Database name as employee and table name as employee_details with 
overwrite mode, format as JSON and partition based on (year, month, day) using replacing where condition on 
year, month, day 

 
5. Question 
DataSet1:Column names(employee id, employee_name, department, State, salary, Age) 
((11,“james”,” D101”,”ny”,9000,34)), 
(12,”michel”,” D101”,”ny”,8900,32), 
(13,“robert”,” D102”,”ca”,7900,29), 
(14,“scott”,” D103”,”ca”,8000,36), 
(15,“jen”,” D102”,”ny”,9500,38), 
(16,”jeff”,” D103”,”uk”,9100,35), 
(17,“maria”,” D101”,”ny”,7900,40)) 
 
Dataset2:Column names(dept_id, dept_name) 
((“D101”,”sales”), 
(“D102”,”finance”), 
(”D103”,”marketing”), 
(“D104”,”hr”), 
(“D105”,”support”)) 
 
Dataset3: Column names(country_code, country_name) 
((“ny”,”newyork”), 
(“ca”,”California”), 
(“uk”,”Russia)) 
 
1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in 
dynamic way 

2. Find avg salary of each department 

3. Find the employee’s name and department name whose name starts with ‘m’  

4. Create another new column in  employee_df as a bonus by multiplying employee salary *2 

5. Reorder the column names of employee_df columns as 
(employee_id,employee_name,salary,State,Age,department) 

6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a 
dynamic way 

7. Derive a new data frame with country_name instead of State in employee_df  
Eg(11,“james”,”D101”,”newyork”,8900,32) 

8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the 
load_date column with the current date 

9. create 2 external tables with parquet, CSV format with the same name database name, and 2 different table 
names as CSV and parquet format. 
 
