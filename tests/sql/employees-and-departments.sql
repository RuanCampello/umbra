CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    salary DOUBLE PRECISION
);

CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50),
    location VARCHAR(50)
);

INSERT INTO departments (department_id, department_name, location) VALUES
(1, 'Engineering', 'New York'),
(2, 'Sales', 'Chicago'),
(3, 'Marketing', 'Los Angeles'),
(4, 'HR', 'Boston');

INSERT INTO employees (employee_id, first_name, last_name, department_id, salary) VALUES
(101, 'John', 'Doe', 1, 75000.00),
(102, 'Jane', 'Smith', 2, 65000.00),
(103, 'Bob', 'Johnson', 1, 80000.00),
(104, 'Alice', 'Brown', 3, 70000.00),
(105, 'Charlie', 'Wilson', 2, 60000.00),
(106, 'Diana', 'Lee', 4, 55000.00);
