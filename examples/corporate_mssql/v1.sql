-- Initial State: Flat Employee Table
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    ManagerID INT, -- Self-reference
    FullName NVARCHAR(100),
    Title NVARCHAR(50)
);

CREATE TABLE Config (
    KeyName VARCHAR(50) PRIMARY KEY,
    ValueString VARCHAR(MAX)
);
