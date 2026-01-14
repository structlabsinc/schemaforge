-- Target State: Hierarchical & Structured

-- 1. Hierarchy: HierarchyID
CREATE TABLE Employees (
    OrgNode HIERARCHYID NOT NULL,
    OrgLevel AS OrgNode.GetLevel(),
    EmployeeID INT UNIQUE,
    FullName NVARCHAR(100),
    Title NVARCHAR(50)
);
CREATE CLUSTERED INDEX IX_Employees_OrgNode ON Employees(OrgNode);

-- 2. Data Structure: XML
CREATE TABLE Config (
    KeyName VARCHAR(50) PRIMARY KEY,
    ValueXML XML -- Typed storage
);
