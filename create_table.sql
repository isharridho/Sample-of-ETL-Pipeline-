CREATE TABLE Employee (
    Id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    EmployeeId VARCHAR(10) NOT NULL UNIQUE,
    FullName VARCHAR(100) NOT NULL,
    BirthDate DATE NOT NULL,
    Address VARCHAR(500)
);

CREATE TABLE PositionHistory (
    Id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    PosId VARCHAR(10) NOT NULL,
    PosTitle VARCHAR(100) NOT NULL,
    EmployeeId VARCHAR(10) NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE
);
=======================================================================================
--INSERT Script for Employee Table:
INSERT INTO Employee (EmployeeId, FullName, BirthDate, Address)
VALUES 
('10105001', 'Ali Anton', '1982-08-19', 'Jakarta Utara');

--INSERT Script for PositionHistory Table:
INSERT INTO PositionHistory (PosId, PosTitle, EmployeeId, StartDate, EndDate)
VALUES 
('50001', 'IT Sr. Manager', '10105001', '2022-03-01', '2022-12-31');

=======================================================================================
SELECT 
    E.EmployeeId,
    E.FullName,
    E.BirthDate,
    E.Address,
    PH.PosId,
    PH.PosTitle,
    PH.StartDate,
    PH.EndDate
FROM 
    Employee E
LEFT JOIN 
    PositionHistory PH
ON 
    E.EmployeeId = PH.EmployeeId
WHERE 
    PH.EndDate IS NULL OR PH.EndDate >= GETDATE();
