-- MSSQL Exhaustive Coverage Test
CREATE TABLE products (
    product_id INT PRIMARY KEY CLUSTERED,
    name NVARCHAR(255) NOT NULL,
    description NTEXT,
    price MONEY
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

CREATE NONCLUSTERED INDEX idx_prod_price ON products (price) 
    WHERE price > 100 
    WITH (DATA_COMPRESSION = PAGE);

CREATE TABLE sensor_data (
    ts DATETIME2 PRIMARY KEY,
    reading FLOAT
) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
CREATE CLUSTERED INDEX idx_test ON dbo.Customers (CustomerID ASC) WITH (DROP_EXISTING = ON);
