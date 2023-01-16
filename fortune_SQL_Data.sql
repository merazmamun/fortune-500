#Create the table for the 4 columns
CREATE TABLE fortune(
year INT(4),
company VARCHAR(150),
revenue FLOAT,
profit VARCHAR(1000)
);

#Assign a row number to each rows as ranking partitioned by year. Use this table as a CTE
WITH fortune_ranking AS(
SELECT ROW_NUMBER() OVER (PARTITION BY year ORDER BY revenue DESC) AS fortune_rank, year, company, revenue
FROM fortune)

#Using the CTE, get data for the first 10 rows of each year and standardize each company name
SELECT DISTINCT fortune_rank, year, 
CASE
WHEN company = "Amazon.com"
THEN "Amazon"
WHEN company = "American Intl. Group" OR company = "American Intl. Group (AIG)"
THEN "AIG"
WHEN company = "Apple, Inc."
THEN "Apple"
WHEN company = "AT&amp;T" OR company = "AT&T Technologies"
THEN "AT&T"
WHEN company = "Berkshire Hathaway Inc."
THEN "Berkshire Hathaway"
WHEN company = "Chevron (CVX)" OR company = "Chevron Corporation" OR company = "ChevronTexaco"
THEN "Chevron"
WHEN company = "Citigroup (C)"
THEN "Citigroup"
WHEN company = "ConocoPhillips (COP)"
THEN "ConocoPhillips"
WHEN company = "Exxon Mobil (XOM)" OR company = "Exxon Mobil Corporation"
THEN "Exxon Mobil"
WHEN company = "Ford Motor (F)" OR company = "Ford Motor Company"
THEN "Ford Motor"
WHEN company = "General Electric (GE)" OR company = "General Electric Company"
THEN "General Electric"
WHEN company = "General Motors (GM)" OR company = "General Motors Company"
THEN "General Motors"
WHEN company = "Intl. Business Machines" OR company = "Intl. Business Machines (IBM)"
THEN "IBM"
WHEN company = "Valero Energy Corporation"
THEN "Valero Energy"
WHEN company = "Wal-Mart Stores" OR company = "Wal-Mart Stores (WMT)" OR company = "Wal-Mart Stores, Inc."
THEN "Walmart"
ELSE company
END AS company_name, revenue
FROM fortune_ranking
WHERE fortune_rank <= 10;