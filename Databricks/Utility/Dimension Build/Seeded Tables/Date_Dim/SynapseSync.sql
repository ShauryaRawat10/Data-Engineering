-- ***********************************************
-- date_dim : This table is one time load into EDW
-- Created By: Shaurya Rawat
-- ***********************************************

-- Update existing records in EDW based on changes in STG
UPDATE target
SET
  target.Calendar_Name         = source.Calendar_Name,
  target.Date_Num              = source.Date_Num,
  target.Calendar_Date         = source.Calendar_Date,
  target.Date_Long_Name        = source.Date_Long_Name,
  target.Weekday_Name          = source.Weekday_Name,
  target.Weekday_Name_Abrv     = source.Weekday_Name_Abrv,
  target.Weekday_Num           = source.Weekday_Num,
  target.Day_of_Month          = source.Day_of_Month,
  target.Day_of_Quarter        = source.Day_of_Quarter,
  target.Day_of_Year           = source.Day_of_Year,
  target.Week_Num              = source.Week_Num,
  target.Week_Long_Name        = source.Week_Long_Name,
  target.Week_Start_Date       = source.Week_Start_Date,
  target.Week_End_Date         = source.Week_End_Date,
  target.Month_Dim_Id          = source.Month_Dim_Id,
  target.Month_Num             = source.Month_Num,
  target.Month_Name            = source.Month_Name,
  target.Month_Name_Abrv       = source.Month_Name_Abrv,
  target.Month_Year_Desc       = source.Month_Year_Desc,
  target.Month_Year_Long_Desc  = source.Month_Year_Long_Desc,
  target.Month_Period_Desc     = source.Month_Period_Desc,
  target.Month_of_Quarter      = source.Month_of_Quarter,
  target.Month_Start_Date      = source.Month_Start_Date,
  target.Month_End_Date        = source.Month_End_Date,
  target.Quarter_Num           = source.Quarter_Num,
  target.Quarter_Name          = source.Quarter_Name,
  target.Quarter_Long_Name     = source.Quarter_Long_Name,
  target.Quarter_Name_Abrv     = source.Quarter_Name_Abrv,
  target.Quarter_Period_Desc   = source.Quarter_Period_Desc,
  target.Quarter_Start_Date    = source.Quarter_Start_Date,
  target.Quarter_End_Date      = source.Quarter_End_Date,
  target.Year_Num              = source.Year_Num,
  target.Year_Period_Desc      = source.Year_Period_Desc,
  target.Year_Start_Date       = source.Year_Start_Date,
  target.Year_End_Date         = source.Year_End_Date,
  target.Prior_Date            = source.Prior_Date,
  target.Prior_Month_Num       = source.Prior_Month_Num,
  target.Prior_Month_Name      = source.Prior_Month_Name,
  target.Prior_Month_Name_Abrv = source.Prior_Month_Name_Abrv,
  target.Prior_Month_Year_Desc = source.Prior_Month_Year_Desc,
  target.Prior_Month_Period_Desc = source.Prior_Month_Period_Desc,
  target.Prior_Month_Quarter_Num = source.Prior_Month_Quarter_Num,
  target.Prior_Month_Year_Num  = source.Prior_Month_Year_Num,
  target.Prior_Month_Start_Date = source.Prior_Month_Start_Date,
  target.Prior_Month_End_Date  = source.Prior_Month_End_Date,
  target.Prior_Quarter_Num     = source.Prior_Quarter_Num,
  target.Prior_Quarter_Name    = source.Prior_Quarter_Name,
  target.Prior_Quarter_Long_Name = source.Prior_Quarter_Long_Name,
  target.Prior_Quarter_Name_Abrv = source.Prior_Quarter_Name_Abrv,
  target.Prior_Quarter_Year_Num = source.Prior_Quarter_Year_Num,
  target.Prior_Quarter_Start_Date = source.Prior_Quarter_Start_Date,
  target.Prior_Quarter_End_Date = source.Prior_Quarter_End_Date,
  target.Prior_Year_Num        = source.Prior_Year_Num,
  target.Prior_Year_Start_Date = source.Prior_Year_Start_Date,
  target.Prior_Year_End_Date   = source.Prior_Year_End_Date,
  target.Next_Date             = source.Next_Date,
  target.Next_Month_Num        = source.Next_Month_Num,
  target.Next_Month_Name       = source.Next_Month_Name,
  target.Next_Month_Name_Abrv  = source.Next_Month_Name_Abrv,
  target.Next_Month_Year_Desc  = source.Next_Month_Year_Desc,
  target.Next_Month_Period_Desc = source.Next_Month_Period_Desc,
  target.Next_Month_Quarter_Num = source.Next_Month_Quarter_Num,
  target.Next_Month_Year_Num   = source.Next_Month_Year_Num,
  target.Next_Month_Start_Date = source.Next_Month_Start_Date,
  target.Next_Month_End_Date   = source.Next_Month_End_Date,
  target.Next_Quarter_Num      = source.Next_Quarter_Num,
  target.Next_Quarter_Name     = source.Next_Quarter_Name,
  target.Next_Quarter_Long_Name = source.Next_Quarter_Long_Name,
  target.Next_Quarter_Name_Abrv = source.Next_Quarter_Name_Abrv,
  target.Next_Quarter_Year_Num = source.Next_Quarter_Year_Num,
  target.Next_Quarter_Start_Date = source.Next_Quarter_Start_Date,
  target.Next_Quarter_End_Date = source.Next_Quarter_End_Date,
  target.Next_Year_Num         = source.Next_Year_Num,
  target.Next_Year_Start_Date  = source.Next_Year_Start_Date,
  target.Next_Year_End_Date    = source.Next_Year_End_Date
FROM STG.Date_Dim source
INNER JOIN EDW.Date_Dim target 
  ON source.Date_Dim_Id = target.Date_Dim_Id;

-- Insert new records from STG to EDW if they don't exist in EDW
INSERT INTO EDW.Date_Dim (
  Date_Dim_Id,
  Calendar_Name,
  Date_Num,
  Calendar_Date,
  Date_Long_Name,
  Weekday_Name,
  Weekday_Name_Abrv,
  Weekday_Num,
  Day_of_Month,
  Day_of_Quarter,
  Day_of_Year,
  Week_Num,
  Week_Long_Name,
  Week_Start_Date,
  Week_End_Date,
  Month_Dim_Id,
  Month_Num,
  Month_Name,
  Month_Name_Abrv,
  Month_Year_Desc,
  Month_Year_Long_Desc,
  Month_Period_Desc,
  Month_of_Quarter,
  Month_Start_Date,
  Month_End_Date,
  Quarter_Num,
  Quarter_Name,
  Quarter_Long_Name,
  Quarter_Name_Abrv,
  Quarter_Period_Desc,
  Quarter_Start_Date,
  Quarter_End_Date,
  Year_Num,
  Year_Period_Desc,
  Year_Start_Date,
  Year_End_Date,
  Prior_Date,
  Prior_Month_Num,
  Prior_Month_Name,
  Prior_Month_Name_Abrv,
  Prior_Month_Year_Desc,
  Prior_Month_Period_Desc,
  Prior_Month_Quarter_Num,
  Prior_Month_Year_Num,
  Prior_Month_Start_Date,
  Prior_Month_End_Date,
  Prior_Quarter_Num,
  Prior_Quarter_Name,
  Prior_Quarter_Long_Name,
  Prior_Quarter_Name_Abrv,
  Prior_Quarter_Year_Num,
  Prior_Quarter_Start_Date,
  Prior_Quarter_End_Date,
  Prior_Year_Num,
  Prior_Year_Start_Date,
  Prior_Year_End_Date,
  Next_Date,
  Next_Month_Num,
  Next_Month_Name,
  Next_Month_Name_Abrv,
  Next_Month_Year_Desc,
  Next_Month_Period_Desc,
  Next_Month_Quarter_Num,
  Next_Month_Year_Num,
  Next_Month_Start_Date,
  Next_Month_End_Date,
  Next_Quarter_Num,
  Next_Quarter_Name,
  Next_Quarter_Long_Name,
  Next_Quarter_Name_Abrv,
  Next_Quarter_Year_Num,
  Next_Quarter_Start_Date,
  Next_Quarter_End_Date,
  Next_Year_Num,
  Next_Year_Start_Date,
  Next_Year_End_Date
)
SELECT
  source.Date_Dim_Id,
  source.Calendar_Name,
  source.Date_Num,
  source.Calendar_Date,
  source.Date_Long_Name,
  source.Weekday_Name,
  source.Weekday_Name_Abrv,
  source.Weekday_Num,
  source.Day_of_Month,
  source.Day_of_Quarter,
  source.Day_of_Year,
  source.Week_Num,
  source.Week_Long_Name,
  source.Week_Start_Date,
  source.Week_End_Date,
  source.Month_Dim_Id,
  source.Month_Num,
  source.Month_Name,
  source.Month_Name_Abrv,
  source.Month_Year_Desc,
  source.Month_Year_Long_Desc,
  source.Month_Period_Desc,
  source.Month_of_Quarter,
  source.Month_Start_Date,
  source.Month_End_Date,
  source.Quarter_Num,
  source.Quarter_Name,
  source.Quarter_Long_Name,
  source.Quarter_Name_Abrv,
  source.Quarter_Period_Desc,
  source.Quarter_Start_Date,
  source.Quarter_End_Date,
  source.Year_Num,
  source.Year_Period_Desc,
  source.Year_Start_Date,
  source.Year_End_Date,
  source.Prior_Date,
  source.Prior_Month_Num,
  source.Prior_Month_Name,
  source.Prior_Month_Name_Abrv,
  source.Prior_Month_Year_Desc,
  source.Prior_Month_Period_Desc,
  source.Prior_Month_Quarter_Num,
  source.Prior_Month_Year_Num,
  source.Prior_Month_Start_Date,
  source.Prior_Month_End_Date,
  source.Prior_Quarter_Num,
  source.Prior_Quarter_Name,
  source.Prior_Quarter_Long_Name,
  source.Prior_Quarter_Name_Abrv,
  source.Prior_Quarter_Year_Num,
  source.Prior_Quarter_Start_Date,
  source.Prior_Quarter_End_Date,
  source.Prior_Year_Num,
  source.Prior_Year_Start_Date,
  source.Prior_Year_End_Date,
  source.Next_Date,
  source.Next_Month_Num,
  source.Next_Month_Name,
  source.Next_Month_Name_Abrv,
  source.Next_Month_Year_Desc,
  source.Next_Month_Period_Desc,
  source.Next_Month_Quarter_Num,
  source.Next_Month_Year_Num,
  source.Next_Month_Start_Date,
  source.Next_Month_End_Date,
  source.Next_Quarter_Num,
  source.Next_Quarter_Name,
  source.Next_Quarter_Long_Name,
  source.Next_Quarter_Name_Abrv,
  source.Next_Quarter_Year_Num,
  source.Next_Quarter_Start_Date,
  source.Next_Quarter_End_Date,
  source.Next_Year_Num,
  source.Next_Year_Start_Date,
  source.Next_Year_End_Date
FROM STG.Date_Dim source
LEFT JOIN EDW.Date_Dim target 
  ON source.Date_Dim_Id = target.Date_Dim_Id
WHERE target.Date_Dim_Id IS NULL;

