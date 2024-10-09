-- Data Cleaning 

select * 
from layoffs;

-- 1 . Remove Duplicates 
-- 2 . Standardize the Data 
-- 3 . Null Values Or blank values 
-- 4 . Remove Any Columns or Rows

select *,
row_number() over(partition by company,industry,
total_laid_off,percentage_laid_off,'data' ) 
from layoffs;
-- نسخ البيانات من الجدوال الاصالي الي جدول تاني عشان اشتغل علي جدول واحد فقط والبيانات تفضل ذي ماهي
insert lauoffs_staging
select * 
from layoffs;
-- 
select *,
row_number() over(
partition by company,industry,
total_laid_off,percentage_laid_off,'data' ) as row_num
from lauoffs_staging;
--
with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,
'data',stage,country,funds_raised_millions ) as row_num
from lauoffs_staging 
)
select *
from duplicate_cte
where row_num > 1 ;

--
select * 
from lauoffs_staging
where company = 'Casper';
--
with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,
'data',stage,country,funds_raised_millions ) as row_num
from lauoffs_staging 
)
delete 
from duplicate_cte
where row_num > 1 ;

--
insert into lauoffs_staging2
select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,
'data',stage,country,funds_raised_millions ) as row_num
from lauoffs_staging ;
--
select * 
from lauoffs_staging2
where  row_num >1;
-- حذف الصف 
delete  
from lauoffs_staging2
where  row_num > 1;

-- نسخ البيانات 

CREATE TABLE `lauoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num`int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
select * 
from layoffs;
-- 
select * 
from lauoffs_staging;
--
select * 
from lauoffs_staging2;

-- Standardizing data توحيد البيانات 
-- مسح المسافات 
select company,trim(company)
from lauoffs_staging2;
-- 
update lauoffs_staging2
set company = trim(company);
--
select distinct industry
from lauoffs_staging2
;

-- industry تنظيف عمود ال 
update lauoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

-- الكشف عن خطء في ال country 

select distinct country,
trim(trailing '.' from country) -- الخطء هو وجود نقطه بعد كل اسم دوله
from lauoffs_staging2
order by 1 ;

-- تصحيح الخطاء 

update lauoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- ظبط التايخ
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from lauoffs_staging2;
update lauoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from lauoffs_staging2;

--  date الي TEXT  منdata تعديل نوع البيانات تبع عمود ال
alter table lauoffs_staging2
modify column `datdatee` date;

-- حذف البيانات الفارغه 

select * 
from lauoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
--
delete
from lauoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
--


select *
from lauoffs_staging2
where industry is null
or industry = '';

select *
from lauoffs_staging2
where company like 'Bally%';

--

select t1.industry, t2.industry
from lauoffs_staging2 t1
join lauoffs_staging2 t2 
    on t1.company = t2.company 
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null ;

-- تحديث البيانات الفارغه

update lauoffs_staging2 -- حتي اقدر علي التعامل معها nall الي black تغير البيانات من  
set industry = null 
where industry = '';

update lauoffs_staging2 t1
join lauoffs_staging2 t2
   on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


-- row_numحذف العمود ال 
alter table lauoffs_staging2
drop column row_num;

--
-- Cleaning الجدول بعد عمليت ال 
select * 
from lauoffs_staging2;

--  الجدول قبل التعديل 
select * 
from lauoffs_staging;

-- DANE 
-- Thank you 
