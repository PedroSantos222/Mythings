select *
from layoffs;

-- 1. Remove Duplicates 
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

create table layoffs_staging
like layoffs;


insert layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;

select *,
ROW_NUMBER() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


with duplicate_cte as 
(
select *,
ROW_NUMBER() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;

select *
from layoffs_staging
where company = "#Paid";

create table `layoffs_staging2`(
	`company` text,
	`location` text,
    `industry` text,
    `total_laid_off` int Default null,
    `percentage_laid_off` text,
    `date` text,
    `stage` text,
    `country` text,
    `funds_raised_millions` int default null,
    `row_numb` int
) Engine= InnoDB Default Charset=utf8mb4 Collate=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
ROW_NUMBER() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;
SET SQL_SAFE_UPDATES = 0;
delete
from layoffs_staging2
where row_numb>1;
select company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
from layoffs_staging2;


-- Standardizing data

update layoffs_staging2
set company = trim(company);

select * 
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = "crypto"
where industry like "crypto%";

update layoffs_staging2
set country = "United States"
where country like "United States%";

select * 
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y'); 

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = "";

select *
from layoffs_staging2
where company = "Airbnb";

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where t1.industry is null or t1.industry = ""
and t2.industry is not null
;

update layoffs_staging2
set industry = Null
where industry = "";

update layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company=t2.company
set t1.industry = t2.industry    
where t1.industry is null
and t2.industry is not null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_numb;

select *
from layoffs_staging2;