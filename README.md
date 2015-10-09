# USDA Nutrition Database in MySQL format

This is the USDA National Nutrition database converted to SQL format for use in MySQL

The orignal data can be found on the USDA website in delimited ASCII and Acesss DB formats:

http://www.ars.usda.gov/Services/docs.htm?docid=8964

Also included in this repostiory is a PHP utility for importing the delimited ASCII version of the db to MySQL.

To use:

1. Create a database in MySQL that the data will be imported into
2. Download the latest version of the USDA db (currently v28) from their website.
3. Edit the $cfg array at the top of the import\_nutdata.php file to fit your environment.
4. Run the utility

It's possible that newer versions of the database might have schema changes that will need to be added to the PHP utility. This should be checked before importing the data.


