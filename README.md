# php-parquet Binding

php-parquet is a Php wrapper for the parquet-cpp lib.


## Installation

Use the script compile_install.sh to compile and install both of the bindings.

```bash
bash compile_install.sh
```

## Usage php-parquet

```php

// Write Functions

$schema = array (
  'id' => 
  array (
    'name' => 'id',
    'type' => 'int32',
    'repeat' => 1,
  ),
  'name' => 
  array (
    'name' => 'name',
    'type' => 'string',
    'repeat' => 1,
  )
);

$value = array (
  array (
   'id' => '001',
    'name' => 'Client_1'
  ),
  array (
   'id' => '002',
    'name' => 'Client_2'
  ),
  array (
   'id' => '003',
    'name' => 'Client_3'
  )
);


$test_file = "file.parquet";

$parquet = new Parquet();

$parquet->create_writer($test_file, $schema);

$parquet->write($value);

$parquet->close_writer();




// Read Functions
$row_group_id = 0;
//create a ParquetReader object, which is the C++ Object read from the extension
$parquet->create_reader($test_file, $row_group_id);

/* Get INFO FROM PARQUET FILE */
$info = $parquet->getInfo();
$strInfo = json_encode($info);
$jsonInfo = json_decode($strInfo);

$created_by = $jsonInfo->created_by;            //String
$version = $jsonInfo->version;                  //Numeric
$num_cols = $jsonInfo->num_columns;             //Numeric
$num_rows = $jsonInfo->num_rows;                //Numeric
$num_row_groups = $jsonInfo->num_row_groups;    //Numeric
$strSchema = json_encode($jsonInfo->schema);    //String: Json formatted 


/* ECHO INFO */

echo "Created_by : ". $created_by. "\n";
echo "Version : " .$version. "\n";
echo "Number of columns : " .$num_cols. "\n";
echo "Number of rows : " .$num_rows. "\n";
echo "Number of row groups : " .$num_row_groups. "\n";
echo "Schema : \n" .$strSchema. "\n";

echo ("+++++++++++++++++++++++++++++++++++++++++++++\n");


#echo "Schema : \n" .$strSchema. "\n";

$json = "";
$parquet->json_file($json, 0);
print_r($json);

$col = 1;
$emptyArray = new stdClass();

#$reader->read_col($col, $emptyArray);
#print_r($emptyArray);

#$element = "ABC";
$row = 1;
$col = 1;
$parquet->read_element($row, $col, $element);
echo "{PHP} element[" .$row. ", " .$col. "] = " .$element. "\n";
echo "\n";



exit();

```
