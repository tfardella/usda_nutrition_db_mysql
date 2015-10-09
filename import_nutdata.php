<?php

	// Edit the $cfg array for your environment
	$cfg = array (
		"dbname" => "nutrition",
		"hostname" => "localhost",
		"username" => "root",
		"password" => "password",
	    "filepath" => "sr28/",

	    "colDelimiter" => "^",
	    "textDelimiter" => "~",
	    "lineDelimiter" => "\r\n",
	);


	// Function to read the data from the nutrition db text files and insert it into the MySQL tables
	function importData( $fileName, $tableName, $db, $dbh ) {
	    $cstr = "";
		echo "Importing " . $fileName . "...\n";

        global $cfg;

        // Import data from a text file
        $query = "LOAD DATA LOCAL"
        . " INFILE '" . $cfg[ "filepath" ] . $fileName . "' INTO TABLE " . $tableName
        . " COLUMNS TERMINATED BY '" . $cfg[ "colDelimiter" ] . "'"
        . " ENCLOSED BY '" . $cfg[ "textDelimiter" ] . "'"
        . " LINES TERMINATED BY '" . $cfg[ "lineDelimiter" ] . "';";

        try {
			$pdos = $dbh->prepare( $query );
    		$pdos->execute();
	    }
        catch( PDOException $e ) {
	    	echo $e->getMessage();
		}
    }


	// Function to print the error array of a PDO statement
	function print_pdo_error( $pdos, $query, $line ) {
		$arr = $pdos->errorInfo();
		if($arr[ 0 ] !== "00000" ) {
			echo "\nPDOStatement::errorInfo():\n";
			print_r( $arr );
			print( "\n" . $query . "\n\n" );
			print( $line . "\n\n" );
			exit;
		}
	}

	/* *************************
	 * Create the schema
	 */
	function createSchema( $dbh )
	{
		echo ">> Creating schema...\n";

		$dbh->query('DROP TABLE IF EXISTS FOOD_DES'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS FOOD_DES (
				NDB_No  VARCHAR(5),
				FdGrp_Cd  VARCHAR(4),
				Long_Desc  VARCHAR(200),
				Shrt_Desc  VARCHAR(60),
				ComName  VARCHAR(100),
				ManufacName  VARCHAR(65),
				Survey  CHAR(1),
				Ref_desc  TINYINT(2) UNSIGNED,
				Refuse  TINYINT(2),
				SciName  VARCHAR(65),
				N_Factor  DEC(4,2) UNSIGNED,
				Pro_Factor  DEC(4,2) UNSIGNED,
				Fat_Factor  DEC(4,2) UNSIGNED,
				CHO_Factor  DEC(4,2) UNSIGNED,
				PRIMARY KEY(NDB_No)
			)'
		);

		$dbh->query('DROP TABLE IF EXISTS FD_GROUP'); // yes
		$pdos = $dbh->query(
		  'CREATE TABLE IF NOT EXISTS FD_GROUP (
		    FdGrp_Cd	CHAR(4),
		    FdGrp_Desc	VARCHAR(60),
			PRIMARY KEY(FdGrp_Cd)
		  )'
		);


		$dbh->query('DROP INDEX IF EXISTS FD_GROUP_INDEX');
		$pdos = $dbh->query(
		  'CREATE INDEX IF NOT EXISTS FD_GROUP_INDEX ON FD_GROUP ( FdGrp_Cd)'
		);

		$dbh->query('DROP TABLE IF EXISTS LANGUAL');
		$pdos = $dbh->query(
		  'CREATE TABLE IF NOT EXISTS LANGUAL (
		    NDB_No  CHAR(5),
		    Factor_Code  CHAR(5),
			PRIMARY KEY(NDB_No)
		  )'
		);

		$dbh->query( 'DROP INDEX IF EXISTS LANGUAL_INDEX' );
		$dbh->query(
		  'CREATE INDEX IF NOT EXISTS LANGUAL_INDEX ON LANGUAL ( NDB_No, Factor_Code )'
		);

		$dbh->query('DROP TABLE IF EXISTS LANGDESC');
		$dbh->query(
		  'CREATE TABLE IF NOT EXISTS LANGDESC (
		    Factor_Code  CHAR(5),
		    Description  VARCHAR(140),
			PRIMARY KEY(Factor_Code)
		  )'
		);

		$dbh->query( 'DROP INDEX IF EXISTS LANGDESC_INDEX' );
		$dbh->query(
		  'CREATE INDEX IF NOT EXISTS LANGDESC_INDEX ON LANGDESC ( Factor_Code )'
		);

		$dbh->query('DROP TABLE IF EXISTS NUT_DATA');
		$pdos = $dbh->query(
		  'CREATE TABLE IF NOT EXISTS NUT_DATA (
		    NDB_No  CHAR(5),
		    Nutr_No CHAR(3),
		    Nutr_Val DEC(10, 3) UNSIGNED,
		    Num_Data_Pts DEC(5, 0) UNSIGNED,
		    Std_Error DEC(8, 3) UNSIGNED,
		    Src_Cd CHAR(2),
		    Deriv_Cd CHAR(4),
		    Ref_NDB_No CHAR(5),
		    Add_Nutr_Mark CHAR(1),
		    Num_Studies DEC(2, 0) UNSIGNED,
		    Min DEC(10, 3) UNSIGNED,
		    Max DEC(10, 3) UNSIGNED,
		    DF DEC(2, 0),
		    Low_EB DEC(10, 3) UNSIGNED,
		    Up_EB DEC(10, 3) UNSIGNED,
		    Stat_cmt VARCHAR(10),
		    AddMod_Date VARCHAR(10),
		    CC  CHAR(1),
			PRIMARY KEY(NDB_No, Nutr_No)
		  )'
		);

		$dbh->query('DROP INDEX IF EXISTS NUT_DATA_INDEX');
		$dbh->query(
		  'CREATE INDEX IF NOT EXISTS NUT_DATA_INDEX ON NUT_DATA ( NDB_No, Nutr_No)'
		);

		$dbh->query('DROP TABLE IF EXISTS NUTR_DEF'); // yes
		$dbh->query(
		  'CREATE TABLE IF NOT EXISTS NUTR_DEF (
		    Nutr_No CHAR(3),
		    Units CHAR(7),
		    Tagname VARCHAR(20),
		    NutrDesc VARCHAR(60),
		    Num_Dec DEC(1, 0) UNSIGNED,
		    SR_Order DEC(6, 0) UNSIGNED,
			PRIMARY KEY(Nutr_No)
		  )'
		);

		$dbh->query('DROP TABLE IF EXISTS SRC_CD'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS SRC_CD (
				Src_Cd  CHAR(2),
				SrcCd_Desc  VARCHAR(60),
				PRIMARY KEY(Src_Cd)
			)'
		);

		$dbh->query('DROP TABLE IF EXISTS DERIV_CD'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS DERIV_CD (
				Deriv_Cd  CHAR(4),
				Deriv_Desc  VARCHAR(120),
				PRIMARY KEY(Deriv_Cd)
			)'
		);

		$dbh->query('DROP INDEX IF EXISTS DERIV_CD_INDEX');
		$dbh->query(
			'CREATE INDEX IF NOT EXISTS DERIV_CD_INDEX ON DERIV_CD ( Deriv_Cd)'
		);

		$dbh->query('DROP TABLE IF EXISTS WEIGHT'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS WEIGHT (
				NDB_No  CHAR(5),
				Seq CHAR(2),
				Amount  DEC(5, 3) UNSIGNED,
				Msre_Desc VARCHAR(80),
				Gm_Wgt  DEC(7, 1) UNSIGNED,
				Num_Data_Pts  DEC(3, 0) UNSIGNED,
				Std_Dev DEC(7, 3) UNSIGNED,
				PRIMARY KEY(NDB_No, Seq)
			)'
		);

		$dbh->query('DROP INDEX IF EXISTS WEIGHT_INDEX');
		$dbh->query(
			'CREATE INDEX IF NOT EXISTS WEIGHT_INDEX ON WEIGHT ( NDB_No, Seq)'
		);

		$dbh->query('DROP TABLE IF EXISTS FOOTNOTE'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS FOOTNOTE (
				NDB_No  CHAR(5),
				Footnt_No CHAR(4),
				Footnt_Typ  CHAR(1),
				Nutr_No CHAR(3),
				Footnt_Txt  VARCHAR(200)
			)'
		);

		$dbh->query('DROP TABLE IF EXISTS DATSRCLN'); // yes
		$dbh->query(
			'CREATE TABLE IF NOT EXISTS DATSRCLN (
				NDB_No  CHAR(5),
				Nutr_No CHAR(3),
				DataSrc_ID  CHAR(6),
				PRIMARY KEY(NDB_No)
			)'
		);

		$dbh->query('DROP INDEX IF EXISTS DATSRCLN_INDEX');
		$dbh->query(
		  'CREATE INDEX IF NOT EXISTS DATSRCLN_INDEX ON DATSRCLN ( NDB_No, Seq)'
		);

		$dbh->query('DROP TABLE IF EXISTS DATA_SRC'); // yes
		$dbh->query(
		  'CREATE TABLE IF NOT EXISTS DATA_SRC (
		    DataSrc_ID	CHAR(6),
		    Authors		VARCHAR(255),
		    Title		VARCHAR(255),
		    Year		CHAR(4),
		    Journal		VARCHAR(135),
		    Vol_City	CHAR(16),
		    Issue_State	CHAR(5),
		    Start_Page	CHAR(5),
		    End_Page	CHAR(5),
			PRIMARY KEY(DataSrc_ID)
		  )'
		);

		$dbh->query('DROP TABLE IF EXISTS ABBREV');
		$dbh->query(
		  'CREATE TABLE IF NOT EXISTS ABBREV (
		    NDB_No		CHAR(5),
		    Shrt_Desc	VARCHAR(60),
		    Water		DEC(10, 2) UNSIGNED,
		    Energ_Kcal	DEC(10) UNSIGNED,
		    Protein		DEC(10, 2) UNSIGNED,
		    Lipid_Tot	DEC(10, 2) UNSIGNED,
		    Ash			DEC(10, 2) UNSIGNED,
		    Carbohydrt	DEC(10, 2) UNSIGNED,
		    Fiber_TD	DEC(10, 1) UNSIGNED,
		    Sugar_Tot	DEC(10, 2) UNSIGNED,
		    Calcium		DEC(10, 0) UNSIGNED,
		    Iron		DEC(10, 2) UNSIGNED,
		    Magnesium	DEC(10, 0) UNSIGNED,
		    Phosphorus	DEC(10, 0) UNSIGNED,
		    Potassium	DEC(10, 0) UNSIGNED,
		    Sodium		DEC(10, 0) UNSIGNED,
		    Zinc		DEC(10, 2) UNSIGNED,
		    Copper		DEC(10, 3) UNSIGNED,
		    Manganese	DEC(10, 3) UNSIGNED,
		    Selenium	DEC(10, 1) UNSIGNED,
		    Vit_C		DEC(10, 1) UNSIGNED,
		    Thiamin		DEC(10, 3) UNSIGNED,
		    Riboflavin	DEC(10, 3) UNSIGNED,
		    Niacin		DEC(10, 3) UNSIGNED,
		    Panto_acid	DEC(10, 3) UNSIGNED,
		    Vit_B6		DEC(10, 3) UNSIGNED,
		    Folate_Tot	DEC(10, 0) UNSIGNED,
		    Folic_acid	DEC(10, 0) UNSIGNED,
		    Food_Folate	DEC(10, 0) UNSIGNED,
		    Folate_DFE	DEC(10, 0) UNSIGNED,
		    Choline_Tot	DEC(10, 0) UNSIGNED,
		    Vit_B12		DEC(10, 2) UNSIGNED,
		    Vit_A_IU	DEC(10, 0) UNSIGNED,
		    Vit_A_RAE	DEC(10, 0) UNSIGNED,
		    Retinol		DEC(10, 0) UNSIGNED,
		    Alpha_Carot	DEC(10, 0) UNSIGNED,
		    Beta_Carot	DEC(10, 0) UNSIGNED,
		    Beta_Crypt	DEC(10, 0) UNSIGNED,
		    Lycopene	DEC(10, 0) UNSIGNED,
		    "Lut+Zea"	DEC(10, 0) UNSIGNED,
		    Vit_E		DEC(10, 2) UNSIGNED,
		    Vit_D_mcg	DEC(10, 1) UNSIGNED,
		    Vit_D_IU	DEC(10, 0) UNSIGNED,
		    Vit_K		DEC(10, 1) UNSIGNED,
		    FA_Sat		DEC(10, 3) UNSIGNED,
		    FA_Mono		DEC(10, 3) UNSIGNED,
		    FA_Poly		DEC(10, 3) UNSIGNED,
		    Cholestrl	DEC(10, 3) UNSIGNED,
		    GmWt_1		DEC(9, 2) UNSIGNED,
		    GmWt_Desc1	VARCHAR(120),
		    GmWt_2		DEC(9, 2) UNSIGNED,
		    GmWt_Desc2	VARCHAR(120),
		    Refuse_Pct	DEC(2,0) UNSIGNED,
			PRIMARY KEY(NDB_No)
		  )'
		);

		echo ">> Creation of " . $dbname . " tables is complete. \n\n";
	}


	/*********************************
	 * Main program starts here
	 */
	$dbname = $cfg[ "dbname" ];
	$username = $cfg[ "username" ];
	$password = $cfg[ "password" ];
	$hostname = $cfg[ "hostname" ];

	try {
		echo ">> Connecting to db " . $dbname . "\n";
		$dbh = new PDO( "mysql:host=$hostname;dbname=$dbname",
						$username ,
						$password ,
						array (
							PDO::MYSQL_ATTR_LOCAL_INFILE => true
						)
					);
	}
	catch( PDOException $e ) {
	    echo $e->getMessage();
	}

	createSchema( $dbh );

	/****************************************
	 * Import data
	 */
	echo ">> Starting data import into " . $dbname . "...\n\n";

    // Import the data from the delimited text files
	importData( "FOOD_DES.txt", "FOOD_DES", $dbname, $dbh );
	importData( "FD_GROUP.txt", "FD_GROUP", $dbname, $dbh );
	importData( "NUTR_DEF.txt", "NUTR_DEF", $dbname, $dbh );
	importData( "SRC_CD.txt", "SRC_CD", $dbname, $dbh );
	importData( "DERIV_CD.txt", "DERIV_CD", $dbname, $dbh );
	importData( "WEIGHT.txt", "WEIGHT", $dbname, $dbh );
	importData( "DATA_SRC.txt", "DATA_SRC", $dbname, $dbh );
	importData( "FOOTNOTE.txt", "FOOTNOTE", $dbname, $dbh );
	importData( "NUT_DATA.txt", "NUT_DATA", $dbname, $dbh );
	importData( "LANGDESC.txt", "LANGDESC", $dbname, $dbh );
	importData( "LANGUAL.txt", "LANGUAL", $dbname, $dbh );
	importData( "DATSRCLN.txt", "DATSRCLN", $dbname, $dbh );


	// Close the db connection
	echo "\n>> Closing connection to " . $dbname . "\n\n";
	$dbh = null;

	echo ">>> Import of data into "	. $dbname . " is complete! <<<\n\n";

?>
