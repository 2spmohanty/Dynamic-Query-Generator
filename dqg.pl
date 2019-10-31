#!/usr/bin/perl

use warnings;
#use diagnostics;
use Carp qw/croak carp/;
use POSIX 'strftime';
use Getopt::Long qw(GetOptions);


$waytouse = << "end_usage";

                                  dqg.pl (Dynamic Query Generator) script.


Author: Smruti P Mohanty (2spmmohanty at gmail dot com )
                         
==========================================       Usage        ======================================

./dqg.pl [-r 0/1] -t 'table_name or t1 joins t2' [-n num_of_columns] [-p projection_file]
   [-w predicate_file] [-s 'sub_queries_file'] [-c cqd_settings_file] [-e 0/1] [-o outfilename]
   [-d 'l:2000' / 'b:1024' / 'q:2500']

*** Values in square bracket are optional parameters. Refer to the use cases below.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  Use Cases ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(1)Case 1 when dqg.dqg.table_name Table has column named as COL[i] where i is an integer less than equal
to value given for n.
./dqg.pl -t 'dqg.dqg.table_name' -n 10 -p sample_project_file [-o sample_output_filename]


(2)Case 2 when Table name is 'dqg.dqg.table_name' and a ''sample_predicate_file'' is given as the predicate.
./dqg.pl -t 'dqg.dqg.table_name' -p sample_project_file -w sample_predicate_file [-o sample_output_filename]
*** When sample_predicate_file is given [-n num_of_columns] option is ignored.


(3)Case 3 when Table name is 'dqg.dqg.table_name' and -r is set to 1.
./dqg.pl -r 1 'dqg.dqg.table_name' -p sample_project_file [-o sample_output_filename]

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  End Use Cases ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

******************************** Parameter Explanation**************************************************
./dqg.pl
       -r apart from 0 if set to any value then read from metadata.*
       -t table_name
       -n number of columns. Ignored when a predicate file is passed.
       -p projection file.
       -w predicate file.
       -s subqueries file.*
       -c cqd_settings_file
       -e apart from 0 if set to any integer, the script generate that many embeded sqls.*
       -o name of output file appended with current timestamp. If ignored then deafult name is output_timestamp.sql
       -d if given with option l(l:2000) split the files on the basis of number of lines specfied.
          if given with option b(b:1024) split the files on the basis of number of bytes specfied.
          if given with option q(q:2500) split the files on the basis of number of queries specfied.
          
*********************************************************************************************************
end_usage

$myhome=`pwd`;
chomp $myhome;

$line=undef;
$pred1 = undef;
$pred2 = undef;
$count=0;
$numofquery=0;
@accessoption = ('READ UNCOMMITTED', 'READ COMMITTED', 'SERIALIZABLE', 'REPEATABLE READ', 'SKIP CONFLICT', 'STABLE');
$dt = strftime '%d%M%S', localtime;

$readmeta = 0;
$tablename='default';
$numcols=0;
$projectionlist='default';
$predicatelist='default';
$sub_q_list='default';
$cqd_setting='default';
$embsql=0;
$outfilename = 'output';
$divfile='default';

GetOptions(
     'r=i' => \$readmeta,
     't=s' => \$tablename,
     'n=i' => \$numcols,
     'p=s' => \$projectionlist,
     'w=s' => \$predicatelist,
     'q=s' => \$sub_q_list,
     'c=s' => \$cqd_setting,
     'e=i' => \$embsql,
     'o=s' => \$outfilename,
     'd=s' => \$divfile) or die "Usage: $waytouse\n";
     
sub open_file_write {
	

	$outfile_read_uncommited=$myhome."/".$outfilename.$dt."_rduncmt.sql";
	chomp $outfile_read_uncommited;
	
	$outfile_read_commited=$myhome."/".$outfilename.$dt."_rdcmt.sql";
	chomp $outfile_read_commited;
	
	$outfile_serializable=$myhome."/".$outfilename.$dt."_srl.sql";
	chomp $outfile_serializable;
	
	$outfile_repeatable_read=$myhome."/".$outfilename.$dt."_rptbl.sql";
	chomp $outfile_repeatable_read;
	
	$outfile_skip_conflict=$myhome."/".$outfilename.$dt."_skpcnflt.sql";
	chomp $outfile_skip_conflict;
	
	$outfile_stable=$myhome."/".$outfilename.$dt."_stbl.sql";
	chomp $outfile_stable;

	open $read_uncommited , ">>" , $outfile_read_uncommited or carp "Could not open '$outfile_read_uncommited' for writing $!";
	open $read_commited , ">>" ,$outfile_read_commited or carp "Could not open '$outfile_read_commited' for writing $!";
	open $read_serializable , ">>" , $outfile_serializable or carp "Could not open '$outfile_serializable' for writing $!";	
	open $read_repeatable_read , ">>" ,$outfile_repeatable_read or carp "Could not open '$outfile_repeatable_read' for writing $!";	
	open $read_skip_conflict , ">>" ,$outfile_skip_conflict	or carp "Could not open '$outfile_skip_conflict' for writing $!";
	open $read_stable , ">>" ,$outfile_stable or carp "Could not open '$outfile_stable' for writing $!";
		
	}
     
sub apply_cqd {
	if($cqd_setting ne 'default'){
		open $ch , "<" , $cqd_setting or carp "Could not open $cqd_setting for reading $!";
		while($cqdlist = <$ch>){
			next if $cqdlist =~/^$/;
			print $read_uncommited "$cqdlist";
			print $read_commited "$cqdlist";
			print $read_serializable "$cqdlist";
			print $read_repeatable_read "$cqdlist";
			print $read_skip_conflict "$cqdlist";
			print $read_stable "$cqdlist";
			}
		}
}


sub print_to_file {
	@itemprint = @_;
	
	foreach $item(@itemprint){
		chomp $item;
				if($typeaccess eq 'READ UNCOMMITTED'){
					print $read_uncommited "$item \n";
				}elsif($typeaccess eq 'READ COMMITTED'){
					print $read_commited "$item\n";
				}elsif($typeaccess eq 'SERIALIZABLE'){
					print $read_serializable "$item\n";
				}elsif($typeaccess eq 'REPEATABLE READ'){
					print $read_repeatable_read "$item\n";
				}elsif($typeaccess eq 'SKIP CONFLICT'){
					print $read_skip_conflict "$item \n";
				}else{
					print $read_stable "$item\n";
				}
		}
}


if($tablename eq 'default' ){
		die "$waytouse \n";
		}
		
############################################## Begin Case 1 ########################################
 
elsif ($readmeta==0 && $tablename ne 'default' && $numcols != 0 && $projectionlist ne 'default' && $predicatelist eq 'default' && $embsql == 0){ # Begin of Case 1. Refer Way to use.
 #  print "$tablename \n  $numcols \n $projectionlist \n"; # for debug only
 print "\n\nI am not reading from meta-data as option 'r' is not passed with a value greater than 0.\n\n";
 print "Dynamic Query Generated after reading projections options from $projectionlist file and randomly generated predicates.\n\n]I assumed that your predicate columns have name COL[i] where i <= n (number of columns)\n";
   open_file_write();  #opening output file handles.
   apply_cqd(); #calling cqd module if applicable.
   open $fh1 , "<" , $projectionlist or carp "Could not open $projectionlist for reading $!"; #opening projection_file for reading.
   while($projectionitem = <$fh1>){
      
      $line = $projectionitem;
      chomp $line;
      next if $line =~/^$/;
      $groupbyvar="";
      $orderbyvar="";
      if($line=~m/.+?GROUP.*/i || $line=~m/.+?ORDER.*/i ){ #Checking if the query needs to have Order by and Group by clauses.
         @testarr = split(/:/,$line);
         if (scalar @testarr == 3){
            ($line,$groupbyvar, $orderbyvar)=split(/:/,$line);
            }else{
               ($line,$groupbyvar) = split(/:/,$line);
               }
         } # closing the check of order by and group by clause.
         for ($x=1;$x <= $numcols; $x++){ #opening left predicate column
            $pred1 = "COL".$x;                # Generating COL.integer_value
            
			for($y=1;$y<= $numcols; $y++){ #opening Right predicate column
               next if $y == $x;
               $pred2= "COL".$y;
			   
               foreach(@accessoption){
                  $typeaccess = $_;
                  chomp $typeaccess;
				  
                  
                  
                  
$withequaloperator = << "equal_operator";
select $line
from $tablename
where $pred1 = $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

equal_operator

$withnotequaloperator = << "not_equal_operator";
select $line
from $tablename
where $pred1 <> $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

not_equal_operator

$greateroperator = << "greater_operator";
select $line
from $tablename
where $pred1 > $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

greater_operator

$lesseroperator = << "lesser_operator";
select $line
from $tablename
where $pred1 < $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

lesser_operator

$greaterequal = << "greaterequal_operator";
select $line
from $tablename
where $pred1 >= $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

greaterequal_operator

$lesserequal = << "lesserequal_operator";
select $line
from $tablename
where $pred1 <= $pred2 $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

lesserequal_operator

 print_to_file($withequaloperator,$withnotequaloperator,$greateroperator,$lesseroperator,$greaterequal,$lesserequal);

			} #Closing the Access Option Loop.
          } # Closing Number of Right Predicate column loop.
            
        } # Closing Number of Left Predicate column loop.
      }# Closing the reading of Projection file.
   
} # Closing Case 1 Logic.


############################################## End Case 1 #########################################


############################################## Begin Case 2 ########################################

elsif ($readmeta==0 && $tablename ne 'default' && $projectionlist ne 'default' && $predicatelist ne 'default' && $embsql == 0){ #Begin case 2
	open_file_write(); #Open files to write.
#	print " Table: $tablename\n projectionlist: $projectionlist\n predicatelist: $predicatelist\n "; #for debug only
print "\nI am not reading from meta-data as option 'r' is not passed with a value greater than 0.\n\n";
print "Dynamic Query Generated after reading projections options from \'$projectionlist\' file and \npredicate options from \'$predicatelist\' file.";
        
        
	apply_cqd(); #calling cqd module if applicable.
        
        open $fh1 , "<" , $projectionlist or carp "Could not open '$projectionlist' for reading $!"; #open file handle to read projection list
	
	while($projectionitem = <$fh1>){ #Reading projection list line by line
		$line = $projectionitem;
		chomp $line;
		next if $line =~/^$/;
		$groupbyvar="";
		$orderbyvar="";
		if($line=~m/.+?GROUP.*/i || $line=~m/.+?ORDER.*/i ){  #Deciding if Projection list contains Order By or Group by clause
			@testarr = split(/:/,$line);
			if (scalar @testarr == 3){
			($line,$groupbyvar, $orderbyvar)=split(/:/,$line);
			}else{
			($line,$groupbyvar) = split(/:/,$line);
			}
		}#Closing Assigning of Projection , Order By and Group By vars.
		foreach(@accessoption){ #Opening elements of each access option
		    
			        $typeaccess = $_;
					chomp $typeaccess;
					open $predicatehandle, "<" ,$predicatelist or carp "Could not open '$predicatelist' for reading $!"; #opening predicate list handle to read predicate file.
					while($predicateline = <$predicatehandle>){ #Reading predicate list line by line from predicate handle
						next if $predicateline =~/^$/;
						@predicatearray = split(/:/,$predicateline); # Checking for AND, OR , ANY, SOME, IN OPTION
						
						
if (scalar @predicatearray == 2){ # For And or OR option


chomp $predicatearray[0];
chomp $predicatearray[1];	
					
$withandoperator = << "and_operator";
select $line
from $tablename
where $predicatearray[0] AND $predicatearray[1] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

and_operator


$withoroperator = << "or_operator";
select $line
from $tablename
where $predicatearray[0] OR $predicatearray[1] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

or_operator

print_to_file($withandoperator,$withoroperator);

}#Closing And or OR option
elsif(scalar @predicatearray == 1){ # Opening Loop for one predicate

chomp $predicatearray[0];	
$withonepredicate = << "one_predicate";
select $line
from $tablename
where $predicatearray[0] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

one_predicate


print_to_file($withonepredicate);
				
}#Closing one option predicate
elsif(scalar @predicatearray == 3){ #Opening for 3 parameters in predicatearray
	if($predicatearray[1]=~m/.+?IN.*/i){#opening FOR IN OR NOT IN Predicate.

chomp $predicatearray[0];
chomp $predicatearray[2];
$withinpredicate = << "in_predicate";
select $line
from $tablename
where $predicatearray[0] IN $predicatearray[2] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

in_predicate


$withnotinpredicate = << "notin_predicate";
select $line
from $tablename
where $predicatearray[0] NOT IN $predicatearray[2]  $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

notin_predicate

print_to_file($withinpredicate,$withnotinpredicate);

}#Closing FOR IN OR NOT IN Predicate.
elsif($predicatearray[1]=~m/.+?ANY.*/i || $predicatearray[1]=~m/.+?ALL.*/i || $predicatearray[1]=~m/.+?SOME.*/i){ #Opening for ALL, ANY, SOME predicate

chomp $predicatearray[0];
chomp $predicatearray[2];
chomp $predicatearray[1];

$withequalanypredicate = << "equalany_predicate";
select $line
from $tablename
where $predicatearray[0] = $predicatearray[1] $predicatearray[2] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

equalany_predicate


$withgreateranypredicate = << "greaterany_predicate";
select $line
from $tablename
where $predicatearray[0] > $predicatearray[1] $predicatearray[2] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

greaterany_predicate


$withglesseranypredicate = << "lesserany_predicate";
select $line
from $tablename
where $predicatearray[0] < $predicatearray[1] $predicatearray[2] $groupbyvar $orderbyvar
FOR $typeaccess ACCESS;

lesserany_predicate

print_to_file($withequalanypredicate,$withgreateranypredicate,$withglesseranypredicate);
				

}#Closing for ALL, ANY, SOME predicate
else{
	next;
	}
		
       }#Closing for 3 parameters in predicatearray
       
      }#Closing for reading predicatehandle
     
     }#Closing for reading elements of each accessoption
    
    }#Closing reading of projection list
 
   }#Closing of Case 2 Loop

############################################## End Case 2 ########################################


############################################## Begin Case3 ########################################

elsif ($readmeta==1 && $tablename ne 'default' && $projectionlist ne 'default' && $predicatelist eq 'default' && $embsql == 0){ #opening logic for reading metadata and forming queries without predicate file.


 print "\n\nI am reading from meta-data as option 'r' is set to 1.\n\n";
 print "Generating Dynamic Query after reading projections options from \'$projectionlist\' file \nand randomly generated predicates with validation from table metadata.\n\n";
  
system("echo \"showddl $tablename ;\" | mxci > dqgtemp ");
 


	
	$meta_data ='dqgtemp';
	
	if (!(-e -s $meta_data )){
		die "Issue while generating temporary copy of metadata file.\nQuitting dqg script.\n\n";
		}


        open $rdmt, "<", $meta_data or carp "Could not open $meta_data file for reading : \n$!"; # Reading output of metadata showddl command.
	
	
	@NUMB=();
	@CHARCT=();
	@DAT=();
	@TIM=();
	@TSTAMP=();
	@INTERYEAR=();
	@INTERTIME=();
	
	while($syntaxline = <$rdmt>){ #Segregating Column Types
	        next if $syntaxline =~/^$/;
	        chomp $syntaxline;
                
		
		if ($syntaxline =~m/\.*?ERROR.*/igm){
			die "Please check the ddl of the table manually. I believe it contains some kind of ERRORS.\n";
			}
		
		if ($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(DECIMAL).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(NUMERIC).*/igm || 
		    $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INT)\b.*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(SMALLINT).*/igm || 
			$syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(LARGEINT).*/igm ||$syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(FLOAT).*/igm ||
            $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(REAL).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(DOUBLE).*/igm){
			chomp $1;
			push @NUMB, $1;
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(CHAR).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(VARCHAR).*/igm ){
			chomp $1;
			push @CHARCT, $1;
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(DATE).*/igm ){
			chomp $1;
			push @DAT, $1;		
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(TIME\s*\().*/igm ){
			chomp $1;
			push @TIM,$1;
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(TIMESTAMP\s*\().*/igm ){
			chomp $1;
			push @TSTAMP, $1;
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL YEAR).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL MONTH).*/igm){
			chomp $1;
			push @INTERYEAR,$1;
		}elsif($syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL DAY).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL HOUR).*/igm ||
		$syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL MINUTE).*/igm || $syntaxline=~m/\s*[,]*\s*([[:alnum:]]+)\s+(INTERVAL SECOND).*/igm){
			chomp $1;
			push @INTERTIME,$1;		
		}
		}#Closing #Segregating Column Types

#************* Validation for Predicate''s Data Types if occuring only Once
@onepredicate= ();
				 
  if(@NUMB == 1){ 
  	chomp $NUMB[0];
  	push @onepredicate, $NUMB[0];  	
  }
  if(@CHARCT == 1){
  	chomp $CHARCT[0];
  	push @onepredicate, $CHARCT[0];
  }
  if(@DAT == 1){
  	chomp $DAT[0];
  	push @onepredicate, $DAT[0];
  }
  if(@TIM == 1){
  	chomp $TIM[0];
  	push @onepredicate, $TIM[0];
  }
  if(@TSTAMP == 1){
  	chomp $TSTAMP[0];
  	push @onepredicate, $TSTAMP[0];
  }
  if(@INTERYEAR == 1){
  	chomp $INTERYEAR[0];
  	push @onepredicate, $INTERYEAR[0];
  }
  if(@INTERTIME == 1){
  	chomp $INTERTIME[0];
  	push @onepredicate, $INTERTIME[0];
  }
  
#print "Onepredicate array: @onepredicate \n"; # for debug only
#*************End of Validation for Predicate''s Data Types if occuring only Once 

#************* Start of Validation when each data type is occuring multiple times.
@multinum=();
  if(@NUMB > 1){ #Validation if multiple occurence of NUM,INT,DECIMAL,DOUBLE,REAL, SMALLINT,LARGEINT 
  	for($i=0; $i < scalar @NUMB ; $i++){
  		for($j=$i+1; $j < scalar @NUMB; $j++){
  			push @multinum, "$NUMB[$i] = $NUMB[$j]";
  			push @multinum, "$NUMB[$i] < $NUMB[$j]";
  			push @multinum, "$NUMB[$i] > $NUMB[$j]";
  			push @multinum, "$NUMB[$i] <> $NUMB[$j]";  			
  			}
  		}
  	  	
  } # Closing of Validation if multiple occurence of NUM,INT,DECIMAL,DOUBLE,REAL, SMALLINT,LARGEINT 
 
 
 
@multichar = ();
 if(@CHARCT > 1){ #Validation if multiple occurence of CHAR, VARCHAR datatype.
 	for($i=0; $i < scalar @CHARCT ; $i++){
 		for($j=$i+1; $j < scalar @CHARCT; $j++){
 			push @multichar, "$CHARCT[$i] = $CHARCT[$j]";
 			push @multichar, "$CHARCT[$i] <> $CHARCT[$j]"; 			
 			
			}
 		
		}
 	 
 	}# Closing of Validation if multiple occurence of CHAR, VARCHAR datatype.
 	
 	
@multidat=();
 if(@DAT > 1){#Validation if multiple occurence of DATE
 	for($i=0; $i < scalar @DAT ; $i++){
		for($j=$i+1; $j < scalar @DAT; $j++){
			push @multidat, "$DAT[$i] = $DAT[$j]";
			push @multidat, "$DAT[$i] < $DAT[$j]";
			push @multidat, "$DAT[$i] > $DAT[$j]";
			push @multidat, "$DAT[$i] <> $DAT[$j]";
			}
		 }		
	}#Closing Validation if multiple occurence of DATE



@multitim = ();
if(@TIM > 1){#Validation if multiple occurence of TIME
	for($i=0; $i < scalar @TIM ; $i++){
		for($j=$i+1; $j < scalar @TIM; $j++){
			push @multitim, "$TIM[$i] = $TIM[$j]";
			push @multitim, "$TIM[$i] < $TIM[$j]";
			push @multitim, "$TIM[$i] > $TIM[$j]";
			push @multitim, "$TIM[$i] <> $TIM[$j]";
			}
		}
	}#Closing Validation if multiple occurence of TIME
	
	
@multitstamp = ();
if(@TSTAMP > 1){#Validation if multiple occurence of TIMESTAMP
	for($i=0; $i < scalar @TSTAMP ; $i++){
		for($j=$i+1; $j < scalar @TSTAMP; $j++){
			push @multitstamp, "$TSTAMP[$i] = $TSTAMP[$j]";
			push @multitstamp, "$TSTAMP[$i] < $TSTAMP[$j]";
			push @multitstamp, "$TSTAMP[$i] > $TSTAMP[$j]";
			push @multitstamp, "$TSTAMP[$i] <> $TSTAMP[$j]";
			}
		}
	}#Closing Validation if multiple occurence of TIMESTAMP
	
	
@multintryr = ();
if(@INTERYEAR > 1){#Validation if multiple occurence of INTERVAL YEAR, INTERVAL MONTH
	for($i=0; $i < scalar @INTERYEAR ; $i++){
		for($j=$i+1; $j < scalar @INTERYEAR; $j++){
			push @multintryr, "$INTERYEAR[$i] = $INTERYEAR[$j]";
			push @multintryr, "$INTERYEAR[$i] < $INTERYEAR[$j]";
			push @multintryr, "$INTERYEAR[$i] > $INTERYEAR[$j]";
			push @multintryr, "$INTERYEAR[$i] <> $INTERYEAR[$j]";
			}
		}
	}#Closing Validation if multiple occurence of INTERVAL YEAR, INTERVAL MONTH
	


@multintrtim = ();
if(@INTERTIME > 1){#Validation if multiple occurence of INTERVAL DAY,INTERVAL HOUR,INTERVAL MINUTE,INTERVAL SECOND
	for($i=0; $i < scalar @INTERTIME ; $i++){
		for($j=$i+1; $j < scalar @INTERTIME; $j++){
			push @multintrtim, "$INTERTIME[$i] = $INTERTIME[$j]";
			push @multintrtim, "$INTERTIME[$i] < $INTERTIME[$j]";
			push @multintrtim, "$INTERTIME[$i] > $INTERTIME[$j]";
			push @multintrtim, "$INTERTIME[$i] <> $INTERTIME[$j]";
			}
		}
	}#Closing Validation if multiple occurence of TIMESTAMP	
	
@multiple_predicate = (@multinum,@multichar,@multidat,@multitim,@multitstamp,@multintryr,@multintrtim);

#************* End of Validation when each data type is occuring multiple times.


 open_file_write();  
		
 apply_cqd();
		
 open $fh1 , "<" , $projectionlist or carp "Could not open '$projectionlist' for reading $!"; #open file handle to read projection list
	
#print "NUMB = @NUMB \n"	; # for debug only
#print "DAT = @DAT \n"; # for debug only
#print "CHARACTER = @CHARCT \n"; # for debug only
        
        
     while($projectionitem = <$fh1>){ # Opening Projection file for reading
             
      $line = $projectionitem;
      chomp $line;
      next if $line =~/^$/;
      $groupbyvar="";
      $orderbyvar="";        	
	
	
      if($line=~m/.+?GROUP.*/i || $line=~m/.+?ORDER.*/i ){ #Checking if the query needs to have Order by and Group by clauses.
         @testarr = split(/:/,$line);
         if (scalar @testarr == 3){
            ($line,$groupbyvar, $orderbyvar)=split(/:/,$line);
            }else{
               ($line,$groupbyvar) = split(/:/,$line);
               }
      } # closing the check of order by and group by clause.
         
foreach(@accessoption){
                  
	$typeaccess = $_;
        chomp $typeaccess;

  
if(@onepredicate > 0) { #query formation on data types occuring once in the table. 
	foreach $pred1(@onepredicate){
$withnulloperator = << "null_operator";
select $line
from $tablename
where $pred1 is NULL $groupbyvar $orderbyvar 
FOR $typeaccess ACCESS;

null_operator


$withnotnulloperator = << "notnull_operator";
select $line
from $tablename
where $pred1 is NOT NULL $groupbyvar $orderbyvar 
FOR $typeaccess ACCESS;

notnull_operator
		
print_to_file($withnulloperator,$withnotnulloperator);


		
         }#closing foreach onepredicate loop
	

  } #closing the query formation on data types occuring once in the table.      
                  
if(@multiple_predicate > 0){#opening query formation on multiple occurence of different data types.
      foreach $syntaxitem(@multiple_predicate){
      	
      	   chomp $syntaxitem;
      	
$syntaxqueries = << "end_of_syntax";
select $line
from $tablename
where $syntaxitem $groupbyvar $orderbyvar 
FOR $typeaccess ACCESS;

end_of_syntax


print_to_file($syntaxqueries);
      	         }# Closing of foreach for multiple_predicate array.
				
	    } # Closing query formation on multiple occurence of occurence of different data types.

         

         
       }#Closing of Access Options
    
   }#closing projection file reading
   
unlink $meta_data;

}#closing logic for reading metadata and forming queries without predicate file.

############################################## End Case 3 ########################################
else{
	print "\n\nI couldnot understand your options / your options is yet to be implemented.\n\nKindly read through the usage to understand the current implementation. \n\n\n";
	die "$waytouse\n";
	}

############################################Starting Printing to Console#########################################


sub countQuery {
  open my $fh, '<', $outfile_read_uncommited or die "Could not open one of the output file to count number of queries: $!";
  return [ grep m|;|, <$fh> ];
}

$lines = countQuery;
$count = @$lines;
$numofquery = $count*6;

print "\n\n         A total of **** $numofquery **** queries were generated.\n\n";

$outputfiles = << "END_OUTPUTFILE";

#################################################
The main output files are available at :
$outfile_read_uncommited
$outfile_read_commited
$outfile_serializable
$outfile_repeatable_read	
$outfile_skip_conflict
$outfile_stable
##################################################

END_OUTPUTFILE

print "$outputfiles\n";

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Logic to Split files based on user Input ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@allfiles=($outfile_read_uncommited,$outfile_read_commited,$outfile_serializable,$outfile_repeatable_read,$outfile_skip_conflict,$outfile_stable);

if($divfile ne 'default'){
	print "Splitting output files as option d is set.\n";
	chomp $divfile;
	($style,$number)=split(/:/,$divfile);
	chomp $style;
	chomp $number;
	
if($style eq 'b'){
	print"Generating outputfiles splited per size $number bytes each.\n";
	print "Style: Size in Bytes\n";
	print "Number: $number\n";
	$inc = 0;
	foreach $singlefile(@allfiles){
		$inc++;
		system("split -b $number $singlefile $singlefile.$inc");
	}
}elsif($style eq 'l'){
	print"Generating outputfiles splited per $number lines each.\n";
	print "Style: Line per file\n";
	print "Number: $number\n";
	foreach $singlefile(@allfiles){
		$inc++;
		system("split -l $number $singlefile $singlefile.$inc");
	}
}elsif($style eq 'q'){
	print "Generating outputfiles splited per $number queries each.\n";
	print "Style: Query per file\n";
	print "Number: $number\n";
	
    foreach $singlefile(@allfiles){
	
		chomp $singlefile;
		
		open $splitread , '<', $singlefile or carp "Cannot open $singlefile for splitting: $!";
		
		$filecount = 1;
		$semicolon = 0;
		
		$filename = $singlefile.$filecount;

		close $splitwrite if $splitwrite;       
		open $splitwrite, '>>', $filename or carp $!;
		
		while($eachline = <$splitread>){
			print $splitwrite "$eachline";
			
			$semicolon++ if $eachline=~m/.+?;\n.*/;
			
			if($semicolon == $number){
				
				close $splitwrite if $splitwrite;
				$semicolon = 1;
				$filecount = $filecount + 1;
				$filename = $singlefile.$filecount;	
				open $splitwrite, '>>', $filename or carp $!;
			 
			}
			
#		    last unless defined $eachline ;		
		}
        	
		close $splitread;
		
	}
		
}else{
	print"Could not understand input to (-d) file splitting options\n";
	
	}
   
   }
	
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Close of Logic to Split files based on user Input ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~	
	
 	
close $read_uncommited or carp "Could not close '$outfile_read_uncommited'  $!";
close $read_commited or carp "Could not close '$outfile_read_commited'  $!";
close $read_serializable  or carp "Could not close '$outfile_serializable'  $!";	
close $read_repeatable_read or carp "Could not close '$outfile_repeatable_read'  $!";	
close $read_skip_conflict or carp "Could not close '$outfile_skip_conflict'  $!";
close $read_stable  or carp "Could not close '$outfile_stable' $!";




################################################### End of Program ################################################
