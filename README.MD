**Data transformation and filtering project**

Executable jar provides ability to filter and transform input data using configuration files and generate output file with transformed data.
Columns and rows order may be differ from input files. 

Program can be executed from command line and configured via command line arguments.
Help text will be shown in case you did not pass enough parameters required for execution.
There are 3 input files:
- Input data file (required)
- Columns mapping file (required)
- Identifier mapping file (optional)

You can use one of following protocols to define URL for this files:
- file: - for files stored locally
- ftp: - for file stored on ftp. Please make sure that files are available without authorization
- http: - for files downloadable via http

To map ids properly you should pass also ID column name parameter which corresponds to name of ID column in input data file.
In case you do not pass 'Identifier mapping file' and column name data will not be filtered by ID's and ids will not be mapped.

There is one output file. You can use file path without protocols. Only local file saving supported right now.

Also, delimiter and batch size for reading input file are configurable, but they are optional cause have default values.

**Requirements:**

- Java 8 installed
- Maven 3 installed (for build purposes)


**How to build ?**

Use mvn command ot corresponding ability of your IDE:
 
`mvn package`

You will be able to find file 'transform-1.0-SNAPSHOT.jar' in target folder after package complete.

**How to run ?**

You should already have jar file built (Ex.: transform-1.0-SNAPSHOT.jar). If you do not have one please build project.
To execute you can use command line:

`java -jar transform-1.0-SNAPSHOT.jar`

You should have java 8 installed. You will see help text about available options like this:

`Available options: `

` -f : URL of input data file. Please use with 'file:', 'ftp:' or 'http:' protocols`

` -o : Path for output file. This is local path so no need to use protocols`

` -c : URL of columns mapping file. Please use with 'file:', 'ftp:' or 'http:' protocols`

` -i : (Optional) URL of identifier mapping file. Please use with 'file:', 'ftp:' or 'http:' protocols. No ID mapping will be performed in case parameters '-i' and '-ic' is not passed`

` -ic : (Optional) Name of identifier column in input file. Required if id mapping file specified`

` -d : (Optional) Custom delimiter to parse all input files and to use in output file. Default delimiter is \t`

` -b : (Optional) Custom batch size for reading input file. Default value is 16`

Please follow instructions. Example of correct command line execution command will looks like:

`java -jar transform-1.0-SNAPSHOT.jar -f file:large-data.csv -c file:column-mapping.csv -i file:identifier-mapping.csv -ic THEIR_ID -o output.csv`

**Testing results:**

Environment configuration:

`Intel® Core™ i5-2500 CPU @ 3.30GHz × 4; 8 GB; SSD OCZ Vertex3` 

Input data size:
- Rows: `500 Columns  X  300 000 rows`
- Size: `2.5 Gb`

Filtering files conditions:
`50% Rows & 80% columns`

Output file:
- Size: `986 Mb`

Processing time: `~20 secconds`