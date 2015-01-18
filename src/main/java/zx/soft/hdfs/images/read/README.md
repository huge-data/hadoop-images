ReadImage
========

I have used the opencv java jars to process images inside the mapper
since we don't have opencv maven repositories you will have to install the jar manually

 mvn install:install-file
-Dfile=<path-to-file>
-DgroupId=<group-id>
-DartifactId=<artifact-id>
-Dversion=<version>
-Dpackaging=<packaging>
-DgeneratePom=true

Where: <path-to-file>  the path to the file to load
   <group-id>      the group that the file should be registered under
   <artifact-id>   the artifact name for the file
   <version>       the version of the file
   <packaging>     the packaging of the file e.g. jar
