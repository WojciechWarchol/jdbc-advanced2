Training project of an application that creates a number of tables with a given number of random columns and given number of random content. 

To run specify **--spring.config.location** parameter. 
Use one of two configuration files: **hsqldb-embedded-configuration.properties** for embedded hsqldb
and **hsqldb-standalone-configuration.properties** when running hsqldb in stand-alone mode. 
Run hsqldb with this command: 
> java -cp .\hsqldb-2.7.0-jdk8.jar org.hsqldb.Server --database.0 mem:jdbcadvanced --dbname.0 jdbcadvanced

Both files have parameters to configure how the application behaves.