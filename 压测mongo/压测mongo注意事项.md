###配置环境
clone问你项目之后，重点关注mongodb这个moudle
其他没必要的moudle我都已经remove掉了
但是在构建之前需要导入`mongodb-async-driver`里面的那个jar包：mongodb-async-driver-2.0.1.jar
构建命令：
````
mvn install:install-file -Dfile=/Users/jinping/Downloads/mongodb-async-driver/2.0.1/mongodb-async-driver-2.0.1.jar -DgroupId=com.allanbank -DartifactId=mongodb-async-driver -Dversion=2.0.1 -Dpackaging=jar
````
然后等待构建就好了
