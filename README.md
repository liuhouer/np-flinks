# np-flink
# flink详细学习实践


## 在 MacOS 上安装 Kafka
记录一下在 Mac 上安装和测试 kafka 的步骤。

MacOS 上可以方便的使用 brew 进行安装。

安装

如果还没有安装Java, 可以先安装Java: 
`brew cask install java`


然后安装zookeeper和kafka。

`brew install kafka
brew install zookeeper`
修改 **/usr/local/etc/kafka/server.properties**, 找到 **listeners=PLAINTEXT://:9092** 那一行，把注释取消掉。
然后修改为:
`############################# Socket Server Settings #############################
# The address the socket server listens on. It will get the value returned from 
# java.net.InetAddress.getCanonicalHostName() if not configured.
#   FORMAT:
#     listeners = listener_name://host_name:port
#   EXAMPLE:
#     listeners = PLAINTEXT://your.host.name:9092
listeners=PLAINTEXT://localhost:9092`
启动

如果想以服务的方式启动，那么可以:

`$ brew services start zookeeper
$ brew services start kafka`

如果只是临时启动，可以:
`$ zkServer start
$ kafka-server-start /usr/local/etc/kafka/server.properties`
创建Topic
`$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flink000`
查看所有topic
`
kafka-topics --list --zookeeper localhost:2181
`
产生消息
`$ kafka-console-producer --broker-list localhost:9092 --topic flink000
>HELLO Kafka
`
消费

简单方式:
`$ kafka-console-consumer --bootstrap-server localhost:9092 --topic flink000 --from-beginning
`
如果使用消费组:
`kafka-console-consumer --bootstrap-server localhost:9092 --topic flink000 --group test-consumer1 --from-beginning
`

Producer：消息生产者。
Broker：kafka集群中的服务器。
Topic：消息的主题，可以理解为消息的分类，kafka的数据就保存在topic。在每个broker上都可以创建多个topic。
Partition：Topic的分区，每个topic可以有多个分区，分区的作用是做负载，提高kafka的吞吐量。
Replication：每一个分区都有多个副本，副本的作用是做备胎。当主分区（Leader）故障的时候会选择一个备胎（Follower）上位，成为Leader。在kafka中默认副本的最大数量是10个，且副本的数量不能大于Broker的数量，follower和leader绝对是在不同的机器，同一机器对同一个分区也只可能存放一个副本（包括自己）。
Consumer：消息消费者。
Consumer Group：我们可以将多个消费组组成一个消费者组，在kafka的设计中同一个分区的数据只能被消费者组中的某一个消费者消费。同一个消费者组的消费者可以消费同一个topic的不同分区的数据，这也是为了提高kafka的吞吐量！
Zookeeper：kafka集群依赖zookeeper来保存集群的的元信息，来保证系统的可用性。


