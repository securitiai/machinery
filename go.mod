module github.com/RichardKnop/machinery

require (
	cloud.google.com/go/pubsub v1.0.1
	cloud.google.com/go/storage v1.1.0 // indirect
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/RichardKnop/redsync v1.2.0
	github.com/aws/aws-sdk-go v1.25.8
	github.com/aws/aws-sdk-go-v2 v1.16.16
	github.com/aws/aws-sdk-go-v2/config v1.17.8
	github.com/aws/aws-sdk-go-v2/service/sqs v1.19.10
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/btcsuite/btcutil v1.0.2
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/golang/groupcache v0.0.0-20191002201903-404acd9df4cc // indirect
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.1.1
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/onsi/ginkgo v1.10.0 // indirect
	github.com/onsi/gomega v1.7.0 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/spf13/cast v1.5.0
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.6.1
	github.com/stvp/tempredis v0.0.0-20181119212430-b82af8480203 // indirect
	github.com/urfave/cli v1.22.1
	go.mongodb.org/mongo-driver v1.11.6
	go.opencensus.io v0.22.1 // indirect
	golang.org/x/exp v0.0.0-20191002040644-a1355ae1e2c3 // indirect
	golang.org/x/lint v0.0.0-20190930215403-16217165b5de // indirect
	golang.org/x/net v0.9.0 // indirect
	google.golang.org/api v0.11.0 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/genproto v0.0.0-20191007204434-a023cd5227bd // indirect
	google.golang.org/grpc v1.24.0 // indirect
	gopkg.in/yaml.v2 v2.2.8
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

go 1.13
