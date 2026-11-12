package scylla

import (
	"log"
	"shortly/helpers"
	"time"

	"github.com/gocql/gocql"
)

func NewScyllaSession() *gocql.Session {


	keyspace := helpers.GetEnv("SCYLLA_KEYSPACE")
	port := helpers.GetEnv("SCYLLA_PORT")
	host := helpers.GetEnv("SCYLLA_HOST")
	disableHostLookup := helpers.GetEnv("SCYLLA_DISABLE_HOST_LOOKUP")
	consistency := helpers.GetEnv("SCYLLA_CONSISTENCY")

	username := helpers.GetEnv("SCYLLA_USERNAME")
	password := helpers.GetEnv("SCYLLA_PASSWORD")

	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Port, _ = strconv.Atoi(port)
	cluster.DisableInitialHostLookup = disableHostLookup == "true"
	cluster.Consistency = gocql.ParseConsistency(consistency)
	cluster.Timeout = 5 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Error create scylla session : %v", err)
	}

	if err := session.Ready(); err != nil {
		log.Fatalf("CQL Session in-active : %v", err)
	}

	log.Println("Scylla DB Connected!")
	return session
}
