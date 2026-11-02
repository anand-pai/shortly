package scylla

import (
	"log"
	"shortly/helpers"
	"time"

	"github.com/gocql/gocql"
)

func NewScyllaSession() *gocql.Session {

	host := helpers.GetEnv("SCYLLA_HOST")
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = "txtly"
	cluster.Port = 9042
	cluster.DisableInitialHostLookup = true
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 5 * time.Second

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
