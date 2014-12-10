package rados_test

import (
	"github.com/sathlan/libradosgo"
	"log"
)

func Example() {
	cephConf := "/etc/ceph/ceph.conf"
	newRadosPool := "my_test"

	r, err := NewRados(cephConf)
	if err != nil {
		log.Fatal(err)
	}
	if err := r.Connect(); err != nil {
		log.Fatal(err)
	}
	defer r.Shutdown()
	exists, err := r.PoolExists(newRadosPool)
	if err != nil {
		log.Fatal(err)
	}
	if !exists {
		r.CreatePool(newRadosPool)
		defer r.DeletePool(newRadosPool)
	}

	for pool := range r.ListPools() {
		log.Info("Got rados %s", pool)
	}
}
