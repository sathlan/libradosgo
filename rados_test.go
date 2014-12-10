package rados

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func radosCmd(t *testing.T, command ...string) ([]byte, error) {
	out, err := exec.Command("/usr/bin/rados", command...).Output()
	if err != nil {
		return nil, fmt.Errorf("Problem running rbd %v (%v)", strings.Join(command, " "), err)
	}
	return out, nil
}

func setupContext(t *testing.T) (cephConf string) {
	cephConf = os.Getenv("CEPH_CONF")
	if cephConf == "" {
		t.Fatalf("CEPH_CONF is not set in the environment.")
	}
	return
}

func Test_ConnectDisconnect(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	if err := r.Connect(); err != nil {
		t.Errorf("Could not connect to ceph")
	}
	defer r.Shutdown()
	t.Logf("Connected to ceph version %v", r.Version())
	if err := r.Shutdown(); err != nil {
		t.Errorf("Could not disconnect from ceph")
	}
	_, ok := r.PoolExists("rbd")
	if ok == nil {
		t.Errorf("Did not disconnect from the cluster")
	}
}

func Test_FailedPointer(t *testing.T) {
	_, err := NewRados("./unexistant_conf_file")
	if err == nil {
		t.Fatalf("Did not detect failed poinder: %v", err)
	}
}

func Test_CreateDeletePool(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	r.Connect()
	poolName := "test_createpool"
	if err := r.CreatePool(poolName); err != nil {
		t.Errorf("Could not create the pool: %v", err)
	}
	out, err := radosCmd(t, "lspools")
	if err != nil {
		t.Errorf("Problem checking the pool creating")
	}
	if !strings.Contains(string(out[:]), poolName) {
		t.Errorf("The pool wasn't created")
	}
	exists, err := r.PoolExists(poolName)
	if err != nil {
		t.Errorf("Problem detecting the existing pool: %v", err)
	}
	if !exists {
		t.Errorf("Could not find the existing pool: %v", err)
	}
	if err := r.DeletePool(poolName); err != nil {
		t.Errorf("Could not delete the pool: %v", err)
	}
	out, err = radosCmd(t, "lspools")
	if err != nil {
		t.Errorf("Problem checking the pool delete")
	}
	if strings.Contains(string(out[:]), poolName) {
		t.Errorf("The pool wasn't deleted")
	}

}

func checkArray(t *testing.T, got []string, expected []string) {
	expCount := len(expected)
	if len(got) != expCount {
		t.Errorf("Did not got the rigth number of pools: %v, %v", len(got), expCount)
	}
	matches := 0
	expectedList := expected
	for _, vRes := range got {
		for i, vExp := range expectedList {
			if vRes == vExp {
				matches++
				expectedList = append(expected[:i], expected[i+1:]...)
				break
			}
		}
	}
	if matches != expCount {
		t.Errorf("Did not got all the pools names right, %v, %v", got, expected)
	}
}

func removeRbd(r *Rados) {
	if exists, _ := r.PoolExists("rbd"); exists {
		r.DeletePool("rbd")
	}
}

func Test_ListPools_SpecialCaseEmptyPoolNameWithNonEmpty(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	r.Connect()
	removeRbd(r)
	r.CreatePool("t")
	r.CreatePool("")
	defer r.DeletePool("t")
	defer r.DeletePool("")
	res, err := r.ListPools()
	if err != nil {
		t.Logf("GOT : %v", err)
	}
	checkArray(t, res, []string{"t", ""})
}

func Test_ListPools_SpecialCase_EmptyPoolName(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	r.Connect()
	removeRbd(r)
	r.CreatePool("")
	defer r.DeletePool("")
	res, err := r.ListPools()
	if err != nil {
		t.Logf("GOT : %v", err)
	}
	checkArray(t, res, []string{""})
}

func Test_ListPools_SpecialCase_NoPool(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	r.Connect()
	removeRbd(r)
	res, err := r.ListPools()
	if err != nil {
		t.Logf("GOT : %v", err)
	}
	checkArray(t, res, []string{})
}

func Test_ListPools(t *testing.T) {
	cephConf := setupContext(t)
	r, _ := NewRados(cephConf)
	r.Connect()
	removeRbd(r)
	r.CreatePool("one")
	r.CreatePool("two")
	r.CreatePool("three")
	defer r.DeletePool("one")
	defer r.DeletePool("two")
	defer r.DeletePool("three")
	res, err := r.ListPools()
	if err != nil {
		t.Logf("GOT : %v", err)
	}
	checkArray(t, res, []string{"one", "two", "three"})
}
