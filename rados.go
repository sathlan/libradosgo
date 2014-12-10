/*
Package rados is a wrapper around a very small subset of the ceph librados.

The main reason this library exists is to implements the IoCtxCreateDestroyer
interface used by the librbdgo library.

*/
package rados

/*
#cgo LDFLAGS: -lrados
#include "stdlib.h"
#include <errno.h>
#include "rados/librados.h"
*/
import "C"
import "fmt"
import "unsafe"
import "reflect"
import "bytes"

// Rados wrap the handle to the ceph cluster.
type Rados struct {
	handle uintptr
	state  string
}

// Version hold the version of the cluster.
type Version struct {
	major, minor, extra int
}

// String implements the stringer interface.
func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.extra)
}

func (r *Rados) cHandle() C.rados_t {
	return (C.rados_t)(r.handle)
}

// isState tests if the current state equal the one passed.
// Can be "connected",
func (r *Rados) isState(state string) bool {
	return r.state == state
}

// RequireConnected requires that the client be connected to ceph.
func (r *Rados) RequireConnected() (err error) {
	if r.state != "connected" {
		return fmt.Errorf("The pool must be connected")
	}
	return nil
}

// IoCtxCreate implements the IoCtxCreateDestroyer interface.
func (r *Rados) IoCtxCreate(poolName string) (uintptr, error) {
	var retC C.int
	poolNameC := C.CString(poolName)
	defer C.free(unsafe.Pointer(poolNameC))
	var ctx C.rados_ioctx_t
	if retC = C.rados_ioctx_create(r.cHandle(), poolNameC, &ctx); retC < 0 {
		return (uintptr)(unsafe.Pointer(nil)), fmt.Errorf("Rados cannot create context for pool %s", poolName)
	}
	ctrx := reflect.ValueOf(ctx).Pointer()
	return ctrx, nil
}

// IoCtxDestroy implements the IoCtxCreateDestroyer interface.
func (r *Rados) IoCtxDestroy(ctx uintptr) {
	ctxC := (C.rados_ioctx_t)(ctx)
	C.rados_aio_flush(ctxC)
	C.rados_ioctx_destroy(ctxC)
}

// NewRados creates the rados object.
func NewRados(confFile string) (*Rados, error) {
	var retC C.int
	var handleC C.rados_t
	confFileC := C.CString(confFile)
	defer C.free(unsafe.Pointer(confFileC))
	if retC = C.rados_create(&handleC, nil); retC < 0 {
		return nil, fmt.Errorf("Rados create error : %d", int(retC))
	}
	configFileC := C.CString(confFile)
	defer C.free(unsafe.Pointer(configFileC))

	if retC = C.rados_conf_read_file(handleC, configFileC); retC < 0 {
		return nil, fmt.Errorf("Rados read conf: %d", int(retC))
	}
	if uintptr(handleC) == 0 {
		return nil, fmt.Errorf("Could not get a valid handle to ceph.")
	}
	return &Rados{(uintptr)(handleC), ""}, nil
}

// Connect to the ceph cluster.
func (r *Rados) Connect() error {
	var retC C.int
	if retC = C.rados_connect(r.cHandle()); retC < 0 {
		return fmt.Errorf("Rados connect error : %d", int(retC))
	}
	r.state = "connected"
	return nil
}

// Shutdown close the connection to the cluster
func (r *Rados) Shutdown() error {
	if !r.isState("shutdown") {
		C.rados_shutdown(r.cHandle())
		r.state = "shutdown"
	}
	return nil
}

// Version returns the library verison.
func (r *Rados) Version() *Version {
	var major, minor, extra C.int
	C.rados_version(&major, &minor, &extra)
	return &Version{int(major), int(minor), int(extra)}
}

// PoolExists checks that the pool poolName exists.
func (r *Rados) PoolExists(poolName string) (bool, error) {
	if !r.isState("connected") {
		return false, fmt.Errorf("Rados not connected")
	}
	poolNameC := C.CString(poolName)
	defer C.free(unsafe.Pointer(poolNameC))
	retC := C.rados_pool_lookup(r.cHandle(), poolNameC)
	if retC >= 0 {
		return true, nil
	} else if retC == -C.ENOENT {
		return false, nil
	} else {
		return false, fmt.Errorf("Problem getting pool status")
	}
}

// CreatePool creates the pool poolName.
func (r *Rados) CreatePool(poolName string) error {
	if err := r.RequireConnected(); err != nil {
		return err
	}
	poolNameC := C.CString(poolName)
	defer C.free(unsafe.Pointer(poolNameC))
	retC := C.rados_pool_create(r.cHandle(), poolNameC)
	if retC < 0 {
		return fmt.Errorf("Cannot create pool %s", poolName)
	}
	return nil
}

// DeletePool deletes the pool poolName.
func (r *Rados) DeletePool(poolName string) error {
	if err := r.RequireConnected(); err != nil {
		return err
	}
	poolNameC := C.CString(poolName)
	defer C.free(unsafe.Pointer(poolNameC))
	retC := C.rados_pool_delete(r.cHandle(), poolNameC)
	if retC < 0 {
		return fmt.Errorf("Cannot create pool %s", poolName)
	}
	return nil
}

func splitData(data []byte) (res []string, err error) {
	// data\0data\0data\0\0

	// Empty list will bring this case.
	if len(data) == 1 && data[0] == []byte{0}[0] {
		return make([]string, 0), nil
	}
	cleanData := data[:len(data)-2]
	for _, v := range bytes.Split(cleanData, []byte{0}) {
		res = append(res, string(v))
	}
	return res, nil
}

// ListPools list all the pools.
func (r *Rados) ListPools() (res []string, err error) {
	if err := r.RequireConnected(); err != nil {
		return nil, err
	}
	sizeC := C.size_t(1)
	poolsName := make([]byte, int(sizeC))
	var retC C.int
	for {
		retC = C.rados_pool_list(
			r.cHandle(),
			(*C.char)(unsafe.Pointer(&poolsName[0])),
			sizeC,
		)
		if retC > C.int(sizeC) {
			sizeC = C.size_t(retC)
			poolsName = make([]byte, C.int(sizeC))
		} else {
			break
		}
	}
	poolsNames, err := splitData(poolsName)
	if err != nil {
		return poolsNames, fmt.Errorf("totto: %v -- %v", err, retC)
	}
	return poolsNames, nil
}
