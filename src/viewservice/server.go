package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	View          View
	Primary_time  int64
	Backup_time   int64
	Primary_ACKed bool
	last_primary  string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	reply.View = vs.View
	if vs.Primary_time == -1 && vs.View.Primary == "" && args.Viewnum == 0 {
		vs.Primary_ACKed = false
		vs.View.Viewnum = 1
		vs.View.Primary = args.Me
		reply.View = vs.View
	} else if args.Viewnum == 0 && vs.View.Primary == args.Me {
		vs.Primary_ACKed = false
		vs.View.Viewnum += 1
		if vs.View.Backup != "" {
			vs.View.Primary = vs.View.Backup
			vs.View.Backup = args.Me
		} else {
			vs.View.Primary = args.Me
		}
	} else if vs.View.Primary != "" && vs.View.Backup == "" && args.Viewnum == 0 {
		//fmt.Println("last_primary:", vs.last_primary, ", args.Me:", args.Me)
		//if vs.Backup_time == -1 || vs.last_primary == args.Me {
		//ok := call(vs.View.Primary, "Cleck.Ping", "", "")
		//if ok == false {
		//	vs.View.Primary = ""
		//} else {
		vs.Primary_ACKed = false
		vs.View.Viewnum += 1
		vs.View.Backup = args.Me
		if vs.Primary_time >= 0 {
			reply.View = vs.View
		}
		//}
		//}
		//} else if vs.View.Primary != "" && vs.View.Backup != "" && args.Viewnum == 0 {
		//	vs.Primary_ACKed = false
		//	vs.View.Viewnum += 1
		//	vs.View.Primary = vs.View.Backup
		//	vs.View.Backup = args.Me
		//	reply.View = vs.View
	} else if args.Viewnum > 0 && vs.View.Primary == args.Me {
		vs.Primary_time = 0
		if args.Viewnum == vs.View.Viewnum {
			vs.Primary_ACKed = true
		}
	} else if args.Viewnum > 0 && vs.View.Backup == args.Me {
		vs.Backup_time = 0
		if vs.View.Primary == "" && vs.View.Backup != "" {
			vs.View.Primary = vs.View.Backup
			vs.View.Backup = ""
			//vs.View.Viewnum += 1
			vs.Primary_ACKed = false
		}
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.View

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	if vs.Primary_time != -1 {
		if vs.Primary_ACKed == true && vs.Primary_time >= DeadPings {
			vs.last_primary = vs.View.Primary
			vs.View.Primary = ""
			vs.View.Viewnum += 1
		}
		vs.Primary_time += 1
	}
	if vs.Backup_time != -1 {
		if vs.Backup_time >= DeadPings {
			vs.View.Backup = ""
			vs.View.Viewnum += 1
		}
		vs.Backup_time += 1
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.View.Viewnum = 0
	vs.View.Primary = ""
	vs.View.Backup = ""
	vs.Primary_time = -1
	vs.Backup_time = -1
	vs.Primary_ACKed = false
	vs.last_primary = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
