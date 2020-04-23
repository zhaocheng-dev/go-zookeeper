package main

import (
	"context"
	"fmt"
	_ "fmt"
	"github.com/zhaocheng-dev/go-zookeeper/zk"
	"time"
)

func main() {
	hosts := []string{"127.0.0.1"}
	//c, _, err := zk.Connect(hosts, time.Second) //*10)
	//if err != nil {
	//	panic(err)
	//}
	//children, stat, ch, err := c.ChildrenW("/edu/prek/fs_trigger/test")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("%+v %+v\n", children, stat)
	//e := <-ch
	//fmt.Printf("%+v\n", e)

	c, _, err := zk.Connect(hosts, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	l := zk.NewLock(c, "zktest/test", zk.WorldACL(zk.PermAll))
	background := context.Background()

	ret, err := l.Lock(background)
	if err != nil || ret != true {
		panic(err)
	}
	println("lock succ, do your business logic")

	// do some thing
	defer func() {
		l.Unlock()
		println("unlock succ, finish business logic")
	}()

	l2 := zk.NewLock(c, "zktest/test", zk.WorldACL(zk.PermAll))
	defer func() {
		l2.Unlock()
		println("unlock succ, finish business logic")
	}()
	ret, err = l2.LockWithTime(background, time.Second*2, 3)
	fmt.Printf("try with time out ret:%v err:%v", ret, err)

	time.Sleep(time.Second * 10)
}
